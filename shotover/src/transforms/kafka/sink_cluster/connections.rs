use crate::{
    connection::{ConnectionError, SinkConnection},
    message::Message,
};
use anyhow::{anyhow, Context, Result};
use fnv::FnvBuildHasher;
use kafka_protocol::{messages::BrokerId, protocol::StrBytes};
use metrics::Counter;
use rand::{rngs::SmallRng, seq::SliceRandom};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use super::{
    node::{ConnectionFactory, KafkaAddress, KafkaNode},
    scram_over_mtls::AuthorizeScramOverMtls,
    SASL_SCRAM_MECHANISMS,
};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Destination {
    Id(BrokerId),
    /// The control connection is a bit weird:
    /// * while !auth_complete it needs to be routed to via `PendingRequest`
    /// * However, once auth_complete is true, Destination::ControlConnection should never be routed to.
    ///     Instead, at this point control_send_receive must be called which will immediately return a
    ///     response in place without going through the routing logic.
    ///
    /// TODO: In the future it might make sense to remove control_send_receive in favor of always routing to the control connection.
    ///       This will avoid latency spikes where a response is delayed because we have to wait for a metadata request to come back.
    ///       However, to implement this we will need to allow request routing to be suspended and resumed once the required response came back.
    ///       So it might not be worth it.
    ControlConnection,
}

pub struct Connections {
    pub connections: HashMap<Destination, KafkaConnection, FnvBuildHasher>,
    out_of_rack_requests: Counter,
}

impl Connections {
    pub fn new(out_of_rack_requests: Counter) -> Self {
        Self {
            connections: Default::default(),
            out_of_rack_requests,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_or_open_connection(
        &mut self,
        rng: &mut SmallRng,
        connection_factory: &ConnectionFactory,
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtls>,
        sasl_mechanism: &Option<String>,
        nodes: &[KafkaNode],
        contact_points: &[KafkaAddress],
        local_rack: &StrBytes,
        recent_instant: Instant,
        destination: Destination,
    ) -> Result<&mut KafkaConnection> {
        let node = match destination {
            Destination::Id(id) => Some(nodes.iter().find(|x| x.broker_id == id).unwrap()),
            Destination::ControlConnection => None,
        };
        if let Some(node) = &node {
            if node
                .rack
                .as_ref()
                .map(|rack| rack != local_rack)
                .unwrap_or(false)
            {
                self.out_of_rack_requests.increment(1);
            }
        }

        match self.get_connection_state(authorize_scram_over_mtls, recent_instant, destination) {
            ConnectionState::Open => {
                // connection already open
            }
            ConnectionState::Unopened => {
                self.create_and_insert_connection(
                    rng,
                    connection_factory,
                    authorize_scram_over_mtls,
                    sasl_mechanism,
                    node,
                    contact_points,
                    None,
                    destination,
                )
                .await
                .context("Failed to create a new connection")?;
            }
            ConnectionState::AtRiskOfTimeout => {
                let old_connection = self.connections.remove(&destination).unwrap();
                if old_connection.old_connection.is_some() {
                    return Err(anyhow!("Old connection had an old connection"));
                }
                let old_connection = if old_connection.connection.pending_requests_count() == 0 {
                    None
                } else {
                    Some(old_connection.connection)
                };

                self.create_and_insert_connection(
                    rng,
                    connection_factory,
                    authorize_scram_over_mtls,
                    sasl_mechanism,
                    node,
                    contact_points,
                    old_connection,
                    destination,
                )
                .await
                .context("Failed to create a new connection to replace an old connection")?;

                tracing::info!("Recreated outgoing connection due to risk of timeout");
            }
        }
        Ok(self.connections.get_mut(&destination).unwrap())
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_and_insert_connection(
        &mut self,
        rng: &mut SmallRng,
        connection_factory: &ConnectionFactory,
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtls>,
        sasl_mechanism: &Option<String>,
        node: Option<&KafkaNode>,
        contact_points: &[KafkaAddress],
        old_connection: Option<SinkConnection>,
        destination: Destination,
    ) -> Result<()> {
        let address = match &node {
            Some(node) => &node.kafka_address,
            None => contact_points.choose(rng).unwrap(),
        };

        self.connections.insert(
            destination,
            KafkaConnection {
                connection: connection_factory
                    .create_connection(address, authorize_scram_over_mtls, sasl_mechanism)
                    .await?,
                old_connection,
                using_scram_over_mtls: authorize_scram_over_mtls.is_some()
                    && sasl_mechanism
                        .as_ref()
                        .map(|x| SASL_SCRAM_MECHANISMS.contains(&x.as_str()))
                        .unwrap_or(false),
                timeout_last_usage: Instant::now(),
            },
        );

        Ok(())
    }

    /// Since shotover maintains multiple outgoing connections for each incoming connection,
    /// one of these outgoing connections may go unused for a while causing the broker to time out the connection and close it,
    /// this may occur even when the client has recently sent a request since shotover may route it to another broker.
    /// Furthermore when scram_over_mtls is enabled, connections will be closed when the token it auth'd with expires.
    /// This is a very similar problem to timeouts described above so we can consider it another kind of timeout.
    /// To prevent both of these issues, this method detects when a connection needs to be recreated to avoid a timeout.
    fn get_connection_state(
        &self,
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtls>,
        recent_instant: Instant,
        destination: Destination,
    ) -> ConnectionState {
        let timeout = if let Some(scram_over_mtls) = authorize_scram_over_mtls {
            // The delegation token is recreated after `0.5 * delegation_token_lifetime`
            // Consider what happens when we match that timing for our connection timeout here:
            //
            //     create token t1      create token t2
            // |--------------------|--------------------|
            // |                    ^ all connections created after this point use token t2 instead of token t1
            // |                                         |
            // |             token t1 lifetime           |
            // |-----------------------------------------|
            // |                                         ^
            // |                   after this point, connections still alive that were authed with token t1 will be closed by the broker.
            // |                                         |
            // |                                         |
            // |                                         |
            // |                                 token t2 lifetime
            // |                    |-----------------------------------------|
            // |                    ^ all connections created after this point use token t2
            // |                                         |
            // |                                         |
            // |                                         |
            // |      connection lifetime using token t1 |
            // |          |--------------------|         |
            // This case is fine, the connection exists entirely within the lifetime of token t1.
            // |                                         |
            // |                                         |
            // |                                         |
            // |                            connection lifetime using token t2
            // |                                |--------------------|
            // This case is fine, the connection exists entirely within the lifetime of token t2.
            // |                                         |
            // |                                         |
            // |                                         |
            // |              connection lifetime using token t?
            // |                    |--------------------|
            // This case is a race condition.
            // We could start with either token t2 or t1.
            // If we start with t1 we could go past the end of t1's lifetime.
            // To avoid this issue we reduce the size of the connection lifetime by a further 25%
            //
            // At low values of delegation_token_lifetime all of this falls apart since something
            // like a VM migration could delay shotover execution for many seconds.
            // However for sufficently large delegation_token_lifetime values (> 1 hour) this should be fine.
            scram_over_mtls
                .delegation_token_lifetime
                // match token recreation time
                .mul_f32(0.5)
                // further reduce connection timeout
                .mul_f32(0.75)
        } else {
            // use 3/4 of the timeout to make sure we trigger this well before it actually times out
            CONNECTIONS_MAX_IDLE_DEFAULT.mul_f32(0.75)
            // TODO: relying on the default value to be unchanged is not ideal, so either:
            // * query the broker for the actual value of connections.max.idle.ms
            // * have the user configure it in shotover's topology.yaml
        };
        if let Some(connection) = self.connections.get(&destination) {
            // Since we cant be 100% exact with time anyway, we use a recent instant that can be reused to reduce syscalls.
            if recent_instant.duration_since(connection.timeout_last_usage) > timeout {
                ConnectionState::AtRiskOfTimeout
            } else {
                ConnectionState::Open
            }
        } else {
            ConnectionState::Unopened
        }
    }
}

pub struct KafkaConnection {
    connection: SinkConnection,
    /// When a connection is recreated to avoid timeouts,
    /// the old connection will be kept around until all responses have been received from it.
    old_connection: Option<SinkConnection>,
    using_scram_over_mtls: bool,
    timeout_last_usage: Instant,
}

impl KafkaConnection {
    /// Attempts to receive messages, if there are no messages available it immediately returns an empty vec.
    /// If there is a problem with the connection an error is returned.
    pub fn try_recv_into(&mut self, responses: &mut Vec<Message>) -> Result<(), ConnectionError> {
        // ensure old connection is completely drained before receiving from new connection
        if let Some(old_connection) = &mut self.old_connection {
            old_connection.try_recv_into(responses)?;
            if old_connection.pending_requests_count() == 0 {
                self.old_connection = None;
                self.connection.try_recv_into(responses)?;
            }
            Ok(())
        } else {
            self.connection.try_recv_into(responses)
        }
    }

    /// Send messages.
    /// If there is a problem with the connection an error is returned.
    pub fn send(&mut self, messages: Vec<Message>) -> Result<(), ConnectionError> {
        if !self.using_scram_over_mtls {
            self.timeout_last_usage = Instant::now();
        }
        self.connection.send(messages)
    }

    /// Receives messages, if there are no messages available it awaits until there are messages.
    /// If there is a problem with the connection an error is returned.
    pub async fn recv(&mut self) -> Result<Vec<Message>, ConnectionError> {
        // ensure old connection is completely drained before receiving from new connection
        if let Some(old_connection) = &mut self.old_connection {
            let mut received = old_connection.recv().await?;
            if old_connection.pending_requests_count() == 0 {
                self.old_connection = None;
                // Do not use `recv` method here since we already have at least one message due to previous `recv`,
                // so we avoid blocking by calling `try_recv_into` instead.
                self.connection.try_recv_into(&mut received)?;
            }
            Ok(received)
        } else {
            self.connection.recv().await
        }
    }
}

/// default value of kafka broker config connections.max.idle.ms (10 minutes)
const CONNECTIONS_MAX_IDLE_DEFAULT: Duration = Duration::from_secs(60 * 10);

enum ConnectionState {
    Open,
    Unopened,
    AtRiskOfTimeout,
}
