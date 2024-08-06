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

pub struct KafkaConnection {
    pub connection: SinkConnection,
    old_connection: Option<SinkConnection>,
    created_at: Instant,
}

impl KafkaConnection {
    pub fn try_recv_into(&mut self, responses: &mut Vec<Message>) -> Result<(), ConnectionError> {
        // ensure old connection is completely drained before receiving from new connection
        if let Some(old_connection) = &mut self.old_connection {
            old_connection.try_recv_into(responses)?;
            if old_connection.pending_requests_count() == 0 {
                self.old_connection = None;
                Ok(())
            } else {
                self.connection.try_recv_into(responses)
            }
        } else {
            self.connection.try_recv_into(responses)
        }
    }
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

        match self.get_connection_state(authorize_scram_over_mtls, destination) {
            ConnectionState::Open => Ok(self.connections.get_mut(&destination).unwrap()),
            ConnectionState::Unopened => {
                let address = match &node {
                    Some(node) => &node.kafka_address,
                    None => contact_points.choose(rng).unwrap(),
                };

                self.connections.insert(
                    destination,
                    KafkaConnection {
                        connection: connection_factory
                            .create_connection(address, authorize_scram_over_mtls, sasl_mechanism)
                            .await
                            .context("Failed to create a new connection")?,
                        old_connection: None,
                        created_at: Instant::now(),
                    },
                );
                Ok(self.connections.get_mut(&destination).unwrap())
            }
            ConnectionState::AtRiskOfTimeout => {
                let address = match &node {
                    Some(node) => &node.kafka_address,
                    None => contact_points.choose(rng).unwrap(),
                };

                let old_connection = self.connections.remove(&destination).unwrap();
                if old_connection.old_connection.is_some() {
                    return Err(anyhow!("Old connection had an old connection"));
                }
                let old_connection = if old_connection.connection.pending_requests_count() == 0 {
                    None
                } else {
                    Some(old_connection.connection)
                };

                self.connections.insert(
                    destination,
                    KafkaConnection {
                        connection: connection_factory
                            .create_connection(address, authorize_scram_over_mtls, sasl_mechanism)
                            .await
                            .context("Failed to create a new connection")?,
                        old_connection,
                        created_at: Instant::now(),
                    },
                );
                tracing::info!("Recreated outgoing connection due to risk of timeout");
                Ok(self.connections.get_mut(&destination).unwrap())
            }
        }
    }

    fn get_connection_state(
        &self,
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtls>,
        destination: Destination,
    ) -> ConnectionState {
        let timeout = if let Some(scram_over_mtls) = authorize_scram_over_mtls {
            scram_over_mtls.delegation_token_lifetime
        } else {
            // TODO: make this configurable
            //       for now, use the default value of connections.max.idle.ms (10 minutes)
            Duration::from_secs(60 * 10)
        };
        if let Some(connection) = self.connections.get(&destination) {
            // TODO: subtract a batch level Instant::now instead of using elapsed
            // use 3/4 of the timeout to make sure we trigger this well before it actually times out
            if connection.created_at.elapsed() > timeout.mul_f32(0.75) {
                ConnectionState::AtRiskOfTimeout
            } else {
                ConnectionState::Open
            }
        } else {
            ConnectionState::Unopened
        }
    }
}

enum ConnectionState {
    Open,
    Unopened,
    // TODO: maybe combine with Unopened since old_connection can just easily take the appropriate Option value
    AtRiskOfTimeout,
}
