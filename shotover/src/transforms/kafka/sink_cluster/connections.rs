use crate::{
    connection::{ConnectionError, SinkConnection},
    message::Message,
};
use anyhow::{anyhow, Context, Result};
use fnv::FnvBuildHasher;
use kafka_protocol::{messages::BrokerId, protocol::StrBytes};
use metrics::Counter;
use rand::{rngs::SmallRng, seq::IteratorRandom};
use std::{collections::HashMap, time::Instant};

use super::{
    kafka_node::{ConnectionFactory, KafkaAddress, KafkaNode, KafkaNodeState},
    scram_over_mtls::{connection::ScramOverMtlsConnection, AuthorizeScramOverMtls},
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
    pub control_connection_address: Option<KafkaAddress>,
    out_of_rack_requests: Counter,
}

impl Connections {
    pub fn new(out_of_rack_requests: Counter) -> Self {
        Self {
            connections: Default::default(),
            control_connection_address: None,
            out_of_rack_requests,
        }
    }

    /// If a connection already exists for the requested Destination return it.
    /// Otherwise create a new connection, cache it and return it.
    #[expect(clippy::too_many_arguments)]
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

        match self.get_connection_state(recent_instant, destination) {
            ConnectionState::Open => {
                let connection = self.connections.get_mut(&destination).unwrap();
                // connection already exists so we can just use it.
                // however if it has an error we need to recreate it.
                if let Some(error) = connection.get_error() {
                    if connection.pending_requests_count() > 0 {
                        return Err(anyhow!(error).context("get_or_open_connection: Outgoing connection had pending requests, those requests/responses are lost so connection recovery cannot be attempted."));
                    }
                    self.create_and_insert_connection(
                        rng,
                        connection_factory,
                        authorize_scram_over_mtls,
                        sasl_mechanism,
                        nodes,
                        node,
                        contact_points,
                        None,
                        destination,
                    )
                    .await
                    .with_context(|| {
                        format!("Failed to recreate connection after encountering error {error:?}")
                    })?;
                    tracing::info!("Recreated connection after it hit error {error:?}")
                }
            }
            ConnectionState::Unopened => {
                self.create_and_insert_connection(
                    rng,
                    connection_factory,
                    authorize_scram_over_mtls,
                    sasl_mechanism,
                    nodes,
                    node,
                    contact_points,
                    None,
                    destination,
                )
                .await
                .context("Failed to create a new connection")?;
            }
            // This variant is only returned when scram_over_mtls is in use
            ConnectionState::AtRiskOfAuthTokenExpiry => {
                let old_connection = self.connections.remove(&destination);

                self.create_and_insert_connection(
                    rng,
                    connection_factory,
                    authorize_scram_over_mtls,
                    sasl_mechanism,
                    nodes,
                    node,
                    contact_points,
                    old_connection,
                    destination,
                )
                .await
                .context("Failed to create a new connection to replace a connection that is at risk of having its delegation token expire")?;

                tracing::info!(
                    "Recreated outgoing connection due to risk of delegation token expiring"
                );
            }
        }
        Ok(self.connections.get_mut(&destination).unwrap())
    }

    #[expect(clippy::too_many_arguments)]
    async fn create_and_insert_connection(
        &mut self,
        rng: &mut SmallRng,
        connection_factory: &ConnectionFactory,
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtls>,
        sasl_mechanism: &Option<String>,
        nodes: &[KafkaNode],
        node: Option<&KafkaNode>,
        contact_points: &[KafkaAddress],
        old_connection: Option<KafkaConnection>,
        destination: Destination,
    ) -> Result<()> {
        let address = match &node {
            // route to ID
            Some(node) => &node.kafka_address,
            // route to control connection
            None => {
                // If we have a node in the nodes list that is up use its address.
                // Otherwise fall back to the first contact points
                let address_from_node = nodes
                    .iter()
                    .filter(|x| x.is_up())
                    .choose(rng)
                    .map(|x| x.kafka_address.clone());
                self.control_connection_address =
                    address_from_node.or_else(|| contact_points.iter().choose(rng).cloned());
                self.control_connection_address.as_ref().unwrap()
            }
        };
        let connection = connection_factory
            .create_connection(address, authorize_scram_over_mtls, sasl_mechanism)
            .await?;

        self.connections.insert(
            destination,
            KafkaConnection::new(
                authorize_scram_over_mtls,
                sasl_mechanism,
                connection,
                old_connection,
            )?,
        );

        Ok(())
    }

    fn get_connection_state(
        &self,
        recent_instant: Instant,
        destination: Destination,
    ) -> ConnectionState {
        if let Some(connection) = self.connections.get(&destination) {
            connection.state(recent_instant)
        } else {
            ConnectionState::Unopened
        }
    }

    /// Open a new connection to the requested Destination.
    /// If the connection is succesfully created, the old connection is replace with the new one.
    /// Otherwise the old connection is just deleted with no replacement and an error is returned.
    pub async fn handle_connection_error(
        &mut self,
        connection_factory: &ConnectionFactory,
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtls>,
        sasl_mechanism: &Option<String>,
        nodes: &[KafkaNode],
        destination: Destination,
        error: anyhow::Error,
    ) -> Result<()> {
        let old_connection = self.connections.remove(&destination);

        let address = match destination {
            Destination::Id(id) => {
                &nodes
                    .iter()
                    .find(|x| x.broker_id == id)
                    .unwrap()
                    .kafka_address
            }
            Destination::ControlConnection => self.control_connection_address.as_ref().unwrap(),
        };

        let connection = connection_factory
            .create_connection(address, authorize_scram_over_mtls, sasl_mechanism)
            .await;

        // Update the node state according to whether we can currently open a connection.
        let node_state = if connection.is_err() {
            KafkaNodeState::Down
        } else {
            KafkaNodeState::Up
        };
        nodes
            .iter()
            .find(|x| match destination {
                Destination::Id(id) => x.broker_id == id,
                Destination::ControlConnection => {
                    &x.kafka_address == self.control_connection_address.as_ref().unwrap()
                }
            })
            .unwrap()
            .set_state(node_state);

        if old_connection
            .map(|old| old.pending_requests_count())
            .unwrap_or(0)
            > 0
        {
            return Err(error.context("Outgoing connection had pending requests, those requests/responses are lost so connection recovery cannot be attempted."));
        }

        let connection =
            connection.context("Failed to create a new connection to test if a node is down")?;

        // Recreating the node succeeded.
        // So store it as the new connection, as long as we werent waiting on any responses in the old connection
        let connection =
            KafkaConnection::new(authorize_scram_over_mtls, sasl_mechanism, connection, None)?;

        self.connections.insert(destination, connection);
        Ok(())
    }
}

pub enum KafkaConnection {
    Regular(SinkConnection),
    ScramOverMtls(ScramOverMtlsConnection),
}

impl KafkaConnection {
    pub fn new(
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtls>,
        sasl_mechanism: &Option<String>,
        connection: SinkConnection,
        old_connection: Option<KafkaConnection>,
    ) -> Result<Self> {
        let using_scram_over_mtls = authorize_scram_over_mtls.is_some()
            && sasl_mechanism
                .as_ref()
                .map(|x| SASL_SCRAM_MECHANISMS.contains(&x.as_str()))
                .unwrap_or(false);
        if using_scram_over_mtls {
            let old_connection = old_connection.map(|x| match x {
                KafkaConnection::Regular(_) => {
                    panic!("Cannot replace a Regular connection with ScramOverMtlsConnection")
                }
                KafkaConnection::ScramOverMtls(old_connection) => old_connection,
            });
            Ok(KafkaConnection::ScramOverMtls(
                ScramOverMtlsConnection::new(
                    connection,
                    old_connection,
                    authorize_scram_over_mtls,
                )?,
            ))
        } else {
            Ok(KafkaConnection::Regular(connection))
        }
    }

    /// Attempts to receive messages, if there are no messages available it immediately returns an empty vec.
    /// If there is a problem with the connection an error is returned.
    pub fn try_recv_into(&mut self, responses: &mut Vec<Message>) -> Result<(), ConnectionError> {
        match self {
            KafkaConnection::Regular(c) => c.try_recv_into(responses),
            KafkaConnection::ScramOverMtls(c) => c.try_recv_into(responses),
        }
    }

    /// Send messages.
    /// If there is a problem with the connection an error is returned.
    pub fn send(&mut self, messages: Vec<Message>) -> Result<(), ConnectionError> {
        match self {
            KafkaConnection::Regular(c) => c.send(messages),
            KafkaConnection::ScramOverMtls(c) => c.send(messages),
        }
    }

    /// Receives messages, if there are no messages available it awaits until there are messages.
    /// If there is a problem with the connection an error is returned.
    pub async fn recv(&mut self) -> Result<Vec<Message>, ConnectionError> {
        match self {
            KafkaConnection::Regular(c) => c.recv().await,
            KafkaConnection::ScramOverMtls(c) => c.recv().await,
        }
    }

    pub fn get_error(&mut self) -> Option<ConnectionError> {
        match self {
            KafkaConnection::Regular(c) => c.get_error(),
            KafkaConnection::ScramOverMtls(c) => c.get_error(),
        }
    }

    /// Number of requests waiting on a response.
    /// The count includes requests that will have a dummy response generated by shotover.
    pub fn pending_requests_count(&self) -> usize {
        match self {
            KafkaConnection::Regular(c) => c.pending_requests_count(),
            KafkaConnection::ScramOverMtls(c) => c.pending_requests_count(),
        }
    }

    /// Returns either ConnectionState::Open or ConnectionState::AtRiskOfAuthTokenExpiry
    pub fn state(&self, recent_instant: Instant) -> ConnectionState {
        match self {
            KafkaConnection::Regular(_) => ConnectionState::Open,
            KafkaConnection::ScramOverMtls(c) => c.state(recent_instant),
        }
    }
}

pub enum ConnectionState {
    Open,
    Unopened,
    AtRiskOfAuthTokenExpiry,
}
