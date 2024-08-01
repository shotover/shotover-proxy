use crate::connection::SinkConnection;
use anyhow::{Context, Result};
use fnv::FnvBuildHasher;
use kafka_protocol::{messages::BrokerId, protocol::StrBytes};
use metrics::Counter;
use rand::{
    rngs::SmallRng,
    seq::{IteratorRandom, SliceRandom},
};
use std::{collections::HashMap, sync::atomic::Ordering};

use super::{
    node::{ConnectionFactory, KafkaAddress, KafkaNode, NodeState},
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

pub struct Connections {
    pub connections: HashMap<Destination, SinkConnection, FnvBuildHasher>,
    control_connection_address: Option<KafkaAddress>,
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
    ) -> Result<&mut SinkConnection> {
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
        // map entry API can not be used with async
        #[allow(clippy::map_entry)]
        if !self.connections.contains_key(&destination) {
            let address = match &node {
                Some(node) => &node.kafka_address,
                None => {
                    // If we have a node in the nodes list that is up use its address.
                    // Otherwise fall back to the first contact points
                    let address_from_node = nodes
                        .iter()
                        .filter(|x| matches!(x.state.load(Ordering::Relaxed), NodeState::Up))
                        .choose(rng)
                        .map(|x| x.kafka_address.clone());
                    self.control_connection_address =
                        address_from_node.or_else(|| contact_points.iter().choose(rng).cloned());
                    self.control_connection_address.as_ref().unwrap()
                }
            };

            let connection = connection_factory
                .create_connection(address, authorize_scram_over_mtls, sasl_mechanism)
                .await
                .context("Failed to create a new connection")?;
            self.connections.insert(destination, connection);
        }
        Ok(self.connections.get_mut(&destination).unwrap())
    }

    /// Open a new connection to the requested Destination and return it.
    /// Any existing cached connection is overwritten by the new one.
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_connection_error(
        &mut self,
        rng: &mut SmallRng,
        connection_factory: &ConnectionFactory,
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtls>,
        sasl_mechanism: &Option<String>,
        nodes: &[KafkaNode],
        contact_points: &[KafkaAddress],
        destination: Destination,
        error: anyhow::Error,
    ) -> Result<()> {
        let address = match destination {
            Destination::Id(id) => {
                &nodes
                    .iter()
                    .find(|x| x.broker_id == id)
                    .unwrap()
                    .kafka_address
            }
            Destination::ControlConnection => contact_points.choose(rng).unwrap(),
        };

        let connection = connection_factory
            .create_connection(address, authorize_scram_over_mtls, sasl_mechanism)
            .await
            .context("Failed to create a new connection");

        match connection {
            Ok(connection) => {
                // Recreating the node succeeded.
                // So cache it and return it, as long as we werent waiting on any responses in the old connection
                let old = self.connections.insert(destination, connection);

                if old.map(|old| old.pending_requests_count()).unwrap_or(0) > 0 {
                    Err(error.context("Succesfully reopened outgoing connection but previous outgoing connection had pending requests."))
                } else {
                    Ok(())
                }
            }
            Err(err) => {
                // Recreating the node failed.
                // So update the metadata and connection so we dont attempt to connect to it again,
                // and then return the error
                nodes
                    .iter()
                    .find(|x| match destination {
                        Destination::Id(id) => x.broker_id == id,
                        Destination::ControlConnection => {
                            &x.kafka_address == self.control_connection_address.as_ref().unwrap()
                        }
                    })
                    .unwrap()
                    .state
                    .store(NodeState::Down, Ordering::Relaxed);

                self.connections.remove(&destination);
                Err(err)
            }
        }
    }
}
