use crate::connection::SinkConnection;
use anyhow::{Context, Result};
use fnv::FnvBuildHasher;
use kafka_protocol::{messages::BrokerId, protocol::StrBytes};
use metrics::Counter;
use rand::{rngs::SmallRng, seq::SliceRandom};
use std::collections::HashMap;

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

pub struct Connections {
    pub connections: HashMap<Destination, SinkConnection, FnvBuildHasher>,
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
                None => contact_points.choose(rng).unwrap(),
            };

            self.connections.insert(
                destination,
                connection_factory
                    .create_connection(address, authorize_scram_over_mtls, sasl_mechanism)
                    .await
                    .context("Failed to create a new connection")?,
            );
        }
        Ok(self.connections.get_mut(&destination).unwrap())
    }
}
