use super::routing_key::calculate;
use super::token_map::TokenMap;
use crate::transforms::cassandra::sink_cluster::node::CassandraNode;
use cassandra_protocol::frame::message_execute::BodyReqExecuteOwned;
use cassandra_protocol::frame::message_result::PreparedMetadata;
use cassandra_protocol::frame::Version;
use cassandra_protocol::token::Murmur3Token;
use rand::prelude::*;
use std::collections::HashMap;

#[derive(Debug)]
pub struct NodePool {
    prepared_metadata: HashMap<Vec<u8>, PreparedMetadata>,
    token_map: TokenMap,
    pub nodes: Vec<CassandraNode>,

    rng: SmallRng,
}

impl NodePool {
    pub fn new(nodes: Vec<CassandraNode>) -> Self {
        Self {
            token_map: TokenMap::new(nodes.as_slice()),
            nodes,
            prepared_metadata: HashMap::new(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn set_nodes(&mut self, nodes: Vec<CassandraNode>) {
        self.nodes = nodes;
        self.token_map = TokenMap::new(self.nodes.as_slice());
    }

    pub fn add_prepared_result(&mut self, id: Vec<u8>, metadata: PreparedMetadata) {
        tracing::warn!("Add prepared result metadata {id:?}");
        self.prepared_metadata.insert(id, metadata);
    }

    // TODO handle the unwrap
    fn get_prepared_metadata(&self, id: &Vec<u8>) -> &PreparedMetadata {
        tracing::warn!("Getting prepeared metadata {id:?}");
        self.prepared_metadata.get(id).unwrap()
    }

    pub fn random_node(&mut self) -> &mut CassandraNode {
        self.nodes
            .iter_mut()
            .filter(|x| x.is_up)
            .choose(&mut self.rng)
            .unwrap()
    }

    pub fn get_random_node_in_dc_rack(&mut self, rack: &String) -> &CassandraNode {
        self.nodes
            .iter()
            .filter(|x| x.rack == *rack && x.is_up)
            .choose(&mut self.rng)
            .unwrap()
    }

    /// Get a token routed replica node for the supplied execute message (if exists)
    pub fn replica_node(
        &mut self,
        execute: &BodyReqExecuteOwned,
        version: &Version,
    ) -> Option<&mut CassandraNode> {
        let metadata = self.get_prepared_metadata(&execute.id.clone().into_bytes().unwrap());

        let routing_key = calculate(
            &metadata.pk_indexes,
            execute.query_parameters.values.as_ref().unwrap(),
            *version,
        )
        .unwrap();

        let mut replica_host_ids = self
            .token_map
            .nodes_for_token_capped(Murmur3Token::generate(&routing_key), 1);

        if let Some(host_id) = replica_host_ids.next() {
            return self
                .nodes
                .iter_mut()
                .find(|node| host_id == node.host_id && node.is_up);
        }

        None
    }
}
