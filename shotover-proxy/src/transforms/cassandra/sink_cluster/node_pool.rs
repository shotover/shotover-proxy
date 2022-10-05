use super::routing_key::calculate_routing_key;
use super::token_map::TokenMap;
use crate::transforms::cassandra::sink_cluster::node::CassandraNode;
use anyhow::{anyhow, Error, Result};
use cassandra_protocol::frame::message_execute::BodyReqExecuteOwned;
use cassandra_protocol::frame::message_result::PreparedMetadata;
use cassandra_protocol::frame::Version;
use cassandra_protocol::token::Murmur3Token;
use cassandra_protocol::types::CBytesShort;
use rand::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{watch, RwLock};

pub enum GetReplicaErr {
    NoMetadata,
    Other(Error),
}

#[derive(Debug)]
pub struct NodePool {
    prepared_metadata: Arc<RwLock<HashMap<CBytesShort, PreparedMetadata>>>,
    token_map: TokenMap,
    nodes: Vec<CassandraNode>,
}

impl Clone for NodePool {
    fn clone(&self) -> Self {
        Self {
            prepared_metadata: self.prepared_metadata.clone(),
            token_map: TokenMap::new(&[]),
            nodes: vec![],
        }
    }
}

impl NodePool {
    pub fn new(nodes: Vec<CassandraNode>) -> Self {
        Self {
            token_map: TokenMap::new(nodes.as_slice()),
            nodes,
            prepared_metadata: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn nodes(&mut self) -> &mut [CassandraNode] {
        &mut self.nodes
    }

    /// if the node list has been updated use the new list, copying over any existing connections
    pub fn update_nodes(&mut self, nodes_rx: &mut watch::Receiver<Vec<CassandraNode>>) {
        let mut new_nodes = nodes_rx.borrow_and_update().clone();

        for node in self.nodes.drain(..) {
            if let Some(outbound) = node.outbound {
                for new_node in &mut new_nodes {
                    if new_node.host_id == node.host_id {
                        new_node.outbound = Some(outbound);
                        break;
                    }
                }
            }
        }
        self.nodes = new_nodes;
        self.token_map = TokenMap::new(self.nodes.as_slice());
    }

    pub async fn add_prepared_result(&mut self, id: CBytesShort, metadata: PreparedMetadata) {
        let mut write_lock = self.prepared_metadata.write().await;
        write_lock.insert(id, metadata);
    }

    pub fn get_random_node_in_dc_rack(
        &mut self,
        rack: &String,
        rng: &mut SmallRng,
    ) -> &mut CassandraNode {
        self.nodes
            .iter_mut()
            .filter(|x| x.rack == *rack && x.is_up)
            .choose(rng)
            .unwrap()
    }

    /// Get a token routed replica node for the supplied execute message (if exists)
    pub async fn replica_node(
        &mut self,
        execute: &BodyReqExecuteOwned,
        version: &Version,
        rng: &mut SmallRng,
    ) -> Result<Option<&mut CassandraNode>, GetReplicaErr> {
        let metadata = {
            let read_lock = self.prepared_metadata.read().await;
            read_lock
                .get(&execute.id)
                .ok_or(GetReplicaErr::NoMetadata)?
                .clone()
        };

        let routing_key = calculate_routing_key(
            &metadata.pk_indexes,
            execute.query_parameters.values.as_ref().ok_or_else(|| {
                GetReplicaErr::Other(anyhow!("Execute body does not have query paramters"))
            })?,
            *version,
        )
        .unwrap();

        // TODO this should use the keyspace info to properly select the replica count
        let replica_host_ids = self
            .token_map
            .iter_replica_nodes(Murmur3Token::generate(&routing_key), 1);

        if let Some(host_id) = replica_host_ids.choose(rng) {
            return Ok(self
                .nodes
                .iter_mut()
                .find(|node| host_id == node.host_id && node.is_up));
        }

        Ok(None)
    }
}
