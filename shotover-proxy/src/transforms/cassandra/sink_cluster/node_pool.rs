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
use split_iter::Splittable;
use std::sync::Arc;
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::{watch, RwLock};

#[derive(Debug)]
pub enum GetReplicaErr {
    NoMetadata,
    Other(Error),
}

#[derive(Debug)]
pub struct NodePool {
    prepared_metadata: Arc<RwLock<HashMap<CBytesShort, PreparedMetadata>>>,
    token_map: TokenMap,
    nodes: Vec<CassandraNode>,
    prev_idx: usize,
}

impl Clone for NodePool {
    fn clone(&self) -> Self {
        Self {
            prepared_metadata: self.prepared_metadata.clone(),
            token_map: TokenMap::new(&[]),
            nodes: vec![],
            prev_idx: 0,
        }
    }
}

impl NodePool {
    pub fn new(nodes: Vec<CassandraNode>) -> Self {
        Self {
            token_map: TokenMap::new(nodes.as_slice()),
            nodes,
            prepared_metadata: Arc::new(RwLock::new(HashMap::new())),
            prev_idx: 0,
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

    pub fn get_shuffled_addresses_in_dc_rack(
        &mut self,
        rack: &str,
        rng: &mut SmallRng,
    ) -> Vec<SocketAddr> {
        let mut nodes: Vec<_> = self
            .nodes
            .iter_mut()
            .filter(|node| node.is_up && node.rack == *rack)
            .map(|node| node.address)
            .collect();

        nodes.shuffle(rng);
        nodes
    }

    pub fn get_round_robin_node_in_dc_rack(&mut self, rack: &str) -> &mut CassandraNode {
        let up_indexes: Vec<usize> = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(i, node)| {
                if node.is_up && node.rack == *rack {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        self.prev_idx = (self.prev_idx + 1) % up_indexes.len();

        self.nodes
            .get_mut(*up_indexes.get(self.prev_idx).unwrap())
            .unwrap()
    }

    /// Get a token routed replica node for the supplied execute message (if exists)
    /// Will attempt to get a replica in the supplied rack if exists, otherwise get one in
    /// the same data center
    pub async fn get_replica_node_in_dc(
        &mut self,
        execute: &BodyReqExecuteOwned,
        rack: &str,
        version: &Version,
        rng: &mut SmallRng,
        rf: usize, // TODO this parameter should be removed
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
            .iter_replica_nodes(Murmur3Token::generate(&routing_key), rf)
            .collect::<Vec<uuid::Uuid>>();

        let (dc_replicas, rack_replicas) = self
            .nodes
            .iter_mut()
            .filter(|node| replica_host_ids.contains(&node.host_id) && node.is_up)
            .split(|node| node.rack == rack);

        if let Some(rack_replica) = rack_replicas.choose(rng) {
            return Ok(Some(rack_replica));
        }

        Ok(dc_replicas.choose(rng))
    }
}

#[cfg(test)]
mod test_node_pool {
    use super::*;
    use crate::transforms::cassandra::sink_cluster::CassandraNode;
    use uuid::Uuid;

    #[test]
    fn test_round_robin() {
        let nodes = prepare_nodes();

        let mut node_pool = NodePool::new(nodes.clone());

        node_pool.nodes[1].is_up = false;
        node_pool.nodes[3].is_up = false;
        node_pool.nodes[5].is_up = false;

        let mut round_robin_nodes = vec![];

        for _ in 0..nodes.iter().filter(|node| node.rack == "rack1").count() - 1 {
            round_robin_nodes.push(
                node_pool
                    .get_round_robin_node_in_dc_rack("rack1")
                    .address
                    .to_string(),
            );
        }

        // only includes up nodes in round robin
        assert_eq!(
            vec![
                "172.16.1.2:9044",
                "172.16.1.4:9044",
                "172.16.1.6:9044",
                "172.16.1.7:9044",
                "172.16.1.0:9044",
                "172.16.1.2:9044",
                "172.16.1.4:9044",
            ],
            round_robin_nodes
        );

        node_pool.nodes[1].is_up = true;
        node_pool.nodes[3].is_up = true;
        node_pool.nodes[5].is_up = true;

        round_robin_nodes.clear();

        for _ in 0..nodes.iter().filter(|node| node.rack == "rack1").count() - 1 {
            round_robin_nodes.push(
                node_pool
                    .get_round_robin_node_in_dc_rack("rack1")
                    .address
                    .to_string(),
            );
        }

        // includes the new up nodes in round robin
        assert_eq!(
            vec![
                "172.16.1.3:9044",
                "172.16.1.4:9044",
                "172.16.1.5:9044",
                "172.16.1.6:9044",
                "172.16.1.7:9044",
                "172.16.1.0:9044",
                "172.16.1.1:9044"
            ],
            round_robin_nodes
        );
    }

    fn prepare_nodes() -> Vec<CassandraNode> {
        vec![
            // rack 1 nodes
            CassandraNode::new(
                "172.16.1.0:9044".parse().unwrap(),
                "rack1".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.1:9044".parse().unwrap(),
                "rack1".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.2:9044".parse().unwrap(),
                "rack1".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.3:9044".parse().unwrap(),
                "rack1".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.4:9044".parse().unwrap(),
                "rack1".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.5:9044".parse().unwrap(),
                "rack1".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.6:9044".parse().unwrap(),
                "rack1".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.7:9044".parse().unwrap(),
                "rack1".into(),
                vec![],
                Uuid::new_v4(),
            ),
            // rack 2 nodes
            CassandraNode::new(
                "172.16.2.0:9044".parse().unwrap(),
                "rack2".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.2.1:9044".parse().unwrap(),
                "rack2".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.2.2:9044".parse().unwrap(),
                "rack2".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.2.3:9044".parse().unwrap(),
                "rack2".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.2.4:9044".parse().unwrap(),
                "rack2".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.2.5:9044".parse().unwrap(),
                "rack2".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.2.6:9044".parse().unwrap(),
                "rack2".into(),
                vec![],
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.2.7:9044".parse().unwrap(),
                "rack2".into(),
                vec![],
                Uuid::new_v4(),
            ),
        ]
    }
}
