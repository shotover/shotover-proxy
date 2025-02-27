use super::KeyspaceChanRx;
use super::connection::CassandraConnection;
use super::node::{CassandraNode, ConnectionFactory};
use super::routing_key::calculate_routing_key;
use super::token_ring::TokenRing;
use anyhow::{Context, Error, Result, anyhow};
use cassandra_protocol::frame::message_execute::BodyReqExecuteOwned;
use cassandra_protocol::types::CBytesShort;
use metrics::{Counter, counter};
use rand::prelude::*;
use std::sync::Arc;
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::{RwLock, watch};

#[derive(Debug, Clone)]
pub struct PreparedMetadata {
    pub pk_indexes: Vec<i16>,
    pub keyspace: Option<String>,
}

#[derive(Debug)]
pub enum GetReplicaErr {
    NoPreparedMetadata,
    NoKeyspaceMetadata,
    NoNodeAvailable(anyhow::Error),
    Other(Error),
    NoRoutingKey,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationStrategy {
    SimpleStrategy,
    NetworkTopologyStrategy,
    LocalStrategy,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyspaceMetadata {
    pub replication_factor: usize,
    pub replication_strategy: ReplicationStrategy,
}

// Values in the builder are shared between transform instances that come from the same transform in the topology.yaml
#[derive(Clone)]
pub struct NodePoolBuilder {
    prepared_metadata: Arc<RwLock<HashMap<CBytesShort, Arc<PreparedMetadata>>>>,
    out_of_rack_requests: Counter,
}

impl NodePoolBuilder {
    pub fn new(chain_name: String) -> Self {
        Self {
            prepared_metadata: Arc::new(RwLock::new(HashMap::new())),
            out_of_rack_requests: counter!("shotover_out_of_rack_requests_count", "chain" => chain_name, "transform" => "CassandraSinkCluster"),
        }
    }

    pub fn build(&self) -> NodePool {
        NodePool {
            prepared_metadata: self.prepared_metadata.clone(),
            keyspace_metadata: HashMap::new(),
            token_map: TokenRing::new(&[]),
            nodes: vec![],
            out_of_rack_requests: self.out_of_rack_requests.clone(),
        }
    }
}

pub struct NodePool {
    prepared_metadata: Arc<RwLock<HashMap<CBytesShort, Arc<PreparedMetadata>>>>,
    keyspace_metadata: HashMap<String, KeyspaceMetadata>,
    token_map: TokenRing,
    nodes: Vec<CassandraNode>,
    out_of_rack_requests: Counter,
}

impl NodePool {
    pub fn nodes_mut(&mut self) -> &mut [CassandraNode] {
        &mut self.nodes
    }

    pub fn nodes(&self) -> &[CassandraNode] {
        &self.nodes
    }

    /// if the node list has been updated use the new list, copying over any existing connections
    pub fn update_nodes(&mut self, nodes_rx: &mut watch::Receiver<Vec<CassandraNode>>) {
        let mut new_nodes = nodes_rx.borrow_and_update().clone();

        for node in self.nodes.drain(..) {
            if let Some(outbound) = node.outbound {
                for new_node in &mut new_nodes {
                    if new_node.host_id == node.host_id && new_node.is_up {
                        new_node.outbound = Some(outbound);
                        break;
                    }
                }
            }
        }
        self.nodes = new_nodes;
        self.token_map = TokenRing::new(self.nodes.as_slice());
        tracing::debug!(
            "nodes updated, nodes={:#?}\ntokens={:#?}",
            self.nodes,
            self.token_map
        );
    }

    pub fn update_keyspaces(&mut self, keyspaces_rx: &mut KeyspaceChanRx) {
        let updated_keyspaces = keyspaces_rx.borrow_and_update().clone();
        self.keyspace_metadata = updated_keyspaces;
    }

    pub async fn add_prepared_result(&mut self, id: CBytesShort, metadata: PreparedMetadata) {
        let mut write_lock = self.prepared_metadata.write().await;
        write_lock.insert(id, Arc::new(metadata));
    }

    pub async fn get_random_node_in_dc_rack(
        &mut self,
        rack: &str,
        rng: &mut SmallRng,
        connection_factory: &ConnectionFactory,
    ) -> Result<&mut CassandraNode> {
        let mut nodes: Vec<_> = self
            .nodes
            .iter_mut()
            .filter(|node| node.is_up && node.rack == *rack)
            .collect();
        nodes.shuffle(rng);
        get_accessible_node(connection_factory, nodes)
            .await
            .with_context(|| {
                format!("Failed to open a connection to any nodes in the rack {rack:?}")
            })
    }

    pub async fn get_random_connection_in_dc_rack(
        &mut self,
        rack: &str,
        rng: &mut SmallRng,
        connection_factory: &ConnectionFactory,
    ) -> Result<&mut CassandraConnection> {
        self.get_random_node_in_dc_rack(rack, rng, connection_factory)
            .await
            .map(|x| {
                x.outbound
                    .as_mut()
                    .expect("it is set to Some by get_random_node_in_dc_rack")
            })
    }

    pub async fn get_random_owned_connection_in_dc_rack(
        &mut self,
        rack: &str,
        rng: &mut SmallRng,
        connection_factory: &ConnectionFactory,
    ) -> Result<(CassandraConnection, SocketAddr)> {
        self.get_random_node_in_dc_rack(rack, rng, connection_factory)
            .await
            .map(|x| {
                (
                    x.outbound
                        .take()
                        .expect("it is set to Some by get_random_node_in_dc_rack"),
                    x.address,
                )
            })
    }

    /// Get a token routed replica node for the supplied execute message (if exists)
    /// Will attempt to get a replica in the supplied rack if exists, otherwise get one in
    /// the same data center
    pub async fn get_replica_node_in_dc(
        &mut self,
        execute: &BodyReqExecuteOwned,
        rack: &str,
        rng: &mut SmallRng,
    ) -> Result<Vec<&mut CassandraNode>, GetReplicaErr> {
        let metadata = {
            let read_lock = self.prepared_metadata.read().await;
            read_lock
                .get(&execute.id)
                .ok_or(GetReplicaErr::NoPreparedMetadata)?
                .clone()
        };

        let keyspace = self
            .keyspace_metadata
            .get(
                metadata
                    .keyspace
                    .as_ref()
                    .ok_or(GetReplicaErr::NoKeyspaceMetadata)?,
            )
            .ok_or(GetReplicaErr::NoKeyspaceMetadata)?;

        let routing_key = calculate_routing_key(
            &metadata.pk_indexes,
            execute.query_parameters.values.as_ref().ok_or_else(|| {
                GetReplicaErr::Other(anyhow!("Execute body does not have query parameters"))
            })?,
        )
        .ok_or(GetReplicaErr::NoRoutingKey)?;

        let replica_host_ids = self
            .token_map
            .iter_replica_nodes(self.nodes(), routing_key, keyspace)
            .collect::<Vec<uuid::Uuid>>();

        let mut nodes: Vec<&mut CassandraNode> = self
            .nodes
            .iter_mut()
            .filter(|node| replica_host_ids.contains(&node.host_id) && node.is_up)
            .collect();
        nodes.shuffle(rng);

        // Move all nodes that are in the rack to the front of the list.
        // This way they will be preferred over all other nodes
        let mut nodes_found_in_rack = 0;
        for i in 0..nodes.len() {
            if nodes[i].rack == rack {
                nodes.swap(i, nodes_found_in_rack);
                nodes_found_in_rack += 1;
            }
        }
        if nodes_found_in_rack == 0 {
            // An execute message is being delivered outside of CassandraSinkCluster's designated rack. The only cases this can occur is when:
            // The client correctly routes to the shotover node that reports it has the token in its rack, however the destination cassandra node has since gone down and is now inaccessible.
            // or
            // ReplicationStrategy::SimpleStrategy is used with a replication factor > 1
            // or
            // The clients token aware routing is broken.
            #[cfg(debug_assertions)]
            tracing::warn!(
                "No suitable nodes to route to found within rack. This error only occurs in debug builds as it should never occur in an ideal integration test situation."
            );
            self.out_of_rack_requests.increment(1);
        }
        tracing::debug!(
            "Shotover with designated rack {rack:?} found replica nodes {replica_host_ids:?}"
        );

        Ok(nodes)
    }

    pub async fn get_replica_connection_in_dc(
        &mut self,
        execute: &BodyReqExecuteOwned,
        rack: &str,
        rng: &mut SmallRng,
        connection_factory: &ConnectionFactory,
    ) -> Result<&mut CassandraConnection, GetReplicaErr> {
        let nodes = self.get_replica_node_in_dc(execute, rack, rng).await?;

        get_accessible_node(connection_factory, nodes)
            .await
            .context("Failed to open a connection to any replicas of a specific token")
            .map_err(GetReplicaErr::NoNodeAvailable)
            .map(|x| {
                x.outbound
                    .as_mut()
                    .expect("it is set to Some by get_accessible_node")
            })
    }
}

pub struct AddressError {
    pub address: SocketAddr,
    pub error: anyhow::Error,
}

pub fn bullet_list_of_node_failures<'a, I: Iterator<Item = &'a AddressError>>(errors: I) -> String {
    let mut node_errors = String::new();
    for AddressError { error, address } in errors {
        node_errors.push_str(&format!("\n* {address:?}:"));
        for sub_error in error.chain() {
            node_errors.push_str(&format!("\n    - {sub_error}"));
        }
    }
    node_errors
}

pub async fn get_accessible_node<'a>(
    connection_factory: &ConnectionFactory,
    nodes: Vec<&'a mut CassandraNode>,
) -> Result<&'a mut CassandraNode> {
    let mut errors = vec![];
    for node in nodes {
        match node.get_connection(connection_factory).await {
            Ok(_) => {
                if !errors.is_empty() {
                    let node_errors = bullet_list_of_node_failures(errors.iter());
                    tracing::warn!(
                        "A successful connection to a node was made but attempts to connect to these nodes failed first:{node_errors}"
                    );
                }
                return Ok(node);
            }
            Err(error) => {
                node.report_issue();
                errors.push(AddressError {
                    error,
                    address: node.address,
                });
            }
        }
    }

    let node_errors = bullet_list_of_node_failures(errors.iter());
    Err(anyhow!(
        "Attempted to open a connection to one of multiple nodes but all attempts failed:{node_errors}"
    ))
}

pub async fn get_accessible_owned_connection(
    connection_factory: &ConnectionFactory,
    nodes: Vec<&'_ mut CassandraNode>,
) -> Result<(CassandraConnection, SocketAddr)> {
    get_accessible_node(connection_factory, nodes)
        .await
        .map(|x| {
            (
                x.outbound
                    .take()
                    .expect("it is set to Some by get_random_node_in_dc_rack"),
                x.address,
            )
        })
}
