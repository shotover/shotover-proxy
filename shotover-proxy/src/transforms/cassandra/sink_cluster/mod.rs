use crate::error::ChainResponse;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{IntSize, Message, MessageValue, Messages};
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::cassandra::connection::CassandraConnection;
use crate::transforms::util::Response;
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use cassandra_protocol::frame::Version;
use cassandra_protocol::query::QueryParams;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{FQName, Identifier};
use cql3_parser::select::SelectElement;
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use itertools::Itertools;
use metrics::{register_counter, Counter};
use node::{CassandraNode, ConnectionFactory};
use rand::prelude::*;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, RwLock};
use topology::{create_topology_task, TaskConnectionInfo};
use uuid::Uuid;
use version_compare::Cmp;

pub mod node;
pub mod topology;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraSinkClusterConfig {
    /// contact points must be within the specified data_center and rack.
    /// If this is not followed, shotover's invariants will still be upheld but shotover will communicate with a
    /// node outside of the specified data_center and rack.
    pub first_contact_points: Vec<String>,
    pub local_shotover_host_id: Uuid,
    pub shotover_nodes: Vec<ShotoverNode>,
    pub tls: Option<TlsConnectorConfig>,
    pub read_timeout: Option<u64>,
}

impl CassandraSinkClusterConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<Transforms> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        let mut shotover_nodes = self.shotover_nodes.clone();
        let index = self
            .shotover_nodes
            .iter()
            .position(|x| x.host_id == self.local_shotover_host_id)
            .ok_or_else(|| {
                anyhow!(
                    "local host_id {} was missing in shotover_nodes",
                    self.local_shotover_host_id
                )
            })?;
        let local_node = shotover_nodes.remove(index);

        Ok(Transforms::CassandraSinkCluster(Box::new(
            CassandraSinkCluster::new(
                self.first_contact_points.clone(),
                shotover_nodes,
                chain_name,
                local_node,
                tls,
                self.read_timeout,
            ),
        )))
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct ShotoverNode {
    pub address: SocketAddr,
    pub data_center: String,
    pub rack: String,
    pub host_id: Uuid,
}

pub struct CassandraSinkCluster {
    contact_points: Vec<String>,

    connection_factory: ConnectionFactory,

    shotover_peers: Vec<ShotoverNode>,
    init_handshake_connection: Option<CassandraConnection>,
    init_handshake_address: Option<SocketAddr>,
    init_handshake_complete: bool,

    chain_name: String,
    failed_requests: Counter,
    read_timeout: Option<Duration>,
    local_table: FQName,
    peers_table: FQName,
    peers_v2_table: FQName,
    local_shotover_node: ShotoverNode,
    /// A local clone of topology_task_nodes.
    /// Internally stores connections to the nodes.
    ///
    /// Can be populated at two points:
    /// 1. If topology_task_nodes is already populated when the very first message is received then we populate local_nodes immediately.
    /// 2. Otherwise local_nodes will be populated immediately after we have a confirmed successful handshake, waiting until topology_task_nodes is populated.
    local_nodes: Vec<CassandraNode>,
    /// Only written to by the topology task
    /// Transform instances should never write to this.
    topology_task_nodes: Arc<RwLock<Vec<CassandraNode>>>,
    rng: SmallRng,
    task_handshake_tx: mpsc::Sender<TaskConnectionInfo>,
}

impl Clone for CassandraSinkCluster {
    fn clone(&self) -> Self {
        Self {
            contact_points: self.contact_points.clone(),
            shotover_peers: self.shotover_peers.clone(),
            init_handshake_connection: None,
            connection_factory: self.connection_factory.new_with_same_config(),
            init_handshake_address: None,
            init_handshake_complete: false,
            chain_name: self.chain_name.clone(),
            failed_requests: self.failed_requests.clone(),
            read_timeout: self.read_timeout,
            local_table: self.local_table.clone(),
            peers_table: self.peers_table.clone(),
            peers_v2_table: self.peers_v2_table.clone(),
            local_shotover_node: self.local_shotover_node.clone(),
            local_nodes: vec![],
            topology_task_nodes: self.topology_task_nodes.clone(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            task_handshake_tx: self.task_handshake_tx.clone(),
        }
    }
}

impl CassandraSinkCluster {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        contact_points: Vec<String>,
        shotover_peers: Vec<ShotoverNode>,
        chain_name: String,
        local_shotover_node: ShotoverNode,
        tls: Option<TlsConnector>,
        timeout: Option<u64>,
    ) -> Self {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name.clone(), "transform" => "CassandraSinkCluster");
        let receive_timeout = timeout.map(Duration::from_secs);

        let nodes_shared = Arc::new(RwLock::new(vec![]));

        let (task_handshake_tx, task_handshake_rx) = mpsc::channel(1);

        create_topology_task(
            nodes_shared.clone(),
            task_handshake_rx,
            local_shotover_node.data_center.clone(),
        );

        Self {
            contact_points,
            connection_factory: ConnectionFactory::new(tls),
            shotover_peers,
            init_handshake_connection: None,
            init_handshake_address: None,
            init_handshake_complete: false,
            chain_name,
            failed_requests,
            read_timeout: receive_timeout,
            local_table: FQName::new("system", "local"),
            peers_table: FQName::new("system", "peers"),
            peers_v2_table: FQName::new("system", "peers_v2"),
            local_shotover_node,
            local_nodes: vec![],
            topology_task_nodes: nodes_shared,
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            task_handshake_tx,
        }
    }
}

fn create_query(messages: &Messages, query: String, version: Version) -> Result<Message> {
    let stream_id = get_unused_stream_id(messages)?;
    Ok(Message::from_frame(Frame::Cassandra(CassandraFrame {
        version,
        stream_id,
        tracing_id: None,
        warnings: vec![],
        operation: CassandraOperation::Query {
            query: Box::new(CassandraStatement::Unknown(query)),
            params: Box::new(QueryParams::default()),
        },
    })))
}

impl CassandraSinkCluster {
    async fn send_message(&mut self, mut messages: Messages) -> ChainResponse {
        let tables_to_rewrite: Vec<TableToRewrite> = messages
            .iter_mut()
            .enumerate()
            .filter_map(|(i, m)| self.get_rewrite_table(m, i))
            .collect();

        for table_to_rewrite in tables_to_rewrite.iter().rev() {
            let query = "SELECT rack, data_center, schema_version, tokens, release_version FROM system.peers";
            messages.insert(
                table_to_rewrite.index + 1,
                create_query(&messages, query.into(), table_to_rewrite.version)?,
            );
            if let RewriteTableTy::Peers = table_to_rewrite.ty {
                let query = "SELECT rack, data_center, schema_version, tokens, release_version FROM system.local";
                messages.insert(
                    table_to_rewrite.index + 2,
                    create_query(&messages, query.into(), table_to_rewrite.version)?,
                );
            }
        }

        // Create the initial connection.
        // Messages will be sent through this connection until we have extracted the handshake.
        if self.init_handshake_connection.is_none() {
            if self.local_nodes.is_empty() {
                let nodes_shared = self.topology_task_nodes.read().await;
                self.local_nodes = nodes_shared.clone();
            }

            let random_point = if self.local_nodes.is_empty() {
                tokio::net::lookup_host(self.contact_points.choose(&mut self.rng).unwrap())
                    .await?
                    .next()
                    .unwrap()
            } else {
                self.get_random_node_in_dc_rack().address
            };
            self.init_handshake_connection =
                Some(self.connection_factory.new_connection(random_point).await?);
            self.init_handshake_address = Some(random_point);
        }

        if !self.init_handshake_complete {
            for message in &mut messages {
                // Filter operation types so we are only left with messages relevant to the handshake.
                // Due to shotover pipelining we could receive non-handshake messages while !self.init_handshake_complete.
                // Despite being used by the client in a handshake, CassandraOperation::Options is not included
                // because it doesnt alter the state of the server and so it isnt needed.
                if let Some(Frame::Cassandra(CassandraFrame {
                    operation: CassandraOperation::Startup(_) | CassandraOperation::AuthResponse(_),
                    ..
                })) = message.frame()
                {
                    self.connection_factory
                        .push_handshake_message(message.clone());
                }
            }
        }

        let mut responses_future = FuturesOrdered::new();
        let mut responses_future_use = FuturesOrdered::new();
        let mut use_future_index_to_node_index = vec![];
        for mut message in messages {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            if self.local_nodes.is_empty()
                || !self.init_handshake_complete
                // system.local and system.peers must be routed to the same node otherwise the system.local node will be amongst the system.peers nodes and a node will be missing
                // DDL statements and system.local must be routed through the same connection, so that schema_version changes appear immediately in system.local
                || is_ddl_statement(&mut message)
                || self.is_system_local_or_peers(&mut message)
            {
                self.init_handshake_connection.as_mut().unwrap()
            } else if is_use_statement(&mut message) {
                // Adding the USE statement to the handshake ensures that any new connection
                // created will have the correct keyspace setup.
                self.connection_factory.set_use_message(message.clone());

                // Send the USE statement to all open connections to ensure they are all in sync
                for (node_index, node) in self.local_nodes.iter().enumerate() {
                    if let Some(outbound) = &node.outbound {
                        let (return_chan_tx, return_chan_rx) = oneshot::channel();
                        outbound.send(message.clone(), return_chan_tx)?;
                        responses_future_use.push_back(return_chan_rx);
                        use_future_index_to_node_index.push(node_index);
                    }
                }

                // Send the USE statement to the handshake connection and use the response as shotovers response
                self.init_handshake_connection.as_mut().unwrap()
            } else {
                // We have a full nodes list and handshake, so we can do proper routing now.
                let random_node = self.local_nodes.choose_mut(&mut self.rng).unwrap();
                random_node.get_connection(&self.connection_factory).await?
            }
            .send(message, return_chan_tx)?;

            responses_future.push_back(return_chan_rx)
        }

        let mut responses =
            super::connection::receive(self.read_timeout, &self.failed_requests, responses_future)
                .await?;

        // When the server indicates that it is ready for normal operation via Ready or AuthSuccess,
        // we have succesfully collected an entire handshake so we mark the handshake as complete.
        if !self.init_handshake_complete {
            for response in &mut responses {
                if let Some(Frame::Cassandra(CassandraFrame {
                    operation: CassandraOperation::Ready(_) | CassandraOperation::AuthSuccess(_),
                    ..
                })) = response.frame()
                {
                    self.complete_handshake().await?;
                    break;
                }
            }
        }

        for node_index in use_future_index_to_node_index {
            let response = responses_future_use
                .next()
                .await
                .map(|x| x.map_err(|e| anyhow!(e)));
            // If any errors occurred close the connection as we can no
            // longer make any guarantees about the current state of the connection
            if !is_use_statement_successful(response) {
                self.local_nodes[node_index].outbound = None;
            }
        }

        for table_to_rewrite in tables_to_rewrite {
            self.rewrite_table(table_to_rewrite, &mut responses).await?;
        }

        Ok(responses)
    }

    async fn complete_handshake(&mut self) -> Result<()> {
        // Only send a handshake if the task really needs it
        // i.e. when the channel of size 1 is empty
        if let Ok(permit) = self.task_handshake_tx.try_reserve() {
            permit.send(TaskConnectionInfo {
                connection_factory: self.connection_factory.clone(),
                address: self.init_handshake_address.unwrap(),
            })
        }
        self.init_handshake_complete = true;

        if self.local_nodes.is_empty() {
            self.populate_local_nodes().await?;

            // If we have to populate the local_nodes at this point then that means the control connection
            // may not have been made against a node in the configured data_center/rack.
            // Therefore we need to recreate the control connection to ensure that it is in the configured data_center/rack.
            let random_address = self.get_random_node_in_dc_rack().address;
            self.init_handshake_connection = Some(
                self.connection_factory
                    .new_connection(random_address)
                    .await?,
            );
        }

        Ok(())
    }

    async fn populate_local_nodes(&mut self) -> Result<()> {
        let start = Instant::now();
        loop {
            if self.local_nodes.is_empty() {
                let nodes_shared = self.topology_task_nodes.read().await;
                self.local_nodes = nodes_shared.clone();
            }

            if !self.local_nodes.is_empty() {
                return Ok(());
            }

            if start.elapsed() > Duration::from_secs(10 * 60) {
                return Err(anyhow!(
                    "10 minute timeout waiting for topology task elapsed"
                ));
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn get_random_node_in_dc_rack(&mut self) -> &CassandraNode {
        self.local_nodes
            .iter()
            .filter(|x| x.rack == self.local_shotover_node.rack)
            .choose(&mut self.rng)
            .unwrap()
    }

    fn get_rewrite_table(&self, request: &mut Message, index: usize) -> Option<TableToRewrite> {
        if let Some(Frame::Cassandra(cassandra)) = request.frame() {
            // No need to handle Batch as selects can only occur on Query
            if let CassandraOperation::Query { query, .. } = &cassandra.operation {
                if let CassandraStatement::Select(select) = query.as_ref() {
                    let ty = if self.local_table == select.table_name {
                        RewriteTableTy::Local
                    } else if self.peers_table == select.table_name
                        || self.peers_v2_table == select.table_name
                    {
                        // TODO: fail if WHERE exists
                        RewriteTableTy::Peers
                    } else {
                        return None;
                    };

                    return Some(TableToRewrite {
                        index,
                        ty,
                        version: cassandra.version,
                        selects: select.columns.clone(),
                    });
                }
            }
        }
        None
    }

    async fn rewrite_table(
        &mut self,
        table: TableToRewrite,
        responses: &mut Vec<Message>,
    ) -> Result<()> {
        if table.index + 1 < responses.len() {
            let peers_response = responses.remove(table.index + 1);
            match table.ty {
                RewriteTableTy::Local => {
                    if let Some(local_response) = responses.get_mut(table.index) {
                        self.rewrite_table_local(table, local_response, peers_response)
                            .await?;
                        local_response.invalidate_cache();
                    }
                }
                RewriteTableTy::Peers => {
                    if table.index + 1 < responses.len() {
                        let local_response = responses.remove(table.index + 1);
                        if let Some(client_peers_response) = responses.get_mut(table.index) {
                            let mut nodes = parse_system_nodes(peers_response)?;
                            nodes.extend(parse_system_nodes(local_response)?);

                            self.rewrite_table_peers(table, client_peers_response, nodes)
                                .await?;
                            client_peers_response.invalidate_cache();
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn rewrite_table_peers(
        &mut self,
        table: TableToRewrite,
        peers_response: &mut Message,
        nodes: Vec<NodeInfo>,
    ) -> Result<()> {
        let mut data_center_alias = "data_center";
        let mut rack_alias = "rack";
        let mut host_id_alias = "host_id";
        let mut native_address_alias = "native_address";
        let mut native_port_alias = "native_port";
        let mut preferred_ip_alias = "preferred_ip";
        let mut preferred_port_alias = "preferred_port";
        let mut rpc_address_alias = "rpc_address";
        let mut peer_alias = "peer";
        let mut peer_port_alias = "peer_port";
        let mut release_version_alias = "release_version";
        let mut tokens_alias = "tokens";
        let mut schema_version_alias = "schema_version";
        for select in &table.selects {
            if let SelectElement::Column(column) = select {
                if let Some(alias) = &column.alias {
                    let alias = match alias {
                        Identifier::Unquoted(alias) => alias,
                        Identifier::Quoted(alias) => alias,
                    };
                    if column.name == Identifier::Unquoted("data_center".to_string()) {
                        data_center_alias = alias;
                    } else if column.name == Identifier::Unquoted("rack".to_string()) {
                        rack_alias = alias;
                    } else if column.name == Identifier::Unquoted("host_id".to_string()) {
                        host_id_alias = alias;
                    } else if column.name == Identifier::Unquoted("native_address".to_string()) {
                        native_address_alias = alias;
                    } else if column.name == Identifier::Unquoted("native_port".to_string()) {
                        native_port_alias = alias;
                    } else if column.name == Identifier::Unquoted("preferred_ip".to_string()) {
                        preferred_ip_alias = alias;
                    } else if column.name == Identifier::Unquoted("preferred_port".to_string()) {
                        preferred_port_alias = alias;
                    } else if column.name == Identifier::Unquoted("rpc_address".to_string()) {
                        rpc_address_alias = alias;
                    } else if column.name == Identifier::Unquoted("peer".to_string()) {
                        peer_alias = alias;
                    } else if column.name == Identifier::Unquoted("peer_port".to_string()) {
                        peer_port_alias = alias;
                    } else if column.name == Identifier::Unquoted("release_version".to_string()) {
                        release_version_alias = alias;
                    } else if column.name == Identifier::Unquoted("tokens".to_string()) {
                        tokens_alias = alias;
                    } else if column.name == Identifier::Unquoted("schema_version".to_string()) {
                        schema_version_alias = alias;
                    }
                }
            }
        }

        if let Some(Frame::Cassandra(frame)) = peers_response.frame() {
            if let CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                metadata,
            }) = &mut frame.operation
            {
                *rows = self
                    .shotover_peers
                    .iter()
                    .map(|shotover_peer| {
                        let mut release_version = "".to_string();
                        let mut schema_version = None;
                        let mut tokens = vec![];
                        for node in &nodes {
                            if node.data_center == shotover_peer.data_center
                                && node.rack == shotover_peer.rack
                            {
                                if release_version.is_empty() {
                                    release_version = node.release_version.clone();
                                }
                                if let Ok(Cmp::Lt) = version_compare::compare(
                                    &node.release_version,
                                    &release_version,
                                ) {
                                    release_version = node.release_version.clone();
                                }

                                match &mut schema_version {
                                    Some(schema_version) => {
                                        if &node.schema_version != schema_version {
                                            *schema_version = Uuid::new_v4();
                                        }
                                    }
                                    None => schema_version = Some(node.schema_version),
                                }
                                tokens.extend(node.tokens.iter().cloned());
                            }
                        }
                        tokens.sort();

                        metadata
                            .col_specs
                            .iter()
                            .map(|colspec| {
                                if colspec.name == data_center_alias {
                                    MessageValue::Varchar(shotover_peer.data_center.clone())
                                } else if colspec.name == rack_alias {
                                    MessageValue::Varchar(shotover_peer.rack.clone())
                                } else if colspec.name == host_id_alias {
                                    MessageValue::Uuid(shotover_peer.host_id)
                                } else if colspec.name == preferred_ip_alias
                                    || colspec.name == preferred_port_alias
                                    || colspec.name == rpc_address_alias
                                {
                                    MessageValue::Null
                                } else if colspec.name == native_address_alias {
                                    MessageValue::Inet(shotover_peer.address.ip())
                                } else if colspec.name == native_port_alias {
                                    MessageValue::Integer(
                                        shotover_peer.address.port() as i64,
                                        IntSize::I32,
                                    )
                                } else if colspec.name == peer_alias {
                                    MessageValue::Inet(shotover_peer.address.ip())
                                } else if colspec.name == peer_port_alias {
                                    MessageValue::Integer(7000, IntSize::I32)
                                } else if colspec.name == release_version_alias {
                                    MessageValue::Varchar(release_version.clone())
                                } else if colspec.name == tokens_alias {
                                    MessageValue::List(tokens.clone())
                                } else if colspec.name == schema_version_alias {
                                    MessageValue::Uuid(schema_version.unwrap_or_else(Uuid::new_v4))
                                } else {
                                    tracing::warn!(
                                        "Unknown column name in system.peers/system.peers_v2: {}",
                                        colspec.name
                                    );
                                    MessageValue::Null
                                }
                            })
                            .collect()
                    })
                    .collect();
            }
            Ok(())
        } else {
            Err(anyhow!(
                "Failed to parse system.local response {:?}",
                peers_response
            ))
        }
    }

    async fn rewrite_table_local(
        &mut self,
        table: TableToRewrite,
        local_response: &mut Message,
        peers_response: Message,
    ) -> Result<()> {
        let mut peers = parse_system_nodes(peers_response)?;
        peers.retain(|node| {
            node.data_center == self.local_shotover_node.data_center
                && node.rack == self.local_shotover_node.rack
        });

        let mut release_version_alias = "release_version";
        let mut tokens_alias = "tokens";
        let mut schema_version_alias = "schema_version";
        let mut broadcast_address_alias = "broadcast_address";
        let mut listen_address_alias = "listen_address";
        let mut host_id_alias = "host_id";
        for select in &table.selects {
            if let SelectElement::Column(column) = select {
                if let Some(alias) = &column.alias {
                    let alias = match alias {
                        Identifier::Unquoted(alias) => alias,
                        Identifier::Quoted(alias) => alias,
                    };
                    if column.name == Identifier::Unquoted("release_version".to_string()) {
                        release_version_alias = alias;
                    } else if column.name == Identifier::Unquoted("tokens".to_string()) {
                        tokens_alias = alias;
                    } else if column.name == Identifier::Unquoted("schema_version".to_string()) {
                        schema_version_alias = alias;
                    } else if column.name == Identifier::Unquoted("broadcast_address".to_string()) {
                        broadcast_address_alias = alias;
                    } else if column.name == Identifier::Unquoted("listen_address".to_string()) {
                        listen_address_alias = alias;
                    } else if column.name == Identifier::Unquoted("host_id".to_string()) {
                        host_id_alias = alias;
                    }
                }
            }
        }

        if let Some(Frame::Cassandra(frame)) = local_response.frame() {
            if let CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                metadata,
            }) = &mut frame.operation
            {
                // The local_response message is guaranteed to come from a node that is in our configured data_center/rack.
                // That means we can leave fields like rack and data_center alone and get exactly what we want.
                for row in rows {
                    for (col, col_meta) in row.iter_mut().zip(metadata.col_specs.iter()) {
                        if col_meta.name == release_version_alias {
                            if let MessageValue::Varchar(release_version) = col {
                                for peer in &peers {
                                    if let Ok(Cmp::Lt) = version_compare::compare(
                                        &peer.release_version,
                                        &release_version,
                                    ) {
                                        *release_version = peer.release_version.clone();
                                    }
                                }
                            }
                        } else if col_meta.name == tokens_alias {
                            if let MessageValue::List(tokens) = col {
                                for peer in &peers {
                                    tokens.extend(peer.tokens.iter().cloned());
                                }
                                tokens.sort();
                            }
                        } else if col_meta.name == schema_version_alias {
                            if let MessageValue::Uuid(schema_version) = col {
                                for peer in &peers {
                                    if schema_version != &peer.schema_version {
                                        *schema_version = Uuid::new_v4();
                                        break;
                                    }
                                }
                            }
                        } else if col_meta.name == broadcast_address_alias
                            || col_meta.name == listen_address_alias
                        {
                            if let MessageValue::Inet(address) = col {
                                *address = self.local_shotover_node.address.ip();
                            }
                        } else if col_meta.name == host_id_alias {
                            if let MessageValue::Uuid(host_id) = col {
                                *host_id = self.local_shotover_node.host_id;
                            }
                        }
                    }
                }
            }
            Ok(())
        } else {
            Err(anyhow!(
                "Failed to parse system.local response {:?}",
                local_response
            ))
        }
    }

    // TODO: handle use statement state
    fn is_system_local_or_peers(&self, request: &mut Message) -> bool {
        if let Some(Frame::Cassandra(frame)) = request.frame() {
            if let CassandraOperation::Query { query, .. } = &mut frame.operation {
                if let CassandraStatement::Select(select) = query.as_ref() {
                    return self.local_table == select.table_name
                        || self.peers_table == select.table_name
                        || self.peers_v2_table == select.table_name;
                }
            }
        }
        false
    }
}

struct TableToRewrite {
    index: usize,
    ty: RewriteTableTy,
    version: Version,
    selects: Vec<SelectElement>,
}

enum RewriteTableTy {
    Local,
    Peers,
}

fn is_use_statement(request: &mut Message) -> bool {
    if let Some(Frame::Cassandra(frame)) = request.frame() {
        if let CassandraOperation::Query { query, .. } = &mut frame.operation {
            if let CassandraStatement::Use(_) = query.as_ref() {
                return true;
            }
        }
    }
    false
}

fn is_ddl_statement(request: &mut Message) -> bool {
    if let Some(Frame::Cassandra(frame)) = request.frame() {
        if let CassandraOperation::Query { query, .. } = &mut frame.operation {
            if let CassandraStatement::CreateAggregate(_)
            | CassandraStatement::CreateFunction(_)
            | CassandraStatement::CreateIndex(_)
            | CassandraStatement::CreateKeyspace(_)
            | CassandraStatement::CreateMaterializedView(_)
            | CassandraStatement::CreateRole(_)
            | CassandraStatement::CreateTable(_)
            | CassandraStatement::CreateTrigger(_)
            | CassandraStatement::CreateType(_)
            | CassandraStatement::CreateUser(_)
            | CassandraStatement::AlterKeyspace(_)
            | CassandraStatement::AlterMaterializedView(_)
            | CassandraStatement::AlterRole(_)
            | CassandraStatement::AlterTable(_)
            | CassandraStatement::AlterType(_)
            | CassandraStatement::AlterUser(_) = query.as_ref()
            {
                return true;
            }
        }
    }
    false
}

fn is_use_statement_successful(response: Option<Result<Response>>) -> bool {
    if let Some(Ok(Response {
        response: Ok(mut response),
        ..
    })) = response
    {
        if let Some(Frame::Cassandra(CassandraFrame {
            operation: CassandraOperation::Result(CassandraResult::SetKeyspace(_)),
            ..
        })) = response.frame()
        {
            return true;
        }
    }
    false
}

fn get_unused_stream_id(messages: &Messages) -> Result<i16> {
    // start at an unusual number to hopefully avoid looping many times when we receive stream ids that look like [0, 1, 2, ..]
    // We can quite happily give up 358 stream ids as that still allows for shotover message batches containing 2 ** 16 - 358 = 65178 messages
    for i in 358..i16::MAX {
        if !messages
            .iter()
            .filter_map(|message| message.stream_id())
            .contains(&i)
        {
            return Ok(i);
        }
    }
    Err(anyhow!("Ran out of stream ids"))
}

struct NodeInfo {
    tokens: Vec<MessageValue>,
    schema_version: Uuid,
    release_version: String,
    rack: String,
    data_center: String,
}

fn parse_system_nodes(mut response: Message) -> Result<Vec<NodeInfo>> {
    if let Some(Frame::Cassandra(frame)) = response.frame() {
        match &mut frame.operation {
            CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                ..
            }) => rows
                .iter_mut()
                .map(|row| {
                    if row.len() != 5 {
                        return Err(anyhow!("expected 5 columns but was {}", row.len()));
                    }

                    let release_version = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("release_version not a varchar"));
                    };

                    let tokens = if let Some(MessageValue::List(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("tokens not a list"));
                    };

                    let schema_version = if let Some(MessageValue::Uuid(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("schema_version not a uuid"));
                    };

                    let data_center = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("data_center not a varchar"));
                    };
                    let rack = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("rack not a varchar"));
                    };

                    Ok(NodeInfo {
                        tokens,
                        schema_version,
                        release_version,
                        data_center,
                        rack,
                    })
                })
                .collect(),
            operation => Err(anyhow!(
                "system.local returned unexpected cassandra operation: {:?}",
                operation
            )),
        }
    } else {
        Err(anyhow!(
            "Failed to parse system.local response {:?}",
            response
        ))
    }
}

#[async_trait]
impl Transform for CassandraSinkCluster {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.send_message(message_wrapper.messages).await
    }

    fn is_terminating(&self) -> bool {
        true
    }

    fn set_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.connection_factory
            .set_pushed_messages_tx(pushed_messages_tx);
    }
}
