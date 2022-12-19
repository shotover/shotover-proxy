use crate::error::ChainResponse;
use crate::frame::cassandra::{parse_statement_single, CassandraMetadata, Tracing};
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{IntSize, Message, MessageValue, Messages, Metadata};
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::cassandra::connection::{CassandraConnection, Response};
use crate::transforms::{Transform, TransformBuilder, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use cassandra_protocol::events::ServerEvent;
use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType, UnpreparedError};
use cassandra_protocol::frame::message_execute::BodyReqExecuteOwned;
use cassandra_protocol::frame::message_result::BodyResResultPrepared;
use cassandra_protocol::frame::message_result::PreparedMetadata;
use cassandra_protocol::frame::{Opcode, Version};
use cassandra_protocol::types::CBytesShort;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{
    FQNameRef, Identifier, IdentifierRef, Operand, RelationElement, RelationOperator,
};
use cql3_parser::select::{Select, SelectElement};
use futures::future::try_join_all;
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use itertools::Itertools;
use metrics::{register_counter, Counter};
use node::{CassandraNode, ConnectionFactory};
use node_pool::{GetReplicaErr, KeyspaceMetadata, NodePool};
use rand::prelude::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use topology::{create_topology_task, TaskConnectionInfo};
use uuid::Uuid;
use version_compare::Cmp;

pub mod node;
mod node_pool;
mod routing_key;
#[cfg(test)]
mod test_router;
mod token_map;
pub mod topology;

pub type KeyspaceChanTx = watch::Sender<HashMap<String, KeyspaceMetadata>>;
pub type KeyspaceChanRx = watch::Receiver<HashMap<String, KeyspaceMetadata>>;

const SYSTEM_KEYSPACES: [IdentifierRef<'static>; 3] = [
    IdentifierRef::Quoted("system"),
    IdentifierRef::Quoted("system_schema"),
    IdentifierRef::Quoted("system_distributed"),
];
const LOCAL_TABLE: FQNameRef = FQNameRef {
    keyspace: Some(IdentifierRef::Quoted("system")),
    name: IdentifierRef::Quoted("local"),
};
const PEERS_TABLE: FQNameRef = FQNameRef {
    keyspace: Some(IdentifierRef::Quoted("system")),
    name: IdentifierRef::Quoted("peers"),
};
const PEERS_V2_TABLE: FQNameRef = FQNameRef {
    keyspace: Some(IdentifierRef::Quoted("system")),
    name: IdentifierRef::Quoted("peers_v2"),
};

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraSinkClusterConfig {
    /// contact points must be within the specified data_center and rack.
    /// If this is not followed, shotover's invariants will still be upheld but shotover will communicate with a
    /// node outside of the specified data_center and rack.
    pub first_contact_points: Vec<String>,
    pub local_shotover_host_id: Uuid,
    pub shotover_nodes: Vec<ShotoverNode>,
    pub tls: Option<TlsConnectorConfig>,
    pub connect_timeout_ms: u64,
    pub read_timeout: Option<u64>,
}

impl CassandraSinkClusterConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<TransformBuilder> {
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

        Ok(TransformBuilder::CassandraSinkCluster(Box::new(
            CassandraSinkClusterBuilder::new(
                self.first_contact_points.clone(),
                shotover_nodes,
                chain_name,
                local_node,
                tls,
                self.connect_timeout_ms,
                self.read_timeout,
            ),
        )))
    }
}

#[derive(Clone)]
pub struct CassandraSinkClusterBuilder {
    contact_points: Vec<String>,
    connection_factory: ConnectionFactory,
    shotover_peers: Vec<ShotoverNode>,
    failed_requests: Counter,
    read_timeout: Option<Duration>,
    local_shotover_node: ShotoverNode,
    nodes_rx: watch::Receiver<Vec<CassandraNode>>,
    keyspaces_rx: KeyspaceChanRx,
    task_handshake_tx: mpsc::Sender<TaskConnectionInfo>,
    pool: NodePool,
}

impl CassandraSinkClusterBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        contact_points: Vec<String>,
        shotover_peers: Vec<ShotoverNode>,
        chain_name: String,
        local_shotover_node: ShotoverNode,
        tls: Option<TlsConnector>,
        connect_timeout_ms: u64,
        timeout: Option<u64>,
    ) -> Self {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name, "transform" => "CassandraSinkCluster");
        let receive_timeout = timeout.map(Duration::from_secs);
        let connect_timeout = Duration::from_millis(connect_timeout_ms);

        let (local_nodes_tx, local_nodes_rx) = watch::channel(vec![]);
        let (keyspaces_tx, keyspaces_rx): (KeyspaceChanTx, KeyspaceChanRx) =
            watch::channel(HashMap::new());

        let (task_handshake_tx, task_handshake_rx) = mpsc::channel(1);

        create_topology_task(
            local_nodes_tx,
            keyspaces_tx,
            task_handshake_rx,
            local_shotover_node.data_center.clone(),
        );

        Self {
            contact_points,
            connection_factory: ConnectionFactory::new(connect_timeout, tls),
            shotover_peers,
            failed_requests,
            read_timeout: receive_timeout,
            local_shotover_node,
            nodes_rx: local_nodes_rx,
            keyspaces_rx,
            task_handshake_tx,
            pool: NodePool::new(vec![]),
        }
    }

    pub fn validate(&self) -> Vec<String> {
        vec![]
    }

    pub fn is_terminating(&self) -> bool {
        true
    }

    pub fn build(self) -> Box<CassandraSinkCluster> {
        Box::new(CassandraSinkCluster {
            contact_points: self.contact_points,
            shotover_peers: self.shotover_peers,
            control_connection: None,
            connection_factory: self.connection_factory.new_with_same_config(),
            control_connection_address: None,
            init_handshake_complete: false,
            version: None,
            failed_requests: self.failed_requests,
            read_timeout: self.read_timeout,
            local_shotover_node: self.local_shotover_node,
            pool: self.pool,
            // Because the self.nodes_rx is always copied from the original nodes_rx created before any node lists were sent,
            // once a single node list has been sent all new connections will immediately recognize it as a change.
            nodes_rx: self.nodes_rx,
            keyspaces_rx: self.keyspaces_rx,
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            task_handshake_tx: self.task_handshake_tx,
        })
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
    // Used for any messages that require a consistent destination, this includes:
    // * The initial handshake
    // * DDL queries
    // * system queries
    control_connection: Option<CassandraConnection>,
    control_connection_address: Option<SocketAddr>,
    init_handshake_complete: bool,

    version: Option<Version>,
    failed_requests: Counter,
    read_timeout: Option<Duration>,
    local_shotover_node: ShotoverNode,
    /// The nodes list is populated as soon as nodes_rx makes one available, but once a confirmed succesful handshake is reached
    /// we await nodes_rx to ensure that we have a nodes list from that point forward.
    /// Addditionally any changes to nodes_rx is observed and copied over.
    pool: NodePool,
    nodes_rx: watch::Receiver<Vec<CassandraNode>>,
    keyspaces_rx: KeyspaceChanRx,
    rng: SmallRng,
    task_handshake_tx: mpsc::Sender<TaskConnectionInfo>,
}

fn create_query(messages: &Messages, query: &str, version: Version) -> Result<Message> {
    let stream_id = get_unused_stream_id(messages)?;
    Ok(Message::from_frame(Frame::Cassandra(CassandraFrame {
        version,
        stream_id,
        tracing: Tracing::Request(false),
        warnings: vec![],
        operation: CassandraOperation::Query {
            query: Box::new(parse_statement_single(query)),
            params: Box::default(),
        },
    })))
}

impl CassandraSinkCluster {
    async fn send_message(&mut self, mut messages: Messages) -> ChainResponse {
        if self.version.is_none() {
            if let Some(message) = messages.first() {
                if let Ok(Metadata::Cassandra(CassandraMetadata { version, .. })) =
                    message.metadata()
                {
                    self.version = Some(version);
                } else {
                    return Err(anyhow!(
                        "Failed to extract cassandra version from incoming message: Not a valid cassandra message"
                    ));
                }
            } else {
                // It's an invariant that self.version is Some.
                // Since we were unable to set it, we need to return immediately.
                // This is ok because if there are no messages then we have no work to do anyway.
                return Ok(vec![]);
            }
        }

        if self.nodes_rx.has_changed()? {
            // This approach to keeping nodes list up to date has a problem when a node goes down and then up again before this transform instance can process the down going down.
            // When this happens we never detect that the node went down and a dead connection is left around.
            // Broadcast channel's SendError::Lagged would solve this problem but we cant use broadcast channels because cloning them doesnt keep a past value.
            // It might be worth implementing a custom watch channel that supports Lagged errors to improve correctness.
            //
            // However none of this is actually a problem because dead connection detection logic handles this case for us.
            self.pool.update_nodes(&mut self.nodes_rx);

            // recreate the control connection if it is down
            if let Some(address) = self.control_connection_address {
                if !self
                    .pool
                    .nodes()
                    .iter()
                    .any(|x| x.address == address && x.is_up)
                {
                    let addresses = self.pool.get_shuffled_addresses_in_dc_rack(
                        &self.local_shotover_node.rack,
                        &mut self.rng,
                    );
                    self.create_control_connection(&addresses).await.map_err(|e|
                        e.context("Failed to recreate control connection after control connection node went down")
                    )?;
                }
            }
        }

        if self.keyspaces_rx.has_changed()? {
            self.pool.update_keyspaces(&mut self.keyspaces_rx).await;
        }

        // If we have a prepare message we will need to open connections one by one for all nodes later on.
        // So as optimization, we instead open them all concurrently first.
        if messages.iter_mut().any(is_prepare_message) {
            try_join_all(
                self.pool
                    .nodes()
                    .iter_mut()
                    .map(|node| node.get_connection(&self.connection_factory)),
            )
            .await?;
        }

        let mut outgoing_index_offset = 0;
        let tables_to_rewrite: Vec<TableToRewrite> = messages
            .iter_mut()
            .enumerate()
            .filter_map(|(i, m)| {
                let table = self.get_rewrite_table(m, i, outgoing_index_offset);
                if let Some(table) = &table {
                    outgoing_index_offset += table.ty.extra_messages_needed();
                }
                table
            })
            .collect();

        // Insert the extra messages required by table rewrites.
        // After this the incoming_index values are now invalid and the outgoing_index values should be used instead
        for table_to_rewrite in tables_to_rewrite.iter().rev() {
            match &table_to_rewrite.ty {
                RewriteTableTy::Local => {
                    let query = "SELECT rack, data_center, schema_version, tokens, release_version FROM system.peers";
                    messages.insert(
                        table_to_rewrite.incoming_index + 1,
                        create_query(&messages, query, self.version.unwrap())?,
                    );
                }
                RewriteTableTy::Peers => {
                    let query = "SELECT rack, data_center, schema_version, tokens, release_version FROM system.peers";
                    messages.insert(
                        table_to_rewrite.incoming_index + 1,
                        create_query(&messages, query, self.version.unwrap())?,
                    );
                    let query = "SELECT rack, data_center, schema_version, tokens, release_version FROM system.local";
                    messages.insert(
                        table_to_rewrite.incoming_index + 2,
                        create_query(&messages, query, self.version.unwrap())?,
                    );
                }
                RewriteTableTy::Prepare { destination_nodes } => {
                    for i in 1..destination_nodes.len() {
                        messages.insert(
                            table_to_rewrite.incoming_index + i,
                            messages[table_to_rewrite.incoming_index].clone(),
                        );
                    }
                }
            }
        }

        // Create the initial connection.
        // Messages will be sent through this connection until we have extracted the handshake.
        if self.control_connection.is_none() {
            let points = if self.pool.nodes().iter().all(|x| !x.is_up) {
                let mut points = Vec::with_capacity(self.contact_points.len());
                for point in &self.contact_points {
                    points.push(tokio::net::lookup_host(point).await?.next().unwrap());
                }
                points
            } else {
                self.pool.get_shuffled_addresses_in_dc_rack(
                    &self.local_shotover_node.rack,
                    &mut self.rng,
                )
            };

            self.create_control_connection(&points)
                .await
                .map_err(|e| e.context("Failed to create initial control connection"))?;
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
        let mut nodes_to_prepare_on: Vec<Uuid> = vec![];

        for (i, mut message) in messages.into_iter().enumerate() {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            if self.pool.nodes().is_empty()
                || !self.init_handshake_complete
                // system.local and system.peers must be routed to the same node otherwise the system.local node will be amongst the system.peers nodes and a node will be missing
                // DDL statements and system.local must be routed through the same connection, so that schema_version changes appear immediately in system.local
                || is_ddl_statement(&mut message)
                || self.is_system_query(&mut message)
            {
                self.control_connection
                    .as_mut()
                    .unwrap()
                    .send(message, return_chan_tx)?;
            } else if is_use_statement(&mut message) {
                // Adding the USE statement to the handshake ensures that any new connection
                // created will have the correct keyspace setup.
                self.connection_factory.set_use_message(message.clone());

                // Send the USE statement to all open connections to ensure they are all in sync
                for (node_index, node) in self.pool.nodes().iter().enumerate() {
                    if let Some(connection) = &node.outbound {
                        let (return_chan_tx, return_chan_rx) = oneshot::channel();
                        connection.send(message.clone(), return_chan_tx)?;
                        responses_future_use.push_back(return_chan_rx);
                        use_future_index_to_node_index.push(node_index);
                    }
                }

                // Send the USE statement to the handshake connection and use the response as shotovers response
                self.control_connection
                    .as_mut()
                    .unwrap()
                    .send(message, return_chan_tx)?;
            } else if is_prepare_message(&mut message) {
                if let Some(rewrite) = tables_to_rewrite.iter().find(|x| x.outgoing_index == i) {
                    if let RewriteTableTy::Prepare { destination_nodes } = &rewrite.ty {
                        nodes_to_prepare_on = destination_nodes.clone();
                    }
                }
                let next_host_id = nodes_to_prepare_on
                    .pop()
                    .ok_or_else(|| anyhow!("ran out of nodes to send prepare messages to"))?;
                self.pool
                    .nodes()
                    .iter_mut()
                    .find(|node| node.host_id == next_host_id)
                    .ok_or_else(|| anyhow!("node {next_host_id} has dissapeared"))?
                    .get_connection(&self.connection_factory)
                    .await?
                    .send(message, return_chan_tx)?;
            } else {
                // If the message is an execute we should perform token aware routing
                if let Some((execute, metadata)) = get_execute_message(&mut message) {
                    match self
                        .pool
                        .get_replica_node_in_dc(
                            execute,
                            &self.local_shotover_node.rack,
                            self.version.unwrap(),
                            &mut self.rng,
                        )
                        .await
                    {
                        Ok(Some(replica_node)) => {
                            replica_node
                                .get_connection(&self.connection_factory)
                                .await?
                                .send(message, return_chan_tx)?;
                        }
                        Ok(None) | Err(GetReplicaErr::NoKeyspaceMetadata) => {
                            let node = self
                                .pool
                                .get_round_robin_node_in_dc_rack(&self.local_shotover_node.rack);
                            node.get_connection(&self.connection_factory)
                                .await?
                                .send(message, return_chan_tx)?;
                        }
                        Err(GetReplicaErr::NoPreparedMetadata) => {
                            let id = execute.id.clone();
                            tracing::info!("forcing re-prepare on {:?}", id);
                            // this shotover node doesn't have the metadata.
                            // send an unprepared error in response to force
                            // the client to reprepare the query
                            return_chan_tx
                                .send(Ok(Message::from_frame(Frame::Cassandra(
                                    CassandraFrame {
                                        operation: CassandraOperation::Error(ErrorBody {
                                            message: "Shotover does not have this query's metadata. Please re-prepare on this Shotover host before sending again.".into(),
                                            ty: ErrorType::Unprepared(UnpreparedError { id }),
                                        }),
                                        stream_id: metadata.stream_id,
                                        tracing: Tracing::Response(None), // We didn't actually hit a node so we don't have a tracing id
                                        version: self.version.unwrap(),
                                        warnings: vec![],
                                    },
                                )))).expect("the receiver is guaranteed to be alive, so this must succeed");
                        }
                        Err(GetReplicaErr::Other(err)) => {
                            return Err(err);
                        }
                    };

                    // otherwise just send to a random node
                } else {
                    let node = self
                        .pool
                        .get_round_robin_node_in_dc_rack(&self.local_shotover_node.rack);
                    node.get_connection(&self.connection_factory)
                        .await?
                        .send(message, return_chan_tx)?;
                }
            }

            responses_future.push_back(return_chan_rx)
        }

        let response_results =
            super::connection::receive(self.read_timeout, &self.failed_requests, responses_future)
                .await?;
        let mut responses = vec![];
        for response in response_results {
            match response {
                Ok(response) => responses.push(response),
                Err(error) => {
                    self.pool.report_issue_with_node(error.destination);
                    responses.push(error.to_response(self.version.unwrap()));
                }
            }
        }

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
                self.pool.nodes()[node_index].outbound = None;
            }
        }

        for table_to_rewrite in tables_to_rewrite {
            self.rewrite_table(table_to_rewrite, &mut responses).await?;
        }

        for response in responses.iter_mut() {
            if let Some((id, metadata)) = get_prepared_result_message(response) {
                self.pool.add_prepared_result(id, metadata).await;
            }
        }

        Ok(responses)
    }

    async fn complete_handshake(&mut self) -> Result<()> {
        // Only send a handshake if the task really needs it
        // i.e. when the channel of size 1 is empty
        if let Ok(permit) = self.task_handshake_tx.try_reserve() {
            permit.send(TaskConnectionInfo {
                connection_factory: self.connection_factory.clone(),
                address: self.control_connection_address.unwrap(),
            })
        }
        self.init_handshake_complete = true;

        if self.pool.nodes().is_empty() {
            self.nodes_rx.changed().await?;
            self.pool.update_nodes(&mut self.nodes_rx);

            // If we have to populate the local_nodes at this point then that means the control connection
            // may not have been made against a node in the configured data_center/rack.
            // Therefore we need to recreate the control connection to ensure that it is in the configured data_center/rack.
            let addresses = self
                .pool
                .get_shuffled_addresses_in_dc_rack(&self.local_shotover_node.rack, &mut self.rng);
            self.create_control_connection(&addresses)
                .await
                .map_err(|e| e.context("Failed to recreate control connection when initial connection was possibly against the wrong node"))?;
        }
        tracing::info!(
            "Control connection finalized against node at: {:?}",
            self.control_connection_address.unwrap()
        );

        Ok(())
    }

    async fn create_control_connection(&mut self, addresses: &[SocketAddr]) -> Result<()> {
        struct AddressError {
            address: SocketAddr,
            error: anyhow::Error,
        }
        fn bullet_list_of_node_failures(errors: &[AddressError]) -> String {
            let mut node_errors = String::new();
            for AddressError { error, address } in errors {
                node_errors.push_str(&format!("\n* {address:?}:"));
                for sub_error in error.chain() {
                    node_errors.push_str(&format!("\n    - {sub_error}"));
                }
            }
            node_errors
        }

        let mut errors = vec![];
        for address in addresses {
            match self.connection_factory.new_connection(address).await {
                Ok(connection) => {
                    self.control_connection = Some(connection);
                    self.control_connection_address = Some(*address);
                    if !errors.is_empty() {
                        let node_errors = bullet_list_of_node_failures(&errors);
                        tracing::warn!("A successful connection to a control node was made but attempts to connect to these nodes failed first:{node_errors}");
                    }
                    return Ok(());
                }
                Err(error) => {
                    errors.push(AddressError {
                        error,
                        address: *address,
                    });
                }
            }
        }

        let node_errors = bullet_list_of_node_failures(&errors);
        Err(anyhow!(
            "Attempted to create a control connection against every node in the rack and all attempts failed:{node_errors}"
        ))
    }

    /// Returns any information required to correctly rewrite the response.
    /// Will also perform minor modifications to the query required for the rewrite.
    fn get_rewrite_table(
        &mut self,
        request: &mut Message,
        incoming_index: usize,
        outgoing_index_offset: usize,
    ) -> Option<TableToRewrite> {
        if let Some(Frame::Cassandra(cassandra)) = request.frame() {
            // No need to handle Batch as selects can only occur on Query
            match &mut cassandra.operation {
                CassandraOperation::Query { query, .. } => {
                    if let CassandraStatement::Select(select) = query.as_mut() {
                        let ty = if LOCAL_TABLE == select.table_name {
                            RewriteTableTy::Local
                        } else if PEERS_TABLE == select.table_name
                            || PEERS_V2_TABLE == select.table_name
                        {
                            RewriteTableTy::Peers
                        } else {
                            return None;
                        };

                        let warnings = if Self::has_no_where_clause(&ty, select) {
                            vec![]
                        } else {
                            select.where_clause.clear();
                            vec![format!(
                            "WHERE clause on the query was ignored. Shotover does not support WHERE clauses on queries against {}",
                            select.table_name
                        )]
                        };

                        return Some(TableToRewrite {
                            incoming_index,
                            outgoing_index: incoming_index + outgoing_index_offset,
                            ty,
                            warnings,
                            selects: select.columns.clone(),
                        });
                    }
                }
                CassandraOperation::Prepare(_) => {
                    return Some(TableToRewrite {
                        incoming_index,
                        outgoing_index: incoming_index + outgoing_index_offset,
                        ty: RewriteTableTy::Prepare {
                            destination_nodes: self
                                .pool
                                .nodes()
                                .iter()
                                .filter(|node| node.is_up)
                                .map(|node| node.host_id)
                                .collect(),
                        },
                        warnings: vec![],
                        selects: vec![],
                    });
                }
                _ => {}
            }
        }
        None
    }

    fn has_no_where_clause(ty: &RewriteTableTy, select: &Select) -> bool {
        select.where_clause.is_empty()
            // Most drivers do `FROM system.local WHERE key = 'local'` when determining the topology.
            // I'm not sure why they do that it seems to have no affect as there is only ever one row and its key is always 'local'.
            // Maybe it was a workaround for an old version of cassandra that got copied around?
            // To keep warning noise down we consider it as having no where clause.
            || (ty == &RewriteTableTy::Local
                && select.where_clause
                    == [RelationElement {
                        obj: Operand::Column(Identifier::Quoted("key".to_owned())),
                        oper: RelationOperator::Equal,
                        value: Operand::Const("'local'".to_owned()),
                    }])
    }

    async fn rewrite_table(
        &mut self,
        table: TableToRewrite,
        responses: &mut Vec<Message>,
    ) -> Result<()> {
        fn get_warnings(message: &mut Message) -> Vec<String> {
            if let Some(Frame::Cassandra(frame)) = message.frame() {
                frame.warnings.clone()
            } else {
                vec![]
            }
        }

        fn remove_extra_message(
            table: &TableToRewrite,
            responses: &mut Vec<Message>,
        ) -> Option<Message> {
            if table.incoming_index + 1 < responses.len() {
                Some(responses.remove(table.incoming_index + 1))
            } else {
                None
            }
        }

        match &table.ty {
            RewriteTableTy::Local => {
                if let Some(mut peers_response) = remove_extra_message(&table, responses) {
                    if let Some(local_response) = responses.get_mut(table.incoming_index) {
                        // Include warnings from every message that gets combined into the final message + any extra warnings noted in the TableToRewrite
                        let mut warnings = table.warnings.clone();
                        warnings.extend(get_warnings(&mut peers_response));
                        warnings.extend(get_warnings(local_response));

                        self.rewrite_table_local(table, local_response, peers_response, warnings)
                            .await?;
                        local_response.invalidate_cache();
                    }
                }
            }
            RewriteTableTy::Peers => {
                if let Some(mut peers_response) = remove_extra_message(&table, responses) {
                    if let Some(mut local_response) = remove_extra_message(&table, responses) {
                        if let Some(client_peers_response) = responses.get_mut(table.incoming_index)
                        {
                            // Include warnings from every message that gets combined into the final message + any extra warnings noted in the TableToRewrite
                            let mut warnings = table.warnings.clone();
                            warnings.extend(get_warnings(&mut peers_response));
                            warnings.extend(get_warnings(&mut local_response));
                            warnings.extend(get_warnings(client_peers_response));

                            let mut nodes = parse_system_nodes(peers_response)?;
                            nodes.extend(parse_system_nodes(local_response)?);

                            self.rewrite_table_peers(table, client_peers_response, nodes, warnings)
                                .await?;
                            client_peers_response.invalidate_cache();
                        }
                    }
                }
            }
            RewriteTableTy::Prepare { destination_nodes } => {
                let prepared_responses = &mut responses
                    [table.incoming_index..table.incoming_index + destination_nodes.len()];

                let mut warnings: Vec<String> = prepared_responses
                    .iter_mut()
                    .flat_map(get_warnings)
                    .collect();

                {
                    let prepared_results: Vec<&mut Box<BodyResResultPrepared>> = prepared_responses
                    .iter_mut()
                    .filter_map(|message| match message.frame() {
                        Some(Frame::Cassandra(CassandraFrame {
                            operation:
                                CassandraOperation::Result(CassandraResult::Prepared(prepared)),
                            ..
                        })) => Some(prepared),
                        other => {
                            tracing::error!("Response to Prepare query was not a Prepared, was instead: {other:?}");
                            warnings.push(format!("Shotover: Response to Prepare query was not a Prepared, was instead: {other:?}"));
                            None
                        }
                    })
                    .collect();
                    if !prepared_results.windows(2).all(|w| w[0] == w[1]) {
                        let err_str = prepared_results
                            .iter()
                            .map(|p| format!("\n{:?}", p))
                            .collect::<String>();

                        tracing::error!(
                            "Nodes did not return the same response to PREPARE statement {err_str}"
                        );
                        warnings.push(format!(
                            "Shotover: Nodes did not return the same response to PREPARE statement {err_str}"
                        ));
                    }
                }

                // If there is a succesful response, use that as our response by moving it to the beginning
                for (i, response) in prepared_responses.iter_mut().enumerate() {
                    if let Some(Frame::Cassandra(CassandraFrame {
                        operation: CassandraOperation::Result(CassandraResult::Prepared(_)),
                        ..
                    })) = response.frame()
                    {
                        prepared_responses.swap(0, i);
                        break;
                    }
                }

                // Finalize our response by setting its warnings list to all the warnings we have accumulated
                if let Some(Frame::Cassandra(frame)) = prepared_responses[0].frame() {
                    frame.warnings = warnings;
                }

                // Remove all unused messages so we are only left with the one message that will be used as the response
                responses.drain(
                    table.incoming_index + 1..table.incoming_index + destination_nodes.len(),
                );
            }
        }

        Ok(())
    }

    async fn rewrite_table_peers(
        &mut self,
        table: TableToRewrite,
        peers_response: &mut Message,
        nodes: Vec<NodeInfo>,
        warnings: Vec<String>,
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
                    if column.name == IdentifierRef::Quoted("data_center") {
                        data_center_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("rack") {
                        rack_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("host_id") {
                        host_id_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("native_address") {
                        native_address_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("native_port") {
                        native_port_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("preferred_ip") {
                        preferred_ip_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("preferred_port") {
                        preferred_port_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("rpc_address") {
                        rpc_address_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("peer") {
                        peer_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("peer_port") {
                        peer_port_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("release_version") {
                        release_version_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("tokens") {
                        tokens_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("schema_version") {
                        schema_version_alias = alias;
                    }
                }
            }
        }

        if let Some(Frame::Cassandra(frame)) = peers_response.frame() {
            frame.warnings = warnings;
            if let CassandraOperation::Result(CassandraResult::Rows { rows, metadata }) =
                &mut frame.operation
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
                                {
                                    MessageValue::Null
                                } else if colspec.name == native_address_alias {
                                    MessageValue::Inet(shotover_peer.address.ip())
                                } else if colspec.name == native_port_alias {
                                    MessageValue::Integer(
                                        shotover_peer.address.port() as i64,
                                        IntSize::I32,
                                    )
                                } else if colspec.name == peer_alias
                                    || colspec.name == rpc_address_alias
                                {
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
        warnings: Vec<String>,
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
        let mut rpc_address_alias = "rpc_address";
        let mut rpc_port_alias = "rpc_port";
        for select in &table.selects {
            if let SelectElement::Column(column) = select {
                if let Some(alias) = &column.alias {
                    let alias = match alias {
                        Identifier::Unquoted(alias) => alias,
                        Identifier::Quoted(alias) => alias,
                    };
                    if column.name == IdentifierRef::Quoted("release_version") {
                        release_version_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("tokens") {
                        tokens_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("schema_version") {
                        schema_version_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("broadcast_address") {
                        broadcast_address_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("listen_address") {
                        listen_address_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("host_id") {
                        host_id_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("rpc_address") {
                        rpc_address_alias = alias
                    } else if column.name == IdentifierRef::Quoted("rpc_port") {
                        rpc_port_alias = alias
                    }
                }
            }
        }

        if let Some(Frame::Cassandra(frame)) = local_response.frame() {
            if let CassandraOperation::Result(CassandraResult::Rows { rows, metadata }) =
                &mut frame.operation
            {
                frame.warnings = warnings;
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
                        } else if col_meta.name == rpc_address_alias {
                            if let MessageValue::Inet(address) = col {
                                if address != &IpAddr::V4(Ipv4Addr::UNSPECIFIED) {
                                    *address = self.local_shotover_node.address.ip()
                                }
                            }
                        } else if col_meta.name == rpc_port_alias {
                            if let MessageValue::Integer(rpc_port, _) = col {
                                *rpc_port = self.local_shotover_node.address.port() as i64;
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

    fn is_system_query(&self, request: &mut Message) -> bool {
        if let Some(Frame::Cassandra(frame)) = request.frame() {
            if let CassandraOperation::Query { query, .. } = &mut frame.operation {
                if let CassandraStatement::Select(select) = query.as_ref() {
                    if let Some(keyspace) = &select.table_name.keyspace {
                        return SYSTEM_KEYSPACES.iter().any(|x| x == keyspace);
                    }
                }
            }
        }
        false
    }
}

struct TableToRewrite {
    incoming_index: usize,
    outgoing_index: usize,
    ty: RewriteTableTy,
    selects: Vec<SelectElement>,
    warnings: Vec<String>,
}

#[derive(PartialEq, Clone)]
enum RewriteTableTy {
    Local,
    Peers,
    // We need to know which nodes we are sending to early on so that we can create the appropriate number of messages early and keep our rewrite indexes intact
    Prepare { destination_nodes: Vec<Uuid> },
}

impl RewriteTableTy {
    fn extra_messages_needed(&self) -> usize {
        match self {
            RewriteTableTy::Local => 1,
            RewriteTableTy::Peers => 2,
            RewriteTableTy::Prepare { destination_nodes } => {
                destination_nodes.len().saturating_sub(1)
            }
        }
    }
}

fn get_prepared_result_message(message: &mut Message) -> Option<(CBytesShort, PreparedMetadata)> {
    if let Some(Frame::Cassandra(CassandraFrame {
        operation: CassandraOperation::Result(CassandraResult::Prepared(prepared)),
        ..
    })) = message.frame()
    {
        return Some((prepared.id.clone(), prepared.metadata.clone()));
    }

    None
}

fn get_execute_message(message: &mut Message) -> Option<(&BodyReqExecuteOwned, CassandraMetadata)> {
    if let Some(Frame::Cassandra(CassandraFrame {
        operation: CassandraOperation::Execute(execute_body),
        version,
        stream_id,
        tracing,
        ..
    })) = message.frame()
    {
        return Some((
            execute_body,
            CassandraMetadata {
                version: *version,
                stream_id: *stream_id,
                tracing: *tracing,
                opcode: Opcode::Execute,
            },
        ));
    }

    None
}

fn is_prepare_message(message: &mut Message) -> bool {
    if let Some(Frame::Cassandra(CassandraFrame {
        operation: CassandraOperation::Prepare(_),
        ..
    })) = message.frame()
    {
        return true;
    }

    false
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
    if let Some(Ok(Ok(mut response))) = response {
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
            CassandraOperation::Result(CassandraResult::Rows { rows, .. }) => rows
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

    async fn transform_pushed<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        message_wrapper.messages.retain_mut(|message| {
            if let Some(Frame::Cassandra(CassandraFrame {
                operation: CassandraOperation::Event(event),
                ..
            })) = message.frame()
            {
                match event {
                    ServerEvent::TopologyChange(_) => false,
                    ServerEvent::StatusChange(_) => false,
                    ServerEvent::SchemaChange(_) => true,
                }
            } else {
                true
            }
        });
        message_wrapper.call_next_transform_pushed().await
    }

    fn set_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.connection_factory
            .set_pushed_messages_tx(pushed_messages_tx);
    }
}
