use self::node_pool::{get_accessible_owned_connection, NodePoolBuilder, PreparedMetadata};
use self::rewrite::{MessageRewriter, RewriteTableTy};
use crate::frame::cassandra::{CassandraMetadata, Tracing};
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{Message, Messages, Metadata};
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::cassandra::connection::{CassandraConnection, Response, ResponseError};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Transforms, Wrapper};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use cassandra_protocol::events::ServerEvent;
use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType, UnpreparedError};
use cassandra_protocol::frame::message_execute::BodyReqExecuteOwned;
use cassandra_protocol::frame::{Opcode, Version};
use cassandra_protocol::types::CBytesShort;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::IdentifierRef;
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use metrics::{register_counter, Counter};
use node::{CassandraNode, ConnectionFactory};
use node_pool::{GetReplicaErr, KeyspaceMetadata, NodePool};
use rand::prelude::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use topology::{create_topology_task, TaskConnectionInfo};
use uuid::Uuid;

pub mod node;
mod node_pool;
mod rewrite;
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

#[derive(Deserialize, Debug)]
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

#[typetag::deserialize(name = "CassandraSinkCluster")]
#[async_trait(?Send)]
impl TransformConfig for CassandraSinkClusterConfig {
    async fn get_builder(&self, chain_name: String) -> Result<Box<dyn TransformBuilder>> {
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

        Ok(Box::new(CassandraSinkClusterBuilder::new(
            self.first_contact_points.clone(),
            shotover_nodes,
            chain_name,
            local_node,
            tls,
            self.connect_timeout_ms,
            self.read_timeout,
        )))
    }
}

#[derive(Clone)]
pub struct CassandraSinkClusterBuilder {
    contact_points: Vec<String>,
    connection_factory: ConnectionFactory,
    failed_requests: Counter,
    read_timeout: Option<Duration>,
    message_rewriter: MessageRewriter,
    nodes_rx: watch::Receiver<Vec<CassandraNode>>,
    keyspaces_rx: KeyspaceChanRx,
    task_handshake_tx: mpsc::Sender<TaskConnectionInfo>,
    pool: NodePoolBuilder,
}

impl CassandraSinkClusterBuilder {
    pub fn new(
        contact_points: Vec<String>,
        shotover_peers: Vec<ShotoverNode>,
        chain_name: String,
        local_shotover_node: ShotoverNode,
        tls: Option<TlsConnector>,
        connect_timeout_ms: u64,
        timeout: Option<u64>,
    ) -> Self {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name.clone(), "transform" => "CassandraSinkCluster");
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

        let message_rewriter = MessageRewriter {
            shotover_peers,
            local_shotover_node,
        };

        Self {
            contact_points,
            connection_factory: ConnectionFactory::new(connect_timeout, tls),
            message_rewriter,
            failed_requests,
            read_timeout: receive_timeout,
            nodes_rx: local_nodes_rx,
            keyspaces_rx,
            task_handshake_tx,
            pool: NodePoolBuilder::new(chain_name),
        }
    }
}

impl TransformBuilder for CassandraSinkClusterBuilder {
    fn build(&self) -> crate::transforms::Transforms {
        Transforms::CassandraSinkCluster(Box::new(CassandraSinkCluster {
            contact_points: self.contact_points.clone(),
            message_rewriter: self.message_rewriter.clone(),
            control_connection: None,
            connection_factory: self.connection_factory.new_with_same_config(),
            control_connection_address: None,
            init_handshake_complete: false,
            version: None,
            failed_requests: self.failed_requests.clone(),
            read_timeout: self.read_timeout,
            pool: self.pool.build(),
            // Because the self.nodes_rx is always copied from the original nodes_rx created before any node lists were sent,
            // once a single node list has been sent all new connections will immediately recognize it as a change.
            nodes_rx: self.nodes_rx.clone(),
            keyspaces_rx: self.keyspaces_rx.clone(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            task_handshake_tx: self.task_handshake_tx.clone(),
        }))
    }

    fn get_name(&self) -> &'static str {
        "CassandraSinkCluster"
    }

    fn is_terminating(&self) -> bool {
        true
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

    message_rewriter: MessageRewriter,
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
    /// The nodes list is populated as soon as nodes_rx makes one available, but once a confirmed succesful handshake is reached
    /// we await nodes_rx to ensure that we have a nodes list from that point forward.
    /// Addditionally any changes to nodes_rx is observed and copied over.
    pool: NodePool,
    nodes_rx: watch::Receiver<Vec<CassandraNode>>,
    keyspaces_rx: KeyspaceChanRx,
    rng: SmallRng,
    task_handshake_tx: mpsc::Sender<TaskConnectionInfo>,
}

impl CassandraSinkCluster {
    async fn send_message(&mut self, mut messages: Messages) -> Result<Messages> {
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
                    let (connection, address) = self.pool.get_random_owned_connection_in_dc_rack(
                        &self.message_rewriter.local_shotover_node.rack,
                        &mut self.rng,
                        &self.connection_factory,
                    ).await
                    .context("Failed to recreate control connection after control connection node went down")?;
                    self.set_control_connection(connection, address)
                }
            }
        }

        if self.keyspaces_rx.has_changed()? {
            self.pool.update_keyspaces(&mut self.keyspaces_rx).await;
        }

        // CAREFUL: indexes into messages are invalidated here
        let tables_to_rewrite = self
            .message_rewriter
            .rewrite_requests(
                &mut messages,
                &self.connection_factory,
                &mut self.pool,
                self.version.unwrap(),
            )
            .await?;

        // Create the initial connection.
        // Messages will be sent through this connection until we have extracted the handshake.
        if self.control_connection.is_none() {
            let (connection, address) = if self
                .pool
                .nodes()
                .iter()
                .any(|x| x.is_up && x.rack == self.message_rewriter.local_shotover_node.rack)
            {
                self.pool
                    .get_random_owned_connection_in_dc_rack(
                        &self.message_rewriter.local_shotover_node.rack,
                        &mut self.rng,
                        &self.connection_factory,
                    )
                    .await
                    .context("Failed to create initial control connection from current node pool")
            } else {
                let mut start_nodes = Vec::with_capacity(self.contact_points.len());
                for point in &self.contact_points {
                    start_nodes.push(CassandraNode::new(
                        tokio::net::lookup_host(point).await?.next().unwrap(),
                        // All of these fields use the cheapest option because get_accessible_owned_connection does not use them at all
                        String::new(),
                        vec![],
                        Uuid::nil(),
                    ));
                }

                get_accessible_owned_connection(
                    &self.connection_factory,
                    start_nodes.iter_mut().collect(),
                )
                .await
                .context("Failed to create initial control connection from initial contact points")
            }?;
            self.set_control_connection(connection, address);
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
            let return_chan_rx = if self.pool.nodes().is_empty()
                || !self.init_handshake_complete
                // system.local and system.peers must be routed to the same node otherwise the system.local node will be amongst the system.peers nodes and a node will be missing
                // DDL statements and system.local must be routed through the same connection, so that schema_version changes appear immediately in system.local
                || is_ddl_statement(&mut message)
                || self.is_system_query(&mut message)
            {
                self.control_connection.as_mut().unwrap().send(message)?
            } else if is_use_statement(&mut message) {
                // Adding the USE statement to the handshake ensures that any new connection
                // created will have the correct keyspace setup.
                self.connection_factory.set_use_message(message.clone());

                // Send the USE statement to all open connections to ensure they are all in sync
                for (node_index, node) in self.pool.nodes().iter().enumerate() {
                    if let Some(connection) = &node.outbound {
                        responses_future_use.push_back(connection.send(message.clone())?);
                        use_future_index_to_node_index.push(node_index);
                    }
                }

                // Send the USE statement to the handshake connection and use the response as shotovers response
                self.control_connection.as_mut().unwrap().send(message)?
            } else if is_prepare_message(&mut message) {
                if let Some(rewrite) = tables_to_rewrite.iter().find(|x| x.outgoing_index == i) {
                    if let RewriteTableTy::Prepare { destination_nodes } = &rewrite.ty {
                        nodes_to_prepare_on = destination_nodes.clone();
                    }
                }
                let next_host_id = nodes_to_prepare_on
                    .pop()
                    .ok_or_else(|| anyhow!("ran out of nodes to send prepare messages to"))?;
                match self
                    .pool
                    .nodes()
                    .iter_mut()
                    .find(|node| node.host_id == next_host_id)
                    .ok_or_else(|| anyhow!("node {next_host_id} has dissapeared"))?
                    .get_connection(&self.connection_factory)
                    .await
                {
                    Ok(connection) => connection.send(message)?,
                    Err(err) => send_error_in_response_to_message(&message, &format!("{err}"))?,
                }
            } else if let Some((execute, metadata)) = get_execute_message(&mut message) {
                // If the message is an execute we should perform token aware routing
                let rack = &self.message_rewriter.local_shotover_node.rack;
                let connection = self
                    .pool
                    .get_replica_connection_in_dc(
                        execute,
                        rack,
                        self.version.unwrap(),
                        &mut self.rng,
                        &self.connection_factory,
                    )
                    .await;

                match connection {
                    Ok(connection) => connection.send(message)?,
                    Err(
                        err @ GetReplicaErr::NoKeyspaceMetadata | err @ GetReplicaErr::NoRoutingKey,
                    ) => {
                        if matches!(err, GetReplicaErr::NoRoutingKey)
                            && self.version.unwrap() != Version::V3
                        {
                            tracing::error!(
                                "No routing key found for message on version: {}",
                                self.version.unwrap()
                            );
                        };

                        match self
                            .pool
                            .get_random_connection_in_dc_rack(
                                rack,
                                &mut self.rng,
                                &self.connection_factory,
                            )
                            .await
                        {
                            Ok(connection) => connection.send(message)?,
                            Err(err) => {
                                send_error_in_response_to_metadata(&metadata, &format!("{err}"))
                            }
                        }
                    }
                    Err(GetReplicaErr::NoPreparedMetadata) => {
                        let (return_chan_tx, return_chan_rx) = oneshot::channel();
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
                        return_chan_rx
                    }
                    Err(GetReplicaErr::NoNodeAvailable(err)) => {
                        send_error_in_response_to_metadata(&metadata, &format!("{err}"))
                    }
                    Err(GetReplicaErr::Other(err)) => {
                        return Err(err);
                    }
                }
            } else {
                // otherwise just send to a random node
                match self
                    .pool
                    .get_random_connection_in_dc_rack(
                        &self.message_rewriter.local_shotover_node.rack,
                        &mut self.rng,
                        &self.connection_factory,
                    )
                    .await
                {
                    Ok(connection) => connection.send(message)?,
                    Err(err) => send_error_in_response_to_message(&message, &format!("{err}"))?,
                }
            };

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

        self.message_rewriter
            .rewrite_responses(tables_to_rewrite, &mut responses)
            .await?;

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
            let (connection, address) = self.pool.get_random_owned_connection_in_dc_rack(
                &self.message_rewriter.local_shotover_node.rack,
                &mut self.rng,
                &self.connection_factory
            ).await
            .context("Failed to recreate control connection when initial connection was possibly against the wrong node")?;
            self.set_control_connection(connection, address);
        }
        tracing::info!(
            "Control connection finalized against node at: {:?}",
            self.control_connection_address.unwrap()
        );

        Ok(())
    }

    fn set_control_connection(&mut self, connection: CassandraConnection, address: SocketAddr) {
        self.control_connection = Some(connection);
        self.control_connection_address = Some(address);
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

fn send_error_in_response_to_metadata(
    metadata: &CassandraMetadata,
    error: &str,
) -> oneshot::Receiver<Result<Message, ResponseError>> {
    let (tx, rx) = oneshot::channel();
    tx.send(Ok(Message::from_frame(Frame::Cassandra(
        CassandraFrame::shotover_error(metadata.stream_id, metadata.version, error),
    ))))
    .unwrap();
    rx
}

fn send_error_in_response_to_message(
    message: &Message,
    error: &str,
) -> Result<oneshot::Receiver<Result<Message, ResponseError>>> {
    if let Ok(Metadata::Cassandra(metadata)) = message.metadata() {
        Ok(send_error_in_response_to_metadata(&metadata, error))
    } else {
        Err(anyhow!("Expected message to be of type cassandra"))
    }
}

fn get_prepared_result_message(message: &mut Message) -> Option<(CBytesShort, PreparedMetadata)> {
    if let Some(Frame::Cassandra(CassandraFrame {
        operation: CassandraOperation::Result(CassandraResult::Prepared(prepared)),
        ..
    })) = message.frame()
    {
        return Some((
            prepared.id.clone(),
            PreparedMetadata {
                pk_indexes: prepared.metadata.pk_indexes.clone(),
                keyspace: prepared
                    .metadata
                    .global_table_spec
                    .as_ref()
                    .map(|x| x.ks_name.clone()),
            },
        ));
    }

    None
}

fn get_execute_message(message: &mut Message) -> Option<(&BodyReqExecuteOwned, CassandraMetadata)> {
    if let Some(Frame::Cassandra(CassandraFrame {
        operation: CassandraOperation::Execute(execute_body),
        version,
        stream_id,
        ..
    })) = message.frame()
    {
        return Some((
            execute_body,
            CassandraMetadata {
                version: *version,
                stream_id: *stream_id,
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

#[async_trait]
impl Transform for CassandraSinkCluster {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> Result<Messages> {
        self.send_message(message_wrapper.requests).await
    }

    async fn transform_pushed<'a>(
        &'a mut self,
        mut message_wrapper: Wrapper<'a>,
    ) -> Result<Messages> {
        message_wrapper.requests.retain_mut(|message| {
            if let Some(Frame::Cassandra(CassandraFrame {
                operation: CassandraOperation::Event(event),
                ..
            })) = message.frame()
            {
                match event {
                    ServerEvent::TopologyChange(_) => false,
                    ServerEvent::StatusChange(_) => false,
                    ServerEvent::SchemaChange(_) => true,
                    _ => unreachable!(),
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
