use crate::codec::cassandra::CassandraCodec;
use crate::error::ChainResponse;
use crate::frame::cassandra::parse_statement_single;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{IntSize, Message, MessageValue, Messages};
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::cassandra::connection::CassandraConnection;
use crate::transforms::util::Response;
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use cassandra_protocol::frame::message_result::{
    ColSpec, ColType, ColTypeOption, ColTypeOptionValue, RowsMetadata, RowsMetadataFlags, TableSpec,
};
use cassandra_protocol::frame::Version;
use cassandra_protocol::query::QueryParams;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{FQName, Identifier};
use cql3_parser::select::SelectElement;
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use metrics::{register_counter, Counter};
use node::CassandraNode;
use rand::prelude::*;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;
use version_compare::Cmp;

mod node;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraSinkClusterConfig {
    pub first_contact_points: Vec<String>,
    pub data_center: String,
    pub rack: String,
    pub host_id: Uuid,
    pub tls: Option<TlsConnectorConfig>,
    pub read_timeout: Option<u64>,
}

impl CassandraSinkClusterConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<Transforms> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Transforms::CassandraSinkCluster(CassandraSinkCluster::new(
            self.first_contact_points.clone(),
            chain_name,
            self.data_center.clone(),
            self.rack.clone(),
            self.host_id,
            tls,
            self.read_timeout,
        )))
    }
}

pub struct CassandraSinkCluster {
    contact_points: Vec<String>,
    init_handshake_connection: Option<CassandraConnection>,
    init_handshake: Vec<Message>,
    init_handshake_address: Option<SocketAddr>,
    init_handshake_complete: bool,
    init_handshake_use_received: bool,
    chain_name: String,
    failed_requests: Counter,
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    read_timeout: Option<Duration>,
    local_table: FQName,
    peer_table: FQName,
    data_center: String,
    rack: String,
    host_id: Uuid,
    /// A local clone of topology_task_nodes
    /// Internally stores connections to the nodes
    local_nodes: Vec<CassandraNode>,
    /// Only written to by the topology task
    /// Transform instances should never write to this.
    topology_task_nodes: Arc<RwLock<Vec<CassandraNode>>>,
    rng: SmallRng,
    task_handshake_tx: mpsc::Sender<TaskHandshake>,
}

impl Clone for CassandraSinkCluster {
    fn clone(&self) -> Self {
        CassandraSinkCluster {
            contact_points: self.contact_points.clone(),
            init_handshake_connection: None,
            init_handshake: vec![],
            init_handshake_address: None,
            init_handshake_complete: false,
            init_handshake_use_received: false,
            chain_name: self.chain_name.clone(),
            tls: self.tls.clone(),
            failed_requests: self.failed_requests.clone(),
            pushed_messages_tx: None,
            read_timeout: self.read_timeout,
            local_table: self.local_table.clone(),
            peer_table: self.peer_table.clone(),
            data_center: self.data_center.clone(),
            rack: self.rack.clone(),
            host_id: self.host_id,
            local_nodes: vec![],
            topology_task_nodes: self.topology_task_nodes.clone(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            task_handshake_tx: self.task_handshake_tx.clone(),
        }
    }
}

impl CassandraSinkCluster {
    pub fn new(
        contact_points: Vec<String>,
        chain_name: String,
        data_center: String,
        rack: String,
        host_id: Uuid,
        tls: Option<TlsConnector>,
        timeout: Option<u64>,
    ) -> CassandraSinkCluster {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name.clone(), "transform" => "CassandraSinkCluster");
        let receive_timeout = timeout.map(Duration::from_secs);

        let nodes_shared = Arc::new(RwLock::new(vec![]));

        let (task_handshake_tx, task_handshake_rx) = mpsc::channel(1);
        create_topology_task(
            tls.clone(),
            nodes_shared.clone(),
            task_handshake_rx,
            data_center.clone(),
        );

        CassandraSinkCluster {
            contact_points,
            init_handshake_connection: None,
            init_handshake: vec![],
            init_handshake_address: None,
            init_handshake_complete: false,
            init_handshake_use_received: false,
            chain_name,
            failed_requests,
            tls,
            pushed_messages_tx: None,
            read_timeout: receive_timeout,
            local_table: FQName::new("system", "local"),
            peer_table: FQName::new("system", "peers"),
            data_center,
            rack,
            host_id,
            local_nodes: vec![],
            topology_task_nodes: nodes_shared,
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            task_handshake_tx,
        }
    }
}

impl CassandraSinkCluster {
    async fn send_message(
        &mut self,
        mut messages: Messages,
        local_addr: SocketAddr,
    ) -> ChainResponse {
        // Attempt to populate nodes list if we still dont have one yet
        if self.local_nodes.is_empty() {
            let nodes_shared = self.topology_task_nodes.read().await;
            self.local_nodes = nodes_shared.clone();
        }

        let tables_to_rewrite: Vec<TableToRewrite> = messages
            .iter_mut()
            .enumerate()
            .filter_map(|(i, m)| self.get_rewrite_table(m, i))
            .collect();

        for table_to_rewrite in tables_to_rewrite.iter().rev() {
            messages.remove(table_to_rewrite.index);
        }

        // Create the initial connection.
        // Messages will be sent through this connection until we have extracted the handshake.
        if self.init_handshake_connection.is_none() {
            let random_point = if let Some(random_point) = self.local_nodes.choose(&mut self.rng) {
                SocketAddr::new(random_point.address, 9042)
            } else {
                tokio::net::lookup_host(self.contact_points.choose(&mut self.rng).unwrap())
                    .await?
                    .next()
                    .unwrap()
            };
            self.init_handshake_connection = Some(
                CassandraConnection::new(
                    random_point,
                    CassandraCodec::new(),
                    self.tls.clone(),
                    self.pushed_messages_tx.clone(),
                )
                .await?,
            );
            self.init_handshake_address = Some(random_point);
        }

        if !self.init_handshake_complete {
            for message in &mut messages {
                // Filter operation types so we are only left with messages relevant to the handshake.
                // Due to shotover pipelining we could receive non-handshake messages while !self.init_handshake_complete.
                // Despite being used by the client in a handshake, CassandraOperation::Options is not included
                // because it doesnt dont alter the state of the server and so it isnt needed.
                if let Some(Frame::Cassandra(CassandraFrame {
                    operation: CassandraOperation::Startup(_) | CassandraOperation::AuthResponse(_),
                    ..
                })) = message.frame()
                {
                    self.init_handshake.push(message.clone());
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
                // DDL statements must be routed through the handshake connection so that later system.local queries are directed to the same node (they also go to the handshake connection)
                // They must be the same node so that schema_version changes appear immediately in system.local
                || is_ddl_statement(&mut message)
            {
                self.init_handshake_connection.as_mut().unwrap()
            } else if is_use_statement(&mut message) {
                // If we have already received a USE statement then pop it off the handshakes list to avoid infinite growth
                if self.init_handshake_use_received {
                    self.init_handshake.pop();
                }
                self.init_handshake_use_received = true;

                // Adding the USE statement to the handshake ensures that any new connection
                // created will have the correct keyspace setup.
                self.init_handshake.push(message.clone());

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
                random_node
                    .get_connection(&self.init_handshake, &self.tls, &self.pushed_messages_tx)
                    .await?
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
                    // Only send a handshake if the task really needs it
                    // i.e. when the channel of size 1 is empty
                    if let Ok(permit) = self.task_handshake_tx.try_reserve() {
                        permit.send(TaskHandshake {
                            handshake: self.init_handshake.clone(),
                            address: self.init_handshake_address.unwrap(),
                        })
                    }
                    self.init_handshake_complete = true;
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
            responses.insert(
                table_to_rewrite.index,
                self.rewrite_table(table_to_rewrite, local_addr).await?,
            );
        }

        Ok(responses)
    }

    fn get_rewrite_table(&self, request: &mut Message, index: usize) -> Option<TableToRewrite> {
        if let Some(Frame::Cassandra(cassandra)) = request.frame() {
            // No need to handle Batch as selects can only occur on Query
            if let CassandraOperation::Query { query, .. } = &cassandra.operation {
                if let CassandraStatement::Select(select) = query.as_ref() {
                    let ty = if self.local_table == select.table_name {
                        RewriteTableTy::Local
                    } else if self.peer_table == select.table_name {
                        RewriteTableTy::Peers
                    } else {
                        return None;
                    };

                    return Some(TableToRewrite {
                        index,
                        ty,
                        stream_id: cassandra.stream_id,
                        selects: select.columns.clone(),
                        version: cassandra.version,
                    });
                }
            }
        }
        None
    }

    async fn rewrite_table(
        &mut self,
        table: TableToRewrite,
        local_addr: SocketAddr,
    ) -> Result<Message> {
        let version = table.version;
        let stream_id = table.stream_id;
        let table_name = match table.ty {
            RewriteTableTy::Local => "local".into(),
            RewriteTableTy::Peers => "peers".into(),
        };
        let (rows, col_specs) = match table.ty {
            RewriteTableTy::Local => self.rewrite_table_local(table, local_addr).await?,
            RewriteTableTy::Peers => self.rewrite_table_peers(table).await?,
        };

        Ok(Message::from_frame(Frame::Cassandra(CassandraFrame {
            version,
            stream_id,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                // TODO: A bunch of these are just hardcoded with the assumption that the client didnt request any exotic features.
                // We should implement them eventually but I cant imagine drivers would bother using them when querying the topology.
                // TODO: flags should be removed upstream and just derived from the other fields at serialization time.
                metadata: Box::new(RowsMetadata {
                    flags: RowsMetadataFlags::GLOBAL_TABLE_SPACE,
                    columns_count: col_specs.len() as i32,
                    paging_state: None,
                    new_metadata_id: None,
                    global_table_spec: Some(TableSpec {
                        ks_name: "system".into(),
                        table_name,
                    }),
                    col_specs,
                }),
            }),
        })))
    }

    async fn rewrite_table_peers(
        &mut self,
        table: TableToRewrite,
    ) -> Result<(Vec<Vec<MessageValue>>, Vec<ColSpec>)> {
        let mut col_specs = vec![];
        let rows = vec![];

        let peer_ident = Identifier::Unquoted("peer".into());
        let data_center_ident = Identifier::Unquoted("data_center".into());
        let host_id_ident = Identifier::Unquoted("host_id".into());
        let preferred_ip = Identifier::Unquoted("preferred_ip".into());
        let rack_ident = Identifier::Unquoted("rack".into());
        let release_version_ident = Identifier::Unquoted("release_version".into());
        let rpc_address_ident = Identifier::Unquoted("rpc_address".into());
        let schema_version_ident = Identifier::Unquoted("schema_version".into());
        let tokens_ident = Identifier::Unquoted("tokens".into());

        for select in table.selects {
            match select {
                SelectElement::Star => {
                    col_specs.extend([
                        meta("peer".into(), ColType::Inet),
                        meta("data_center".into(), ColType::Varchar),
                        meta("host_id".into(), ColType::Uuid),
                        meta("preferred_ip".into(), ColType::Inet),
                        meta("rack".into(), ColType::Varchar),
                        meta("release_version".into(), ColType::Varchar),
                        meta("rpc_address".into(), ColType::Inet),
                        meta("schema_version".into(), ColType::Uuid),
                        meta_tokens("tokens".into()),
                    ]);
                }
                SelectElement::Function(name) => {
                    // TODO: 90% sure SelectElement::Function(name) is not actually a name but the entire function call stuffed in a name LOL
                    col_specs.push(meta(name.to_string(), ColType::Varchar));
                }
                SelectElement::Column(name) => {
                    if name.name == peer_ident {
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Inet));
                    } else if name.name == data_center_ident {
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Varchar));
                    } else if name.name == host_id_ident {
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Uuid));
                    } else if name.name == preferred_ip {
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Inet));
                    } else if name.name == rack_ident {
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Varchar));
                    } else if name.name == rpc_address_ident {
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Inet));
                    } else if name.name == release_version_ident {
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Varchar));
                    } else if name.name == schema_version_ident {
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Uuid));
                    } else if name.name == tokens_ident {
                        col_specs.push(meta_tokens(name.alias_or_name().to_string()));
                    } else {
                        tracing::error!("Unknown system.peer column requested {:?}", name)
                    }
                }
            }
        }

        // TODO: generate rows for shotover peers
        //       the current implementation will at least direct all traffic through shotover

        // TODO: schema_version and gossip_generation should be obtained by querying all nodes

        Ok((rows, col_specs))
    }

    async fn rewrite_table_local(
        &mut self,
        table: TableToRewrite,
        local_address: SocketAddr,
    ) -> Result<(Vec<Vec<MessageValue>>, Vec<ColSpec>)> {
        let outbound = self.init_handshake_connection.as_ref().unwrap();
        let (peers_tx, peers_rx) = oneshot::channel();
        outbound.send(
            Message::from_frame(Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                stream_id: 0,
                tracing_id: None,
                warnings: vec![],
                operation: CassandraOperation::Query {
                    query: Box::new(parse_statement_single(
                        "SELECT rack, data_center, schema_version, tokens, release_version FROM system.peers",
                    )),
                    params: Box::new(QueryParams::default()),
                },
            })),
            peers_tx,
        )?;

        let (local_tx, local_rx) = oneshot::channel();
        outbound.send(
            Message::from_frame(Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                stream_id: 1,
                tracing_id: None,
                warnings: vec![],
                operation: CassandraOperation::Query {
                    query: Box::new(parse_statement_single(
                        "SELECT cql_version, release_version, cluster_name, partitioner, schema_version, gossip_generation, tokens FROM system.local",
                    )),
                    params: Box::new(QueryParams::default()),
                },
            })),
            local_tx,
        )?;

        let (peers, local) = tokio::join!(
            async { parse_system_peers(peers_rx.await?.response?, &self.data_center, &self.rack) },
            async { parse_system_local(local_rx.await?.response?) },
        );
        let mut local = local?;
        let mut peers = peers?;

        for peer in &mut peers {
            local.tokens.append(&mut peer.tokens);
            if let Ok(Cmp::Lt) =
                version_compare::compare(&peer.release_version, &local.release_version)
            {
                std::mem::swap(&mut local.release_version, &mut peer.release_version);
            }
        }
        local.tokens.sort();
        for peer in &peers {
            if local.schema_version != peer.schema_version {
                local.schema_version = Uuid::new_v4();
                break;
            }
        }

        let version = match table.version {
            Version::V3 => "3".to_string(),
            Version::V4 => "4".to_string(),
            Version::V5 => "5".to_string(),
        };

        let key_ident = Identifier::Unquoted("key".into());
        let bootstrapped_ident = Identifier::Unquoted("bootstrapped".into());
        let broadcast_address_ident = Identifier::Unquoted("broadcast_address".into());
        let cluster_name_ident = Identifier::Unquoted("cluster_name".into());
        let cql_version_ident = Identifier::Unquoted("cql_version".into());
        let data_center_ident = Identifier::Unquoted("data_center".into());
        let gossip_generation_ident = Identifier::Unquoted("gossip_generation".into());
        let host_id_ident = Identifier::Unquoted("host_id".into());
        let listen_address_ident = Identifier::Unquoted("listen_address".into());
        let native_protocol_version_ident = Identifier::Unquoted("native_protocol_version".into());
        let partitioner_ident = Identifier::Unquoted("partitioner".into());
        let rack_ident = Identifier::Unquoted("rack".into());
        let release_version_ident = Identifier::Unquoted("release_version".into());
        let rpc_address_ident = Identifier::Unquoted("rpc_address".into());
        let schema_version_ident = Identifier::Unquoted("schema_version".into());
        let tokens_ident = Identifier::Unquoted("tokens".into());
        let truncated_at_ident = Identifier::Unquoted("truncated_at".into());
        let mut col_specs = vec![];
        let mut row = vec![];
        for select in table.selects {
            match select {
                SelectElement::Star => {
                    row.extend([
                        MessageValue::Varchar("local".into()),
                        MessageValue::Varchar("COMPLETED".into()),
                        MessageValue::Inet(local_address.ip()),
                        MessageValue::Varchar(local.cluster_name.clone()),
                        MessageValue::Varchar(local.cql_version.clone()),
                        MessageValue::Varchar(self.data_center.clone()),
                        MessageValue::Integer(local.gossip_generation, IntSize::I32),
                        MessageValue::Uuid(self.host_id),
                        MessageValue::Inet(local_address.ip()),
                        MessageValue::Varchar(version.clone()),
                        MessageValue::Varchar(local.partitioner.clone()),
                        MessageValue::Varchar(self.rack.clone()),
                        MessageValue::Varchar(local.release_version.clone()),
                        MessageValue::Inet("0.0.0.0".parse().unwrap()),
                        MessageValue::Uuid(local.schema_version),
                        MessageValue::List(local.tokens.clone()),
                        MessageValue::Null,
                    ]);
                    col_specs.extend([
                        meta("key".into(), ColType::Varchar),
                        meta("bootstrapped".into(), ColType::Varchar),
                        meta("broadcast_address".into(), ColType::Inet),
                        //meta("broadcast_port".into(), ColType::Int),
                        meta("cluster_name".into(), ColType::Varchar),
                        meta("cql_version".into(), ColType::Varchar),
                        meta("data_center".into(), ColType::Varchar),
                        meta("gossip_generation".into(), ColType::Int),
                        meta("host_id".into(), ColType::Uuid),
                        meta("listen_address".into(), ColType::Inet),
                        //meta("listen_port".into(), ColType::Int),
                        meta("native_protocol_version".into(), ColType::Varchar),
                        meta("partitioner".into(), ColType::Varchar),
                        meta("rack".into(), ColType::Varchar),
                        meta("release_version".into(), ColType::Varchar),
                        meta("rpc_address".into(), ColType::Inet),
                        //meta("rpc_port".into(), ColType::Int),
                        meta("schema_version".into(), ColType::Uuid),
                        meta_tokens("tokens".into()),
                        meta_truncated_at("truncated_at".into()),
                    ]);
                }
                SelectElement::Function(name) => {
                    row.push(MessageValue::Varchar(
                        "ERROR: Functions are not supported by shotover".into(),
                    ));
                    // TODO: 90% sure SelectElement::Function(name) is not actually a name but the entire function call stuffed in a name LOL
                    col_specs.push(meta(name.to_string(), ColType::Varchar));
                }
                SelectElement::Column(name) => {
                    if name.name == key_ident {
                        row.push(MessageValue::Varchar("local".into()));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Varchar));
                    } else if name.name == bootstrapped_ident {
                        row.push(MessageValue::Varchar("COMPLETED".into()));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Varchar));
                    } else if name.name == broadcast_address_ident {
                        row.push(MessageValue::Inet(local_address.ip()));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Inet));
                    } else if name.name == cluster_name_ident {
                        row.push(MessageValue::Varchar(local.cluster_name.clone()));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Varchar));
                    } else if name.name == cql_version_ident {
                        row.push(MessageValue::Varchar(local.cql_version.clone()));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Varchar));
                    } else if name.name == data_center_ident {
                        row.push(MessageValue::Varchar(self.data_center.clone()));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Varchar));
                    } else if name.name == gossip_generation_ident {
                        row.push(MessageValue::Integer(local.gossip_generation, IntSize::I32));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Int));
                    } else if name.name == host_id_ident {
                        row.push(MessageValue::Uuid(self.host_id));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Uuid));
                    } else if name.name == listen_address_ident {
                        row.push(MessageValue::Inet(local_address.ip()));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Inet));
                    } else if name.name == native_protocol_version_ident {
                        row.push(MessageValue::Varchar(version.clone()));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Varchar));
                    } else if name.name == partitioner_ident {
                        row.push(MessageValue::Varchar(local.partitioner.clone()));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Varchar));
                    } else if name.name == rack_ident {
                        row.push(MessageValue::Varchar(self.rack.clone()));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Varchar));
                    } else if name.name == release_version_ident {
                        row.push(MessageValue::Varchar(local.release_version.clone()));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Varchar));
                    } else if name.name == rpc_address_ident {
                        row.push(MessageValue::Inet("0.0.0.0".parse().unwrap()));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Inet));
                    } else if name.name == schema_version_ident {
                        row.push(MessageValue::Uuid(local.schema_version));
                        col_specs.push(meta(name.alias_or_name().to_string(), ColType::Uuid));
                    } else if name.name == tokens_ident {
                        row.push(MessageValue::List(local.tokens.clone()));
                        col_specs.push(meta_tokens(name.alias_or_name().to_string()));
                    } else if name.name == truncated_at_ident {
                        row.push(MessageValue::Null);
                        col_specs.push(meta_truncated_at(name.alias_or_name().to_string()));
                    } else {
                        tracing::error!("Unknown system.local column requested {:?}", name)
                    }
                }
            }
        }

        Ok((vec![row], col_specs))
    }
}

struct TableToRewrite {
    index: usize,
    ty: RewriteTableTy,
    stream_id: i16,
    version: Version,
    selects: Vec<SelectElement>,
}

enum RewriteTableTy {
    Local,
    Peers,
}

fn meta(name: String, col_type: ColType) -> ColSpec {
    ColSpec {
        name,
        table_spec: None,
        col_type: ColTypeOption {
            id: col_type,
            value: None,
        },
    }
}

fn meta_tokens(name: String) -> ColSpec {
    ColSpec {
        name,
        table_spec: None,
        col_type: ColTypeOption {
            id: ColType::Set,
            value: Some(ColTypeOptionValue::CSet(Box::new(ColTypeOption {
                id: ColType::Varchar,
                value: None,
            }))),
        },
    }
}

fn meta_truncated_at(name: String) -> ColSpec {
    ColSpec {
        name,
        table_spec: None,
        col_type: ColTypeOption {
            id: ColType::Map,
            value: Some(ColTypeOptionValue::CMap(
                Box::new(ColTypeOption {
                    id: ColType::Uuid,
                    value: None,
                }),
                Box::new(ColTypeOption {
                    id: ColType::Blob,
                    value: None,
                }),
            )),
        },
    }
}

pub fn create_topology_task(
    tls: Option<TlsConnector>,
    nodes: Arc<RwLock<Vec<CassandraNode>>>,
    mut handshake_rx: mpsc::Receiver<TaskHandshake>,
    data_center: String,
) {
    tokio::spawn(async move {
        while let Some(handshake) = handshake_rx.recv().await {
            let mut attempts = 0;
            while let Err(err) = topology_task_process(&tls, &nodes, &handshake, &data_center).await
            {
                tracing::error!("topology task failed, retrying, error was: {err:?}");
                attempts += 1;
                if attempts > 3 {
                    // 3 attempts have failed, lets try a new handshake
                    break;
                }
            }

            // Sleep for an hour.
            // TODO: This is a crude way to ensure we dont overload the transforms with too many topology changes.
            // This will be replaced with:
            // * the task subscribes to events
            // * the transforms request a reload when they hit connection errors
            tokio::time::sleep(std::time::Duration::from_secs(60 * 60)).await;
        }
    });
}

async fn topology_task_process(
    tls: &Option<TlsConnector>,
    nodes: &Arc<RwLock<Vec<CassandraNode>>>,
    handshake: &TaskHandshake,
    data_center: &str,
) -> Result<()> {
    let outbound =
        node::new_connection(&handshake.address, &handshake.handshake, tls, &None).await?;

    let (peers_tx, peers_rx) = oneshot::channel();
    outbound.send(
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single(
                    "SELECT peer, rack, data_center, tokens FROM system.peers",
                )),
                params: Box::new(QueryParams::default()),
            },
        })),
        peers_tx,
    )?;

    let (local_tx, local_rx) = oneshot::channel();
    outbound.send(
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 1,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single(
                    "SELECT listen_address, rack, data_center, tokens FROM system.local",
                )),
                params: Box::new(QueryParams::default()),
            },
        })),
        local_tx,
    )?;

    let (new_nodes, more_nodes) = tokio::join!(
        async { system_peers_into_nodes(peers_rx.await?.response?, data_center) },
        async { system_peers_into_nodes(local_rx.await?.response?, data_center) }
    );
    let mut new_nodes = new_nodes?;
    new_nodes.extend(more_nodes?);

    let mut write_lock = nodes.write().await;
    let expensive_drop = std::mem::replace(&mut *write_lock, new_nodes);

    // Make sure to drop write_lock before the expensive_drop which will have to perform many deallocations.
    std::mem::drop(write_lock);
    std::mem::drop(expensive_drop);

    Ok(())
}

fn system_peers_into_nodes(
    mut response: Message,
    config_data_center: &str,
) -> Result<Vec<CassandraNode>> {
    if let Some(Frame::Cassandra(frame)) = response.frame() {
        match &mut frame.operation {
            CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                ..
            }) => rows
                .iter_mut()
                .filter(|row| {
                    if let Some(MessageValue::Varchar(data_center)) = row.get(2) {
                        data_center == config_data_center
                    } else {
                        false
                    }
                })
                .map(|row| {
                    if row.len() != 4 {
                        return Err(anyhow!("expected 4 columns but was {}", row.len()));
                    }

                    let tokens = if let Some(MessageValue::List(list)) = row.pop() {
                        list.into_iter()
                            .map::<Result<String>, _>(|x| match x {
                                MessageValue::Varchar(a) => Ok(a),
                                _ => Err(anyhow!("tokens value not a varchar")),
                            })
                            .collect::<Result<Vec<String>>>()?
                    } else {
                        return Err(anyhow!("tokens not a list"));
                    };
                    let _data_center = row.pop();
                    let rack = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("rack not a varchar"));
                    };
                    let address = if let Some(MessageValue::Inet(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("address not an inet"));
                    };

                    Ok(CassandraNode {
                        address,
                        _rack: rack,
                        _tokens: tokens,
                        outbound: None,
                    })
                })
                .collect(),
            operation => Err(anyhow!(
                "system.peers returned unexpected cassandra operation: {:?}",
                operation
            )),
        }
    } else {
        Err(anyhow!(
            "Failed to parse system.peers response {:?}",
            response
        ))
    }
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

struct SystemLocal {
    schema_version: Uuid,
    gossip_generation: i64,
    tokens: Vec<MessageValue>,
    partitioner: String,
    cluster_name: String,
    release_version: String,
    cql_version: String,
}

struct SystemPeer {
    tokens: Vec<MessageValue>,
    schema_version: Uuid,
    release_version: String,
}

fn parse_system_peers(
    mut response: Message,
    config_data_center: &str,
    config_rack: &str,
) -> Result<Vec<SystemPeer>> {
    if let Some(Frame::Cassandra(frame)) = response.frame() {
        match &mut frame.operation {
            CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                ..
            }) => rows
                .iter_mut()
                .filter(|row| {
                    if let (
                        Some(MessageValue::Varchar(data_center)),
                        Some(MessageValue::Varchar(rack)),
                    ) = (row.get(1), row.get(0))
                    {
                        data_center == config_data_center && rack == config_rack
                    } else {
                        false
                    }
                })
                .map(|row| {
                    if row.len() != 5 {
                        return Err(anyhow!("expected 5 columns but was {}", row.len()));
                    }

                    let release_version = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("release_version not a list"));
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

                    let _data_center = row.pop();
                    let _rack = row.pop();

                    Ok(SystemPeer {
                        tokens,
                        schema_version,
                        release_version,
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

fn parse_system_local(mut response: Message) -> Result<SystemLocal> {
    if let Some(Frame::Cassandra(frame)) = response.frame() {
        match &mut frame.operation {
            CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                ..
            }) => {
                if rows.len() > 1 {
                    tracing::error!("system.local returned more than one row");
                }
                if let Some(row) = rows.first_mut() {
                    if row.len() != 7 {
                        return Err(anyhow!("expected 7 columns but was {}", row.len()));
                    }

                    let tokens = if let Some(MessageValue::List(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("tokens not a list"));
                    };

                    let gossip_generation = if let Some(MessageValue::Integer(value, _)) = row.pop()
                    {
                        value
                    } else {
                        return Err(anyhow!("gossip_generation not an int"));
                    };

                    let schema_version = if let Some(MessageValue::Uuid(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("schema_version not a uuid"));
                    };

                    let partitioner = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("partitioner not a varchar"));
                    };

                    let cluster_name = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("cluster_name not a varchar"));
                    };

                    let release_version = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("release_version not a varchar"));
                    };

                    let cql_version = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("cql_version not a varchar"));
                    };

                    Ok(SystemLocal {
                        schema_version,
                        gossip_generation,
                        tokens,
                        partitioner,
                        cluster_name,
                        release_version,
                        cql_version,
                    })
                } else {
                    Err(anyhow!("system.local returned no rows"))
                }
            }
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
        self.send_message(message_wrapper.messages, message_wrapper.local_addr)
            .await
    }

    fn is_terminating(&self) -> bool {
        true
    }

    fn add_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }
}

#[derive(Debug)]
pub struct TaskHandshake {
    pub handshake: Vec<Message>,
    pub address: SocketAddr,
}
