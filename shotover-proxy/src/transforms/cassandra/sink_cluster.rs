use super::connection::CassandraConnection;
use crate::codec::cassandra::CassandraCodec;
use crate::concurrency::FuturesOrdered;
use crate::error::ChainResponse;
use crate::frame::cassandra::parse_statement_single;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{Message, MessageValue, Messages};
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::frame::Version;
use cassandra_protocol::query::QueryParams;
use cql3_parser::common::{FQName, Identifier};
use metrics::{register_counter, Counter};
use rand::prelude::*;
use serde::Deserialize;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::ToSocketAddrs;
use tokio::sync::{mpsc, oneshot, RwLock};

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraSinkClusterConfig {
    pub first_contact_points: Vec<String>,
    pub data_center: String,
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
            tls,
            self.read_timeout,
        )))
    }
}

pub struct CassandraSinkCluster {
    contact_points: Vec<String>,
    contact_point_connection: Option<CassandraConnection>,
    init_handshake: Vec<Message>,
    init_handshake_address: Option<String>,
    init_handshake_complete: bool,
    chain_name: String,
    failed_requests: Counter,
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    read_timeout: Option<Duration>,
    peer_table: FQName,
    data_center: String,
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
            contact_point_connection: None,
            init_handshake: vec![],
            init_handshake_address: None,
            init_handshake_complete: false,
            chain_name: self.chain_name.clone(),
            tls: self.tls.clone(),
            failed_requests: self.failed_requests.clone(),
            pushed_messages_tx: None,
            read_timeout: self.read_timeout,
            peer_table: self.peer_table.clone(),
            data_center: self.data_center.clone(),
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
            contact_point_connection: None,
            init_handshake: vec![],
            init_handshake_address: None,
            init_handshake_complete: false,
            chain_name,
            failed_requests,
            tls,
            pushed_messages_tx: None,
            read_timeout: receive_timeout,
            peer_table: FQName::new("system", "peers"),
            data_center,
            local_nodes: vec![],
            topology_task_nodes: nodes_shared,
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            task_handshake_tx,
        }
    }
}

impl CassandraSinkCluster {
    async fn send_message(&mut self, messages: Messages) -> ChainResponse {
        // Create the initial connection.
        // Messages will be sent through this connection until we have extracted the handshake and list of nodes
        // TODO: initial connection should come from node list too
        if self.contact_point_connection.is_none() {
            let random_point = self.contact_points.choose(&mut self.rng).unwrap();
            self.contact_point_connection = Some(
                CassandraConnection::new(
                    random_point,
                    CassandraCodec::new(),
                    self.tls.clone(),
                    self.pushed_messages_tx.clone(),
                )
                .await?,
            );
            self.init_handshake_address = Some(random_point.clone());
        }

        // Attempt to populate nodes list if we still dont have one yet
        if self.local_nodes.is_empty() {
            let nodes_shared = self.topology_task_nodes.read().await;
            self.local_nodes.extend(nodes_shared.iter().cloned());
        }

        if !self.init_handshake_complete {
            for message in &messages {
                if let Some(last) = self.init_handshake.last_mut() {
                    if let Some(Frame::Cassandra(CassandraFrame {
                        operation: CassandraOperation::AuthResponse(_),
                        ..
                    })) = last.frame()
                    {
                        // Only send a handshake if the task really needs it
                        // i.e. when the channel of size 1 is empty
                        if let Ok(permit) = self.task_handshake_tx.try_reserve() {
                            permit.send(TaskHandshake {
                                handshake: self.init_handshake.clone(),
                                address: self.init_handshake_address.as_ref().unwrap().clone(),
                            })
                        }
                        self.init_handshake_complete = true;
                        break;
                    }
                }
                self.init_handshake.push(message.clone());
            }
        }

        let mut responses_future = FuturesOrdered::new();
        for message in messages {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            if self.local_nodes.is_empty() || !self.init_handshake_complete {
                self.contact_point_connection.as_mut().unwrap()
            } else {
                let random_node = self.local_nodes.choose_mut(&mut self.rng).unwrap();
                random_node
                    .get_connection(&self.init_handshake, &self.tls, &self.pushed_messages_tx)
                    .await?
            }
            .send(message, return_chan_tx)?;

            responses_future.push(return_chan_rx)
        }

        let responses =
            super::connection::receive(self.read_timeout, &self.failed_requests, responses_future)
                .await?;

        Ok(responses)
    }
}

fn create_topology_task(
    tls: Option<TlsConnector>,
    nodes: Arc<RwLock<Vec<CassandraNode>>>,
    mut handshake_rx: mpsc::Receiver<TaskHandshake>,
    data_center: String,
) {
    tokio::spawn(async move {
        while let Some(handshake) = handshake_rx.recv().await {
            if let Err(err) = topology_task_process(&tls, &nodes, handshake, &data_center).await {
                tracing::error!("{err:?}");
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
    handshake: TaskHandshake,
    data_center: &str,
) -> Result<()> {
    let outbound = new_connection(handshake.address, &handshake.handshake, tls, &None).await?;

    let (return_chan_tx, return_chan_rx) = oneshot::channel();
    outbound.send(
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single("SELECT * FROM system.peers")),
                params: Box::new(QueryParams {
                    consistency: Consistency::One,
                    with_names: false,
                    values: None,
                    page_size: Some(5000),
                    paging_state: None,
                    serial_consistency: None,
                    timestamp: Some(1643855761086585),
                    keyspace: None,
                    now_in_seconds: None,
                }),
            },
        })),
        return_chan_tx,
    )?;
    let mut response = return_chan_rx.await?.response?;
    let new_nodes = get_nodes_from_system_peers(&mut response, data_center);

    let mut write_lock = nodes.write().await;
    let expensive_drop = std::mem::replace(&mut *write_lock, new_nodes);

    // Make sure to drop write_lock before the expensive_drop which will have to perform many deallocations.
    std::mem::drop(write_lock);
    std::mem::drop(expensive_drop);

    Ok(())
}

fn get_nodes_from_system_peers(
    response: &mut Message,
    config_data_center: &str,
) -> Vec<CassandraNode> {
    let mut new_nodes = vec![];
    let peer_ident = Identifier::Unquoted("peer".into());
    let rack_ident = Identifier::Unquoted("rack".into());
    let data_center_ident = Identifier::Unquoted("data_center".into());
    let tokens_ident = Identifier::Unquoted("tokens".into());

    if let Some(Frame::Cassandra(frame)) = response.frame() {
        // CassandraOperation::Error(_) is another possible case, we should silently ignore such cases
        if let CassandraOperation::Result(CassandraResult::Rows {
            value: MessageValue::Rows(rows),
            metadata,
        }) = &mut frame.operation
        {
            for row in rows.iter() {
                let mut address = None;
                let mut rack = None;
                let mut data_center = None;
                let mut tokens = vec![];
                for (i, col) in metadata.col_specs.iter().enumerate() {
                    let ident = Identifier::parse(&col.name);
                    if ident == peer_ident {
                        if let Some(MessageValue::Inet(value)) = row.get(i) {
                            address = Some(*value);
                        }
                    } else if ident == rack_ident {
                        if let Some(MessageValue::Varchar(value)) = row.get(i) {
                            rack = Some(value.clone());
                        }
                    } else if ident == data_center_ident {
                        if let Some(MessageValue::Varchar(value)) = row.get(i) {
                            data_center = Some(value.clone());
                        }
                    } else if ident == tokens_ident {
                        if let Some(MessageValue::List(list)) = row.get(i) {
                            tokens = list
                                .iter()
                                .filter_map(|x| match x {
                                    MessageValue::Varchar(a) => Some(a.clone()),
                                    _ => None,
                                })
                                .collect();
                        }
                    }
                }
                if let (Some(address), Some(rack), Some(data_center)) = (address, rack, data_center)
                {
                    if data_center == config_data_center {
                        new_nodes.push(CassandraNode {
                            address,
                            _rack: rack,
                            _tokens: tokens,
                            outbound: None,
                        });
                    }
                }
            }
        }
    }
    new_nodes
}

#[async_trait]
impl Transform for CassandraSinkCluster {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.send_message(message_wrapper.messages).await
    }

    fn is_terminating(&self) -> bool {
        true
    }

    fn add_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }
}

#[derive(Debug, Clone)]
struct CassandraNode {
    address: IpAddr,
    _rack: String,
    _tokens: Vec<String>,
    outbound: Option<CassandraConnection>,
}

struct TaskHandshake {
    handshake: Vec<Message>,
    address: String,
}

impl CassandraNode {
    async fn get_connection(
        &mut self,
        handshake: &[Message],
        tls: &Option<TlsConnector>,
        pushed_messages_tx: &Option<mpsc::UnboundedSender<Messages>>,
    ) -> Result<&mut CassandraConnection> {
        if self.outbound.is_none() {
            self.outbound = Some(
                new_connection((self.address, 9042), handshake, tls, pushed_messages_tx).await?,
            )
        }

        Ok(self.outbound.as_mut().unwrap())
    }
}

async fn new_connection<A: ToSocketAddrs>(
    address: A,
    handshake: &[Message],
    tls: &Option<TlsConnector>,
    pushed_messages_tx: &Option<mpsc::UnboundedSender<Messages>>,
) -> Result<CassandraConnection> {
    let outbound = CassandraConnection::new(
        address,
        CassandraCodec::new(),
        tls.clone(),
        pushed_messages_tx.clone(),
    )
    .await?;

    for handshake_message in handshake {
        let (return_chan_tx, return_chan_rx) = oneshot::channel();
        outbound.send(handshake_message.clone(), return_chan_tx)?;
        return_chan_rx.await?;
    }

    Ok(outbound)
}
