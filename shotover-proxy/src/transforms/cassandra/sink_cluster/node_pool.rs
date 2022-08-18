use super::super::connection::{receive, CassandraConnection};
use super::node::CassandraNode;
use super::node::ConnectionFactory;
use crate::error::ChainResponse;
use crate::frame::cassandra::parse_statement_single;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{Message, MessageValue, Messages};
use crate::tls::TlsConnector;
use anyhow::{anyhow, Result};
use arc_swap::ArcSwapOption;
use cassandra_protocol::frame::Version;
use cassandra_protocol::query::QueryParams;
use futures::stream::FuturesOrdered;
use metrics::register_counter;
use rand::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};

#[derive(Debug)]
pub enum TopologyTask {
    TaskHandshake(TaskHandshake),
    Keyspace(Message),
}

#[derive(Debug)]
struct TaskHandshake {
    pub address: SocketAddr,
    pub connection_factory: ConnectionFactory,
}

pub struct NodePool {
    /// A local clone of topology_task_nodes
    /// Internally stores connections to the nodes
    nodes: Vec<CassandraNode>,
    contact_points: Vec<String>,

    /// Only written to by the topology task
    /// Transform instances should never write to this.
    topology_task_nodes: Arc<RwLock<Vec<CassandraNode>>>,

    connection_factory: Arc<ConnectionFactory>,
    handshake_complete: bool,

    control_connection: ArcSwapOption<CassandraConnection>,

    task_tx: mpsc::Sender<TopologyTask>,
}

impl NodePool {
    pub fn new(
        contact_points: Vec<String>,
        data_center: String,
        tls: Option<TlsConnector>,
    ) -> Self {
        let (task_tx, task_rx) = mpsc::channel(1);

        let nodes_shared = Arc::new(RwLock::new(vec![]));

        create_topology_task(nodes_shared.clone(), task_rx, data_center);

        Self {
            nodes: vec![],
            contact_points,
            topology_task_nodes: nodes_shared,

            connection_factory: Arc::new(ConnectionFactory::new(tls)),

            control_connection: ArcSwapOption::empty(),

            handshake_complete: false,

            task_tx,
        }
    }

    pub fn send_messages(&mut self, messages: Messages) {
        // if handshake not complete
        //
        if !self.handshake_complete {}

        // use statement
        //

        // other query
    }

    pub async fn use_keyspace(&mut self, message: Message) -> ChainResponse {
        let control_connection = self.get_control_connection().await?;

        // send keyspace handler event
        //
        self.task_tx.send(TopologyTask::Keyspace(message.clone()));

        if self.connection_factory.has_use_message() {
            // remove it

            self.connection_factory.pop_use_message();
        }
        self.connection_factory.add_use_message(message.clone());

        //
        // update connection factory
        //
        // return response from connection
        //
        let (return_chan_tx, return_chan_rx) = oneshot::channel();
        control_connection.send(message, return_chan_tx);
        let mut responses_future = FuturesOrdered::new();
        responses_future.push_back(return_chan_rx);
        let failed_requests = register_counter!("failed_requests");
        let responses = receive(None, &failed_requests, responses_future).await?;

        Ok(responses)
    }

    pub async fn route_to_node(&mut self, messages: Messages) -> ChainResponse {
        // choose connection and send node
        //
        // We have a full nodes list and handshake, so we can do proper routing now.
        //let random_node = self.local_nodes.choose_mut(&mut self.rng).unwrap();
        //random_node.get_connection(&self.connection_factory).await?
        let control_connection = self.get_control_connection().await?;

        let mut responses_future = FuturesOrdered::new();
        for message in messages {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            control_connection.send(message, return_chan_tx);
            responses_future.push_back(return_chan_rx)
        }

        let failed_requests = register_counter!("failed_requests");
        let responses = receive(None, &failed_requests, responses_future).await?;

        Ok(responses)
    }

    async fn get_control_connection(&mut self) -> Result<Arc<CassandraConnection>> {
        if self.control_connection.load().is_none() {
            let random_point = tokio::net::lookup_host(
                self.contact_points
                    .choose(&mut SmallRng::from_rng(rand::thread_rng()).unwrap())
                    .unwrap(),
            )
            .await?
            .next()
            .unwrap();

            self.control_connection.store(Some(Arc::new(
                self.connection_factory.new_connection(random_point).await?,
            )));
        }

        let control_connection = self.control_connection.load().clone().unwrap();
        Ok(control_connection)
    }

    pub async fn handshake_messages(&mut self, mut messages: Messages) -> ChainResponse {
        //if no control connection create one

        let control_connection = self.get_control_connection().await?;

        // if not complete filter out any handshake messages and add to the connection factory
        // if the handshake is not complete grab the control connection and send the messages
        // handshake_messages()
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
                self.connection_factory
                    .push_handshake_message(message.clone());
            }
        }

        // send all the handshake messages
        //
        let mut responses_future = FuturesOrdered::new();
        for message in messages {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            control_connection.send(message, return_chan_tx);
            responses_future.push_back(return_chan_rx)
        }

        // TODO
        let failed_requests = register_counter!("failed_requests");
        let mut responses = receive(None, &failed_requests, responses_future).await?;

        // check handshake messages? mark handshake as complete
        for response in &mut responses {
            if let Some(Frame::Cassandra(CassandraFrame {
                operation: CassandraOperation::Ready(_) | CassandraOperation::AuthSuccess(_),
                ..
            })) = response.frame()
            {
                self.handshake_complete = true;
                break;
            }
        }

        // // if complete forward task

        // return response from control connection
        Ok(responses)
    }
}

pub fn create_topology_task(
    nodes: Arc<RwLock<Vec<CassandraNode>>>,
    mut handshake_rx: mpsc::Receiver<TopologyTask>,
    data_center: String,
) {
    tokio::spawn(async move {
        while let Some(task) = handshake_rx.recv().await {
            match task {
                TopologyTask::TaskHandshake(handshake) => {
                    let mut attempts = 0;
                    while let Err(err) =
                        topology_task_process(&nodes, &handshake, &data_center).await
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
                TopologyTask::Keyspace(message) => {

                    // TODO send to all nodes and make sure responses are correct
                    // otherwise remove
                }
            }
        }
    });
}

async fn topology_task_process(
    nodes: &Arc<RwLock<Vec<CassandraNode>>>,
    task_handshake: &TaskHandshake,
    data_center: &str,
) -> Result<()> {
    let outbound = task_handshake
        .connection_factory
        .new_connection(&task_handshake.address)
        .await?;

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
