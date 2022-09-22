use super::node::{CassandraNode, ConnectionFactory};
use crate::frame::cassandra::parse_statement_single;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{Message, MessageValue};
use crate::transforms::cassandra::connection::CassandraConnection;
use anyhow::{anyhow, Result};
use cassandra_protocol::token::Murmur3Token;
use cassandra_protocol::{frame::Version, query::QueryParams};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};

#[derive(Debug)]
pub struct TaskConnectionInfo {
    pub connection_factory: ConnectionFactory,
    pub address: SocketAddr,
}

pub fn create_topology_task(
    nodes: Arc<RwLock<Vec<CassandraNode>>>,
    mut handshake_rx: mpsc::Receiver<TaskConnectionInfo>,
    data_center: String,
) {
    tokio::spawn(async move {
        while let Some(handshake) = handshake_rx.recv().await {
            let mut attempts = 0;
            while let Err(err) = topology_task_process(&nodes, &handshake, &data_center).await {
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
    nodes: &Arc<RwLock<Vec<CassandraNode>>>,
    handshake: &TaskConnectionInfo,
    data_center: &str,
) -> Result<()> {
    let outbound = handshake
        .connection_factory
        .new_connection(handshake.address)
        .await?;

    let (new_nodes, more_nodes) = tokio::join!(
        system_local::query(&outbound, data_center, handshake.address),
        system_peers::query(&outbound, data_center)
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

mod system_local {
    use super::*;

    pub async fn query(
        connection: &CassandraConnection,
        data_center: &str,
        address: SocketAddr,
    ) -> Result<Vec<CassandraNode>> {
        let (tx, rx) = oneshot::channel();
        connection.send(
            Message::from_frame(Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                stream_id: 1,
                tracing_id: None,
                warnings: vec![],
                operation: CassandraOperation::Query {
                    query: Box::new(parse_statement_single(
                        "SELECT rack, tokens, host_id, data_center FROM system.local",
                    )),
                    params: Box::new(QueryParams::default()),
                },
            })),
            tx,
        )?;

        into_nodes(rx.await?.response?, data_center, address)
    }

    fn into_nodes(
        mut response: Message,
        config_data_center: &str,
        address: SocketAddr,
    ) -> Result<Vec<CassandraNode>> {
        if let Some(Frame::Cassandra(frame)) = response.frame() {
            match &mut frame.operation {
                CassandraOperation::Result(CassandraResult::Rows { rows, .. }) => rows
                    .iter_mut()
                    .filter(|row| {
                        if let Some(MessageValue::Varchar(data_center)) = row.last() {
                            data_center == config_data_center
                        } else {
                            false
                        }
                    })
                    .map(|row| {
                        let _data_center = row.pop();

                        let host_id = if let Some(MessageValue::Uuid(host_id)) = row.pop() {
                            host_id
                        } else {
                            return Err(anyhow!("system.local.host_id not a uuid"));
                        };

                        let tokens = if let Some(MessageValue::List(mut list)) = row.pop() {
                            list.drain(..)
                                .map::<Result<Murmur3Token>, _>(|x| match x {
                                    MessageValue::Varchar(a) => Ok(a.try_into()?),
                                    _ => Err(anyhow!("system.local.tokens value not a varchar")),
                                })
                                .collect::<Result<Vec<Murmur3Token>>>()?
                        } else {
                            return Err(anyhow!("system.local.tokens not a list"));
                        };

                        let rack = if let Some(MessageValue::Varchar(value)) = row.pop() {
                            value
                        } else {
                            return Err(anyhow!("system.local.rack not a varchar"));
                        };

                        Ok(CassandraNode {
                            address,
                            rack,
                            tokens,
                            outbound: None,
                            host_id,
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
                "Failed to parse system.local response {:?}",
                response
            ))
        }
    }
}

mod system_peers {
    use super::*;

    pub async fn query(
        connection: &CassandraConnection,
        data_center: &str,
    ) -> Result<Vec<CassandraNode>> {
        let (tx, rx) = oneshot::channel();
        connection.send(
            Message::from_frame(Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                stream_id: 0,
                tracing_id: None,
                warnings: vec![],
                operation: CassandraOperation::Query {
                    query: Box::new(parse_statement_single(
                        "SELECT native_port, native_address, rack, tokens, host_id, data_center FROM system.peers_v2",
                    )),
                    params: Box::new(QueryParams::default()),
                },
            })),
            tx,
        )?;

        let mut response = rx.await?.response?;

        if is_peers_v2_does_not_exist_error(&mut response) {
            let (tx, rx) = oneshot::channel();
            connection.send(
                Message::from_frame(Frame::Cassandra(CassandraFrame {
                    version: Version::V4,
                    stream_id: 0,
                    tracing_id: None,
                    warnings: vec![],
                    operation: CassandraOperation::Query {
                        query: Box::new(parse_statement_single(
                            "SELECT peer, rack, tokens, host_id, data_center FROM system.peers",
                        )),
                        params: Box::new(QueryParams::default()),
                    },
                })),
                tx,
            )?;
            response = rx.await?.response?;
        }

        into_nodes(response, data_center)
    }

    fn is_peers_v2_does_not_exist_error(message: &mut Message) -> bool {
        if let Some(Frame::Cassandra(CassandraFrame {
            operation: CassandraOperation::Error(error),
            ..
        })) = message.frame()
        {
            return error.message == "unconfigured table peers_v2";
        }

        false
    }

    fn into_nodes(mut response: Message, config_data_center: &str) -> Result<Vec<CassandraNode>> {
        if let Some(Frame::Cassandra(frame)) = response.frame() {
            match &mut frame.operation {
                CassandraOperation::Result(CassandraResult::Rows { rows, .. }) => rows
                    .iter_mut()
                    .filter(|row| {
                        if let Some(MessageValue::Varchar(data_center)) = row.last() {
                            data_center == config_data_center
                        } else {
                            false
                        }
                    })
                    .map(|row| {
                        if row.len() != 5 && row.len() != 6 {
                            return Err(anyhow!("expected 5 or 6 columns but was {}", row.len()));
                        }

                        let _data_center = row.pop();

                        let host_id = if let Some(MessageValue::Uuid(host_id)) = row.pop() {
                            host_id
                        } else {
                            return Err(anyhow!("system.peers(v2).host_id not a uuid"));
                        };

                        let tokens = if let Some(MessageValue::List(list)) = row.pop() {
                            list.into_iter()
                                .map::<Result<Murmur3Token>, _>(|x| match x {
                                    MessageValue::Varchar(a) => Ok(a.try_into()?),
                                    _ => {
                                        Err(anyhow!("system.peers(v2).tokens value not a varchar"))
                                    }
                                })
                                .collect::<Result<Vec<Murmur3Token>>>()?
                        } else {
                            return Err(anyhow!("system.peers(v2).tokens not a list"));
                        };

                        let rack = if let Some(MessageValue::Varchar(value)) = row.pop() {
                            value
                        } else {
                            return Err(anyhow!("system.peers(v2).rack not a varchar"));
                        };

                        let ip = if let Some(MessageValue::Inet(value)) = row.pop() {
                            value
                        } else {
                            return Err(anyhow!("system.peers(v2).native_address not an inet"));
                        };

                        let port = if let Some(message_value) = row.pop() {
                            if let MessageValue::Integer(value, _) = message_value {
                                value
                            } else {
                                return Err(anyhow!("system.peers(v2).port is not an integer"));
                            }
                        } else {
                            //this method supports both system.peers and system.peers_v2, system.peers does not have a field for the port so we fallback to the default port.
                            9042
                        };

                        Ok(CassandraNode {
                            address: SocketAddr::new(ip, port.try_into()?),
                            rack,
                            tokens,
                            outbound: None,
                            host_id,
                        })
                    })
                    .collect(),
                operation => Err(anyhow!(
                    "system.peers or system.peers_v2 returned unexpected cassandra operation: {:?}",
                    operation
                )),
            }
        } else {
            Err(anyhow!(
                "Failed to parse system.peers or system.peers_v2 response {:?}",
                response
            ))
        }
    }
}
