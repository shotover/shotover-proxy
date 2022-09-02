use super::node::{CassandraNode, ConnectionFactory};
use crate::frame::cassandra::parse_statement_single;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{Message, MessageValue};
use anyhow::{anyhow, Result};
use cassandra_protocol::frame::Version;
use cassandra_protocol::query::QueryParams;
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
                    "SELECT broadcast_address, rack, data_center, tokens FROM system.local",
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
                        rack,
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
