use super::node::{CassandraNode, ConnectionFactory};
use super::node_pool::KeyspaceMetadata;
use super::KeyspaceChanTx;
use crate::frame::cassandra::{parse_statement_single, Tracing};
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{Message, MessageValue};
use crate::transforms::cassandra::connection::CassandraConnection;
use anyhow::{anyhow, Result};
use cassandra_protocol::events::{ServerEvent, SimpleServerEvent};
use cassandra_protocol::frame::events::{StatusChangeType, TopologyChangeType};
use cassandra_protocol::frame::message_register::BodyReqRegister;
use cassandra_protocol::token::Murmur3Token;
use cassandra_protocol::{frame::Version, query::QueryParams};
use itertools::Itertools;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Debug)]
pub struct TaskConnectionInfo {
    pub connection_factory: ConnectionFactory,
    pub address: SocketAddr,
}

pub fn create_topology_task(
    nodes_tx: watch::Sender<Vec<CassandraNode>>,
    keyspaces_tx: KeyspaceChanTx,
    mut connection_info_rx: mpsc::Receiver<TaskConnectionInfo>,
    data_center: String,
) {
    tokio::spawn(async move {
        while let Some(mut connection_info) = connection_info_rx.recv().await {
            let mut attempts = 0;
            match topology_task_process(
                &nodes_tx,
                &keyspaces_tx,
                &mut connection_info,
                &data_center,
            )
            .await
            {
                Err(err) => {
                    tracing::error!("topology task failed, retrying, error was: {err:?}");
                    attempts += 1;
                    if attempts > 3 {
                        // 3 attempts have failed, lets try a new handshake
                        break;
                    }
                }
                Ok(()) => {
                    // cleanly shutdown the task
                    return;
                }
            }
        }
    });
}

async fn topology_task_process(
    nodes_tx: &watch::Sender<Vec<CassandraNode>>,
    keyspaces_tx: &KeyspaceChanTx,
    connection_info: &mut TaskConnectionInfo,
    data_center: &str,
) -> Result<()> {
    let (pushed_messages_tx, mut pushed_messages_rx) = unbounded_channel();
    connection_info
        .connection_factory
        .set_pushed_messages_tx(pushed_messages_tx);

    let connection = connection_info
        .connection_factory
        .new_connection(connection_info.address)
        .await?;

    let version = connection_info.connection_factory.get_version()?;

    let mut nodes = fetch_current_nodes(&connection, connection_info, data_center).await?;
    if let Err(watch::error::SendError(_)) = nodes_tx.send(nodes.clone()) {
        return Ok(());
    }

    let mut keyspaces = system_keyspaces::query(&connection, data_center).await?;
    if let Err(watch::error::SendError(_)) = keyspaces_tx.send(keyspaces.clone()) {
        return Ok(());
    }

    register_for_topology_and_status_events(&connection, version).await?;

    'listen: loop {
        // Wait for events to come in from the cassandra node.
        // If all the nodes receivers are closed then immediately stop listening and shutdown the task
        let pushed_messages = tokio::select! {
            pushed_messages = pushed_messages_rx.recv() => pushed_messages,
            _ = nodes_tx.closed() => return Ok(())
        };
        match pushed_messages {
            Some(messages) => {
                for mut message in messages {
                    if let Some(Frame::Cassandra(CassandraFrame {
                        operation: CassandraOperation::Event(event),
                        ..
                    })) = message.frame()
                    {
                        match event {
                            ServerEvent::TopologyChange(topology) => match topology.change_type {
                                TopologyChangeType::NewNode => {
                                    let mut new_nodes = fetch_current_nodes(
                                        &connection,
                                        connection_info,
                                        data_center,
                                    )
                                    .await?;

                                    // is_up state gets carried over to new list
                                    for node in &nodes {
                                        if !node.is_up {
                                            for new_node in &mut new_nodes {
                                                if new_node.address == node.address {
                                                    new_node.is_up = false;
                                                }
                                            }
                                        }
                                    }

                                    nodes = new_nodes;

                                    if let Err(watch::error::SendError(_)) =
                                        nodes_tx.send(nodes.clone())
                                    {
                                        break 'listen;
                                    }
                                }
                                TopologyChangeType::RemovedNode => {
                                    nodes.retain(|node| node.address != topology.addr);

                                    if let Err(watch::error::SendError(_)) =
                                        nodes_tx.send(nodes.clone())
                                    {
                                        break 'listen;
                                    }
                                }
                            },
                            ServerEvent::StatusChange(status) => {
                                for node in &mut nodes {
                                    if node.address == status.addr {
                                        node.is_up = match status.change_type {
                                            StatusChangeType::Up => true,
                                            StatusChangeType::Down => false,
                                        }
                                    }
                                }
                                if let Err(watch::error::SendError(_)) =
                                    nodes_tx.send(nodes.clone())
                                {
                                    break 'listen;
                                }
                            }
                            ServerEvent::SchemaChange(change) => {
                                tracing::warn!("{:?}", change);
                                keyspaces =
                                    system_keyspaces::query(&connection, data_center).await?;
                                if let Err(watch::error::SendError(_)) =
                                    keyspaces_tx.send(keyspaces.clone())
                                {
                                    break 'listen;
                                }
                            }
                        }
                    }
                }
            }
            None => return Err(anyhow!("topology control connection was closed")),
        }
    }

    Ok(())
}

async fn register_for_topology_and_status_events(
    connection: &CassandraConnection,
    version: Version,
) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    connection
        .send(
            Message::from_frame(Frame::Cassandra(CassandraFrame {
                version,
                stream_id: 0,
                tracing: Tracing::Request(false),
                warnings: vec![],
                operation: CassandraOperation::Register(BodyReqRegister {
                    events: vec![
                        SimpleServerEvent::TopologyChange,
                        SimpleServerEvent::StatusChange,
                        SimpleServerEvent::SchemaChange,
                    ],
                }),
            })),
            tx,
        )
        .unwrap();

    if let Some(Frame::Cassandra(CassandraFrame { operation, .. })) = rx.await?.response?.frame() {
        match operation {
            CassandraOperation::Ready(_) => Ok(()),
            operation => Err(anyhow!("Expected Cassandra to respond to a Register with a Ready. Instead it responded with {:?}", operation))
        }
    } else {
        Err(anyhow!("Failed to parse cassandra message"))
    }
}

async fn fetch_current_nodes(
    connection: &CassandraConnection,
    connection_info: &TaskConnectionInfo,
    data_center: &str,
) -> Result<Vec<CassandraNode>> {
    let (new_nodes, more_nodes) = tokio::join!(
        system_local::query(connection, data_center, connection_info.address),
        system_peers::query(connection, data_center)
    );

    let mut new_nodes = new_nodes?;
    new_nodes.extend(more_nodes?);

    Ok(new_nodes)
}

enum ReplicationStrategy {
    SimpleStrategy {
        replication_factor: usize,
    },
    NetworkTopologyStrategy {
        datacenter_replication_factor: HashMap<String, usize>,
    },
    Other,
}

mod system_keyspaces {
    use super::*;
    use serde_json::{Map, Value as JsonValue};
    use std::str::FromStr;

    pub async fn query(
        connection: &CassandraConnection,
        data_center: &str,
    ) -> Result<HashMap<String, KeyspaceMetadata>> {
        let (tx, rx) = oneshot::channel();

        connection.send(Message::from_frame(
            Frame::Cassandra(CassandraFrame{
                version: Version::V4,
                stream_id: 0,
                tracing: Tracing::Request(false),
                warnings: vec![],
                operation: CassandraOperation::Query{
                    query: Box::new(
                        parse_statement_single(

                            "SELECT keyspace_name, toJson(replication) AS replication FROM system_schema.keyspaces",
                        )
                    ),

                    params: Box::new(QueryParams::default()),
                }
            })),
            tx
        )?;

        let response = rx.await?.response?;
        into_keyspaces(response, data_center)
    }

    fn into_keyspaces(
        mut response: Message,
        data_center: &str,
    ) -> Result<HashMap<String, KeyspaceMetadata>> {
        if let Some(Frame::Cassandra(frame)) = response.frame() {
            match &mut frame.operation {
                CassandraOperation::Result(CassandraResult::Rows { rows, .. }) => rows
                    .iter_mut()
                    .map(|row| build_keyspace(row, data_center))
                    .try_collect(),
                operation => Err(anyhow!(
                    "keyspace query returned unexpected cassandra operation: {:?}",
                    operation
                )),
            }
        } else {
            Err(anyhow!("Failed to parse keyspace query response"))
        }
    }

    fn build_keyspace(
        row: &mut Vec<MessageValue>,
        data_center: &str,
    ) -> Result<(String, KeyspaceMetadata)> {
        let metadata = if let Some(MessageValue::Varchar(string)) = row.pop() {
            let replication: JsonValue = serde_json::from_str(&string).map_err(|error| {
                anyhow!(format!(
                    "Error parsing replication. Error: {} Replication: {}",
                    error, string
                ))
            })?;

            let replication_strategy = match replication {
                JsonValue::Object(properties) => build_replication_strategy(properties)?,
                _ => {
                    return Err(anyhow!(format!(
                        "Error parsing replication strategy: {}",
                        replication
                    )))
                }
            };

            let replication_factor = match replication_strategy {
                ReplicationStrategy::SimpleStrategy { replication_factor } => replication_factor,
                ReplicationStrategy::NetworkTopologyStrategy {
                    datacenter_replication_factor,
                } => *datacenter_replication_factor.get(data_center).unwrap_or(&0),
                _ => 0,
            };

            KeyspaceMetadata { replication_factor }
        } else {
            return Err(anyhow!(
                "system_schema.keyspaces.replication is not a varchar"
            ));
        };

        let name = if let Some(MessageValue::Varchar(name)) = row.pop() {
            name
        } else {
            return Err(anyhow!("system_schema_keyspaces.name"));
        };

        Ok((name, metadata))
    }

    fn build_replication_strategy(
        mut properties: Map<String, JsonValue>,
    ) -> Result<ReplicationStrategy> {
        match properties.remove("class") {
            Some(JsonValue::String(class)) => Ok(match class.as_str() {
                "org.apache.cassandra.locator.SimpleStrategy" | "SimpleStrategy" => {
                    ReplicationStrategy::SimpleStrategy {
                        replication_factor: extract_replication_factor(
                            properties.get("replication_factor"),
                        )?,
                    }
                }
                "org.apache.cassandra.locator.NetworkTopologyStrategy"
                | "NetworkTopologyStrategy" => ReplicationStrategy::NetworkTopologyStrategy {
                    datacenter_replication_factor: extract_datacenter_replication_factor(
                        properties,
                    )?,
                },
                _ => ReplicationStrategy::Other,
            }),
            _ => Err(anyhow!("Missing replication strategy class")),
        }
    }

    fn extract_datacenter_replication_factor(
        properties: Map<String, JsonValue>,
    ) -> Result<HashMap<String, usize>> {
        properties
            .into_iter()
            .map(|(key, replication_factor)| {
                extract_replication_factor(Some(&replication_factor))
                    .map(move |replication_factor| (key, replication_factor))
            })
            .try_collect()
    }

    fn extract_replication_factor(value: Option<&JsonValue>) -> Result<usize> {
        match value {
            Some(JsonValue::String(replication_factor)) => {
                let result = if let Some(slash) = replication_factor.find('/') {
                    usize::from_str(&replication_factor[..slash])
                } else {
                    usize::from_str(replication_factor)
                };

                result.map_err(|error| {
                    anyhow!(format!(
                        "Failed to parse ('{}'): {}",
                        replication_factor, error
                    ))
                })
            }
            _ => Err(anyhow!("Missing replication factor")),
        }
    }
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
                tracing: Tracing::Request(false),
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

                        Ok(CassandraNode::new(address, rack, tokens, host_id))
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
                tracing: Tracing::Request(false),
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
                    tracing: Tracing::Request(false),
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

                        Ok(CassandraNode::new(
                            SocketAddr::new(ip, port.try_into()?),
                            rack,
                            tokens,
                            host_id,
                        ))
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
