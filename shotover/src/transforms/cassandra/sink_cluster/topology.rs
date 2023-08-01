use super::node::{CassandraNode, ConnectionFactory};
use super::node_pool::KeyspaceMetadata;
use super::KeyspaceChanTx;
use crate::frame::{
    cassandra::{parse_statement_single, Tracing},
    value::GenericValue,
    CassandraFrame, CassandraOperation, CassandraResult, Frame,
};
use crate::message::Message;
use crate::transforms::cassandra::connection::CassandraConnection;
use anyhow::{anyhow, Result};
use cassandra_protocol::events::{ServerEvent, SimpleServerEvent};
use cassandra_protocol::frame::events::{StatusChangeType, TopologyChangeType};
use cassandra_protocol::frame::message_register::BodyReqRegister;
use cassandra_protocol::frame::Version;
use cassandra_protocol::token::Murmur3Token;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{mpsc, watch};

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

    let mut nodes = fetch_current_nodes(&connection, connection_info, data_center, version).await?;
    if let Err(watch::error::SendError(_)) = nodes_tx.send(nodes.clone()) {
        return Ok(());
    }

    let mut keyspaces = system_keyspaces::query(&connection, data_center, version).await?;
    if let Err(watch::error::SendError(_)) = keyspaces_tx.send(keyspaces.clone()) {
        return Ok(());
    }

    register_for_topology_and_status_events(&connection, version).await?;

    tracing::info!(
        "Topology task control connection finalized against node at: {:?}",
        connection_info.address
    );

    loop {
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
                                        version,
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
                                        return Ok(());
                                    }
                                }
                                TopologyChangeType::RemovedNode => {
                                    nodes.retain(|node| node.address != topology.addr);

                                    if let Err(watch::error::SendError(_)) =
                                        nodes_tx.send(nodes.clone())
                                    {
                                        return Ok(());
                                    }
                                }
                                _ => unreachable!(),
                            },
                            ServerEvent::StatusChange(status) => {
                                for node in &mut nodes {
                                    if node.address == status.addr {
                                        node.is_up = match status.change_type {
                                            StatusChangeType::Up => true,
                                            StatusChangeType::Down => false,
                                            _ => unreachable!(),
                                        }
                                    }
                                }
                                if let Err(watch::error::SendError(_)) =
                                    nodes_tx.send(nodes.clone())
                                {
                                    return Ok(());
                                }
                            }
                            ServerEvent::SchemaChange(_change) => {
                                keyspaces =
                                    system_keyspaces::query(&connection, data_center, version)
                                        .await?;
                                if let Err(watch::error::SendError(_)) =
                                    keyspaces_tx.send(keyspaces.clone())
                                {
                                    return Ok(());
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            }
            None => return Err(anyhow!("topology control connection was closed")),
        }
    }
}

async fn register_for_topology_and_status_events(
    connection: &CassandraConnection,
    version: Version,
) -> Result<()> {
    let mut response = connection
        .send(Message::from_frame(Frame::Cassandra(CassandraFrame {
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
        })))
        .unwrap()
        .await??;

    if let Some(Frame::Cassandra(CassandraFrame { operation, .. })) = response.frame() {
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
    version: Version,
) -> Result<Vec<CassandraNode>> {
    let (new_nodes, more_nodes) = tokio::join!(
        system_local::query(connection, data_center, connection_info.address, version),
        system_peers::query(connection, data_center, version)
    );

    let mut new_nodes = new_nodes?;
    new_nodes.extend(more_nodes?);

    Ok(new_nodes)
}

mod system_keyspaces {
    use super::*;
    use std::str::FromStr;

    pub async fn query(
        connection: &CassandraConnection,
        data_center: &str,
        version: Version,
    ) -> Result<HashMap<String, KeyspaceMetadata>> {
        let response = connection
            .send(Message::from_frame(Frame::Cassandra(CassandraFrame {
                version,
                stream_id: 0,
                tracing: Tracing::Request(false),
                warnings: vec![],
                operation: CassandraOperation::Query {
                    query: Box::new(parse_statement_single(
                        "SELECT keyspace_name, replication FROM system_schema.keyspaces",
                    )),

                    params: Box::default(),
                },
            })))?
            .await??;
        into_keyspaces(response, data_center)
    }

    fn into_keyspaces(
        mut response: Message,
        data_center: &str,
    ) -> Result<HashMap<String, KeyspaceMetadata>> {
        if let Some(Frame::Cassandra(frame)) = response.frame() {
            match &mut frame.operation {
                CassandraOperation::Result(CassandraResult::Rows { rows, .. }) => rows
                    .drain(..)
                    .map(|row| build_keyspace(row, data_center))
                    .collect(),
                operation => Err(anyhow!(
                    "keyspace query returned unexpected cassandra operation: {:?}",
                    operation
                )),
            }
        } else {
            Err(anyhow!("Failed to parse keyspace query response"))
        }
    }

    pub fn build_keyspace(
        mut row: Vec<GenericValue>,
        data_center: &str,
    ) -> Result<(String, KeyspaceMetadata)> {
        let metadata = if let Some(GenericValue::Map(mut replication_strategy)) = row.pop() {
            let strategy_name: String = match replication_strategy
                .remove(&GenericValue::Varchar("class".into()))
                .ok_or_else(|| anyhow!("replication strategy map should have a 'class' field",))?
            {
                GenericValue::Varchar(name) => name,
                _ => return Err(anyhow!("'class' field should be a varchar")),
            };

            match strategy_name.as_str() {
                "org.apache.cassandra.locator.SimpleStrategy" | "SimpleStrategy" => {
                    let rf_str: String =
                        match replication_strategy.remove(&GenericValue::Varchar("replication_factor".into())).ok_or_else(||
                         anyhow!("SimpleStrategy in replication strategy map does not have a replication factor")
                        )?{
                            GenericValue::Varchar(rf) => rf,
                            _ => return Err(anyhow!("SimpleStrategy replication factor should be a varchar "))
                        };

                    let replication_factor: usize = usize::from_str(&rf_str).map_err(|_| {
                        anyhow!("Could not parse replication factor as an integer",)
                    })?;

                    KeyspaceMetadata { replication_factor }
                }
                "org.apache.cassandra.locator.NetworkTopologyStrategy"
                | "NetworkTopologyStrategy" => {
                    let data_center_rf = match replication_strategy
                        .remove(&GenericValue::Varchar(data_center.into()))
                    {
                        Some(GenericValue::Varchar(rf_str)) => {
                            usize::from_str(&rf_str).map_err(|_| {
                                anyhow!("Could not parse replication factor as an integer",)
                            })?
                        }
                        Some(_other) => {
                            return Err(anyhow!(
                                "NetworkTopologyStrategy replication factor should be a varchar"
                            ))
                        }
                        None => 0,
                    };

                    KeyspaceMetadata {
                        replication_factor: data_center_rf,
                    }
                }
                "org.apache.cassandra.locator.LocalStrategy" | "LocalStrategy" => {
                    KeyspaceMetadata {
                        replication_factor: 1,
                    }
                }
                _ => {
                    tracing::warn!("Unrecognised replication strategy: {strategy_name:?}");
                    KeyspaceMetadata {
                        replication_factor: 1,
                    }
                }
            }
        } else {
            return Err(anyhow!("replication strategy should be a map"));
        };

        let name = if let Some(GenericValue::Varchar(name)) = row.pop() {
            name
        } else {
            return Err(anyhow!("system_schema_keyspaces.name should be a varchar"));
        };

        Ok((name, metadata))
    }
}

mod system_local {
    use super::*;

    pub async fn query(
        connection: &CassandraConnection,
        data_center: &str,
        address: SocketAddr,
        version: Version,
    ) -> Result<Vec<CassandraNode>> {
        let response = connection
            .send(Message::from_frame(Frame::Cassandra(CassandraFrame {
                version,
                stream_id: 1,
                tracing: Tracing::Request(false),
                warnings: vec![],
                operation: CassandraOperation::Query {
                    query: Box::new(parse_statement_single(
                        "SELECT rack, tokens, host_id, data_center FROM system.local",
                    )),
                    params: Box::default(),
                },
            })))?
            .await??;

        into_nodes(response, data_center, address)
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
                        if let Some(GenericValue::Varchar(data_center)) = row.last() {
                            data_center == config_data_center
                        } else {
                            false
                        }
                    })
                    .map(|row| {
                        let _data_center = row.pop();

                        let host_id = if let Some(GenericValue::Uuid(host_id)) = row.pop() {
                            host_id
                        } else {
                            return Err(anyhow!("system.local.host_id not a uuid"));
                        };

                        let tokens = if let Some(GenericValue::List(mut list)) = row.pop() {
                            list.drain(..)
                                .map::<Result<Murmur3Token>, _>(|x| match x {
                                    GenericValue::Varchar(a) => Ok(a.try_into()?),
                                    _ => Err(anyhow!("system.local.tokens value not a varchar")),
                                })
                                .collect::<Result<Vec<Murmur3Token>>>()?
                        } else {
                            return Err(anyhow!("system.local.tokens not a list"));
                        };

                        let rack = if let Some(GenericValue::Varchar(value)) = row.pop() {
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
        version: Version,
    ) -> Result<Vec<CassandraNode>> {
        let mut response = connection.send(
            Message::from_frame(Frame::Cassandra(CassandraFrame {
                version,
                stream_id: 0,
                tracing: Tracing::Request(false),
                warnings: vec![],
                operation: CassandraOperation::Query {
                    query: Box::new(parse_statement_single(
                        "SELECT native_port, native_address, rack, tokens, host_id, data_center FROM system.peers_v2",
                    )),
                params: Box::default(),
                },
            })),
        )?.await??;

        if is_peers_v2_does_not_exist_error(&mut response) {
            response = connection
                .send(Message::from_frame(Frame::Cassandra(CassandraFrame {
                    version,
                    stream_id: 0,
                    tracing: Tracing::Request(false),
                    warnings: vec![],
                    operation: CassandraOperation::Query {
                        query: Box::new(parse_statement_single(
                            "SELECT peer, rack, tokens, host_id, data_center FROM system.peers",
                        )),
                        params: Box::default(),
                    },
                })))?
                .await??;
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
                        if let Some(GenericValue::Varchar(data_center)) = row.last() {
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

                        let host_id = if let Some(GenericValue::Uuid(host_id)) = row.pop() {
                            host_id
                        } else {
                            return Err(anyhow!("system.peers(v2).host_id not a uuid"));
                        };

                        let tokens = if let Some(GenericValue::List(list)) = row.pop() {
                            list.into_iter()
                                .map::<Result<Murmur3Token>, _>(|x| match x {
                                    GenericValue::Varchar(a) => Ok(a.try_into()?),
                                    _ => {
                                        Err(anyhow!("system.peers(v2).tokens value not a varchar"))
                                    }
                                })
                                .collect::<Result<Vec<Murmur3Token>>>()?
                        } else {
                            return Err(anyhow!("system.peers(v2).tokens not a list"));
                        };

                        let rack = if let Some(GenericValue::Varchar(value)) = row.pop() {
                            value
                        } else {
                            return Err(anyhow!("system.peers(v2).rack not a varchar"));
                        };

                        let ip = if let Some(GenericValue::Inet(value)) = row.pop() {
                            value
                        } else {
                            return Err(anyhow!("system.peers(v2).native_address not an inet"));
                        };

                        let port = if let Some(message_value) = row.pop() {
                            if let GenericValue::Integer(value, _) = message_value {
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

#[cfg(test)]
mod test_system_keyspaces {
    use super::*;

    #[test]
    fn test_simple() {
        let row = vec![
            GenericValue::Varchar("test".into()),
            GenericValue::Map(
                vec![
                    (
                        GenericValue::Varchar("class".into()),
                        GenericValue::Varchar("org.apache.cassandra.locator.SimpleStrategy".into()),
                    ),
                    (
                        GenericValue::Varchar("replication_factor".into()),
                        GenericValue::Varchar("2".into()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        ];

        let result = system_keyspaces::build_keyspace(row, "dc1").unwrap();
        assert_eq!(
            result,
            (
                "test".into(),
                KeyspaceMetadata {
                    replication_factor: 2
                }
            )
        )
    }

    #[test]
    fn test_network() {
        let row = vec![
            GenericValue::Varchar("test".into()),
            GenericValue::Map(
                vec![
                    (
                        GenericValue::Varchar("class".into()),
                        GenericValue::Varchar(
                            "org.apache.cassandra.locator.NetworkTopologyStrategy".into(),
                        ),
                    ),
                    (
                        GenericValue::Varchar("dc1".into()),
                        GenericValue::Varchar("3".into()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        ];

        let result = system_keyspaces::build_keyspace(row, "dc1").unwrap();

        assert_eq!(
            result,
            (
                "test".into(),
                KeyspaceMetadata {
                    replication_factor: 3
                }
            )
        )
    }
}
