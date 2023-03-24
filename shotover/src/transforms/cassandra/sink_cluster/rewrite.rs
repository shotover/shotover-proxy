use super::ShotoverNode;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::Message;
use crate::message_value::{IntSize, MessageValue};
use anyhow::{anyhow, Result};
use cassandra_protocol::frame::message_result::BodyResResultPrepared;
use cql3_parser::common::{Identifier, IdentifierRef};
use cql3_parser::select::SelectElement;
use std::net::{IpAddr, Ipv4Addr};
use uuid::Uuid;
use version_compare::Cmp;

#[derive(Clone)]
pub struct MessageRewriter {
    pub shotover_peers: Vec<ShotoverNode>,
    pub local_shotover_node: ShotoverNode,
}

impl MessageRewriter {
    pub async fn rewrite_table(
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

                            let mut nodes = match parse_system_nodes(peers_response) {
                                Ok(x) => x,
                                Err(MessageParseError::CassandraError(err)) => {
                                    *client_peers_response = client_peers_response
                                        .to_error_response(format!("{:?}", err.context("Shotover failed to generate system.peers, an error occured when an internal system.peers query was run.")));
                                    return Ok(());
                                }
                                Err(MessageParseError::ParseFailure(err)) => return Err(err),
                            };
                            match parse_system_nodes(local_response) {
                                Ok(x) => nodes.extend(x),
                                Err(MessageParseError::CassandraError(err)) => {
                                    *client_peers_response = client_peers_response
                                        .to_error_response(format!("{:?}", err.context("Shotover failed to generate system.peers, an error occured when an internal system.local query was run.")));
                                    return Ok(());
                                }
                                Err(MessageParseError::ParseFailure(err)) => return Err(err),
                            };

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
        let mut peers = match parse_system_nodes(peers_response) {
            Ok(x) => x,
            Err(MessageParseError::CassandraError(err)) => {
                *local_response = local_response.to_error_response(format!("{:?}", err.context("Shotover failed to generate system.local, an error occured when an internal system.peers query was run.")));
                return Ok(());
            }
            Err(MessageParseError::ParseFailure(err)) => return Err(err),
        };
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
}

pub struct TableToRewrite {
    pub incoming_index: usize,
    pub outgoing_index: usize,
    pub ty: RewriteTableTy,
    pub selects: Vec<SelectElement>,
    pub warnings: Vec<String>,
}

#[derive(PartialEq, Clone)]
pub enum RewriteTableTy {
    Local,
    Peers,
    // We need to know which nodes we are sending to early on so that we can create the appropriate number of messages early and keep our rewrite indexes intact
    Prepare { destination_nodes: Vec<Uuid> },
}

impl RewriteTableTy {
    pub fn extra_messages_needed(&self) -> usize {
        match self {
            RewriteTableTy::Local => 1,
            RewriteTableTy::Peers => 2,
            RewriteTableTy::Prepare { destination_nodes } => {
                destination_nodes.len().saturating_sub(1)
            }
        }
    }
}

struct NodeInfo {
    tokens: Vec<MessageValue>,
    schema_version: Uuid,
    release_version: String,
    rack: String,
    data_center: String,
}

enum MessageParseError {
    /// The db returned a message that shotover failed to parse or process, this indicates a bug in shotover or the db
    ParseFailure(anyhow::Error),
    /// The db returned an error message, this indicates a user error e.g. the user does not have sufficient permissions
    CassandraError(anyhow::Error),
}

fn parse_system_nodes(mut response: Message) -> Result<Vec<NodeInfo>, MessageParseError> {
    if let Some(Frame::Cassandra(frame)) = response.frame() {
        match &mut frame.operation {
            CassandraOperation::Result(CassandraResult::Rows { rows, .. }) => rows
                .iter_mut()
                .map(|row| {
                    if row.len() != 5 {
                        return Err(MessageParseError::ParseFailure(anyhow!(
                            "expected 5 columns but was {}",
                            row.len()
                        )));
                    }

                    let release_version = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(MessageParseError::ParseFailure(anyhow!(
                            "release_version not a varchar"
                        )));
                    };

                    let tokens = if let Some(MessageValue::List(value)) = row.pop() {
                        value
                    } else {
                        return Err(MessageParseError::ParseFailure(anyhow!(
                            "tokens not a list"
                        )));
                    };

                    let schema_version = if let Some(MessageValue::Uuid(value)) = row.pop() {
                        value
                    } else {
                        return Err(MessageParseError::ParseFailure(anyhow!(
                            "schema_version not a uuid"
                        )));
                    };

                    let data_center = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(MessageParseError::ParseFailure(anyhow!(
                            "data_center not a varchar"
                        )));
                    };
                    let rack = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(MessageParseError::ParseFailure(anyhow!(
                            "rack not a varchar"
                        )));
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
            CassandraOperation::Error(error) => Err(MessageParseError::CassandraError(anyhow!(
                "system.local returned error: {error:?}",
            ))),
            operation => Err(MessageParseError::ParseFailure(anyhow!(
                "system.local returned unexpected cassandra operation: {operation:?}",
            ))),
        }
    } else {
        Err(MessageParseError::ParseFailure(anyhow!(
            "Failed to parse system.local response {:?}",
            response
        )))
    }
}
