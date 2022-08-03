use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{IntSize, Message, MessageValue};
use crate::transforms::cassandra::peers_rewrite::CassandraOperation::Event;
use crate::{
    error::ChainResponse,
    transforms::{Transform, Transforms, Wrapper},
};
use anyhow::Result;
use async_trait::async_trait;
use cassandra_protocol::frame::events::{ServerEvent, StatusChange};
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{FQName, Identifier};
use cql3_parser::select::SelectElement;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraPeersRewriteConfig {
    pub port: u16,
    pub assign_token_range: Option<bool>,
}

impl CassandraPeersRewriteConfig {
    pub async fn get_transform(&self) -> Result<Transforms> {
        Ok(Transforms::CassandraPeersRewrite(
            CassandraPeersRewrite::new(self.port, self.assign_token_range.unwrap_or(false)),
        ))
    }
}

#[derive(Clone)]
pub struct CassandraPeersRewrite {
    port: u16,
    peers_table: FQName,
    peers_table_v2: FQName,
    assign_token_range: bool,
}

impl CassandraPeersRewrite {
    pub fn new(port: u16, assign_token_range: bool) -> Self {
        CassandraPeersRewrite {
            port,
            peers_table: FQName::new("system", "peers"),
            peers_table_v2: FQName::new("system", "peers_v2"),
            assign_token_range,
        }
    }
}

#[async_trait]
impl Transform for CassandraPeersRewrite {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        Ok(if self.assign_token_range {
            // Find the indices of queries to system.peers or system.peers_v2 in order
            // to know what messages we need to clear the rows from.
            let peers_queries: Vec<usize> = message_wrapper
                .messages
                .iter_mut()
                .enumerate()
                .filter_map(|(i, message)| {
                    if is_selecting_peers_table(message, &self.peers_table, &self.peers_table_v2) {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect();

            let mut response = message_wrapper.call_next_transform().await?;

            for i in peers_queries {
                clear_rows(&mut response[i]);
            }

            response
        } else {
            // Find the indices of queries to system.peers_v2
            // we need to know which columns in which CQL queries in which messages have system peers
            let column_names: Vec<(usize, Vec<Identifier>)> = message_wrapper
                .messages
                .iter_mut()
                .enumerate()
                .filter_map(|(i, m)| {
                    let sys_peers = extract_native_port_column(&self.peers_table_v2, m);
                    if sys_peers.is_empty() {
                        None
                    } else {
                        Some((i, sys_peers))
                    }
                })
                .collect();

            let mut response = message_wrapper.call_next_transform().await?;

            for (i, name_list) in column_names {
                rewrite_port(&mut response[i], &name_list, self.port);
            }

            response
        })
    }

    async fn transform_pushed<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        Ok(if self.assign_token_range {
            message_wrapper.messages.retain_mut(|message| {
                if let Some(Frame::Cassandra(CassandraFrame {
                    operation: Event(event),
                    ..
                })) = message.frame()
                {
                    // exhaustive match to protect against new events being added
                    //
                    // If we are giving Shotover the entire token range the drivers
                    // should not receive any topology information
                    match event {
                        ServerEvent::SchemaChange(_) => true,
                        ServerEvent::TopologyChange(_) | ServerEvent::StatusChange(_) => false,
                    }
                } else {
                    true
                }
            });

            message_wrapper.call_next_transform_pushed().await?
        } else {
            for message in &mut message_wrapper.messages {
                if let Some(Frame::Cassandra(CassandraFrame {
                    operation: Event(ServerEvent::StatusChange(StatusChange { addr, .. })),
                    ..
                })) = message.frame()
                {
                    addr.addr.set_port(self.port);
                    message.invalidate_cache();
                }
            }

            message_wrapper.call_next_transform_pushed().await?
        })
    }
}

/// Determine if the message contains a SELECT from `system.peers_v2` that includes the `native_port` column
/// return a list of column names (or their alias) for each `native_port`.
fn extract_native_port_column(peer_table: &FQName, message: &mut Message) -> Vec<Identifier> {
    let mut result = vec![];
    let native_port = Identifier::parse("native_port");

    if let Some(Frame::Cassandra(CassandraFrame {
        operation: CassandraOperation::Query { query, .. },
        ..
    })) = message.frame()
    {
        // No need to handle Batch as selects can only occur on Query
        if let CassandraStatement::Select(select) = query.as_ref() {
            if peer_table == &select.table_name {
                for select_element in &select.columns {
                    match select_element {
                        SelectElement::Column(col_name) if col_name.name == native_port => {
                            result.push(col_name.alias_or_name().clone());
                        }
                        SelectElement::Star => result.push(native_port.clone()),
                        _ => {}
                    }
                }
            }
        }
    }
    result
}

/// Rewrite the `native_port` field in the results from a query to `system.peers_v2` table
/// Only Cassandra queries to the `system.peers` table found via the `is_system_peers` function should be passed to this
fn rewrite_port(message: &mut Message, column_names: &[Identifier], new_port: u16) {
    // CassandraOperation::Error(_) is another possible case, we should silently ignore such cases
    if let Some(Frame::Cassandra(CassandraFrame {
        operation:
            CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                metadata,
            }),
        ..
    })) = message.frame()
    {
        for (i, col) in metadata.col_specs.iter().enumerate() {
            if column_names.contains(&Identifier::parse(&col.name)) {
                for row in rows.iter_mut() {
                    row[i] = MessageValue::Integer(new_port as i64, IntSize::I32);
                }
            }
        }
        message.invalidate_cache();
    }
}

// Determine if the message contains a SELECT from the `system.peers` or `system.peers_v2` tables.
// Similar to the other function but doesn't need to check columns, only the table name.
fn is_selecting_peers_table(
    message: &mut Message,
    peers_table: &FQName,
    peers_table_v2: &FQName,
) -> bool {
    if let Some(Frame::Cassandra(CassandraFrame {
        operation: CassandraOperation::Query { query, .. },
        ..
    })) = message.frame()
    {
        if let CassandraStatement::Select(select) = query.as_ref() {
            if peers_table == &select.table_name || peers_table_v2 == &select.table_name {
                return true;
            }
        }
    }

    false
}

// Clear the rows from a CassandraResult response
fn clear_rows(message: &mut Message) {
    if let Some(Frame::Cassandra(CassandraFrame {
        operation:
            CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                ..
            }),
        ..
    })) = message.frame()
    {
        rows.clear();
    }

    message.invalidate_cache();
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frame::cassandra::parse_statement_single;
    use crate::frame::CassandraFrame;
    use crate::transforms::cassandra::peers_rewrite::CassandraResult::Rows;
    use cassandra_protocol::consistency::Consistency;
    use cassandra_protocol::frame::message_result::{
        ColSpec, ColType, ColTypeOption, RowsMetadata, RowsMetadataFlags, TableSpec,
    };
    use cassandra_protocol::frame::Version;
    use cassandra_protocol::query::QueryParams;

    fn create_query_message(query: &str) -> Message {
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single(query)),
                params: Box::new(QueryParams {
                    keyspace: None,
                    now_in_seconds: None,
                    consistency: Consistency::One,
                    with_names: false,
                    values: None,
                    page_size: Some(5000),
                    paging_state: None,
                    serial_consistency: None,
                    timestamp: Some(1643855761086585),
                }),
            },
        }))
    }

    fn create_response_message(col_specs: &[ColSpec], rows: Vec<Vec<MessageValue>>) -> Message {
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Result(Rows {
                value: MessageValue::Rows(rows),
                metadata: Box::new(RowsMetadata {
                    flags: RowsMetadataFlags::GLOBAL_TABLE_SPACE,
                    columns_count: 1,
                    paging_state: None,
                    new_metadata_id: None,
                    global_table_spec: Some(TableSpec {
                        ks_name: "system".into(),
                        table_name: "peers_v2".into(),
                    }),
                    col_specs: col_specs.to_owned(),
                }),
            }),
        }))
    }

    #[test]
    fn test_extract_native_port_column() {
        let native_port = Identifier::parse("native_port");
        let foo = Identifier::parse("foo");
        let peer_table = FQName::new("system", "peers_v2");

        assert_eq!(
            vec![native_port.clone()],
            extract_native_port_column(
                &peer_table,
                &mut create_query_message("SELECT * FROM system.peers_v2;"),
            )
        );

        assert_eq!(
            Vec::<Identifier>::new(),
            extract_native_port_column(
                &peer_table,
                &mut create_query_message("SELECT * FROM not_system.peers_v2;"),
            )
        );

        assert_eq!(
            vec![foo.clone()],
            extract_native_port_column(
                &peer_table,
                &mut create_query_message("SELECT native_port as foo from system.peers_v2"),
            )
        );

        assert_eq!(
            vec![foo, native_port],
            extract_native_port_column(
                &peer_table,
                &mut create_query_message(
                    "SELECT native_port as foo, native_port from system.peers_v2",
                ),
            )
        );
    }

    #[test]
    fn test_rewrite_port_match() {
        let col_spec = &[ColSpec {
            table_spec: None,
            name: "native_port".into(),
            col_type: ColTypeOption {
                id: ColType::Int,
                value: None,
            },
        }];

        let mut message = create_response_message(
            col_spec,
            vec![
                vec![MessageValue::Integer(9042, IntSize::I32)],
                vec![MessageValue::Integer(9042, IntSize::I32)],
            ],
        );

        let expected = create_response_message(
            col_spec,
            vec![
                vec![MessageValue::Integer(9043, IntSize::I32)],
                vec![MessageValue::Integer(9043, IntSize::I32)],
            ],
        );

        rewrite_port(&mut message, &[Identifier::parse("native_port")], 9043);

        assert_eq!(message, expected);
    }

    #[test]
    fn test_rewrite_port_no_match() {
        let col_spec = &[ColSpec {
            table_spec: None,
            name: "peer".into(),
            col_type: ColTypeOption {
                id: ColType::Inet,
                value: None,
            },
        }];

        let mut original = create_response_message(
            col_spec,
            vec![
                vec![MessageValue::Inet("127.0.0.1".parse().unwrap())],
                vec![MessageValue::Inet("10.123.56.1".parse().unwrap())],
            ],
        );

        let expected = create_response_message(
            col_spec,
            vec![
                vec![MessageValue::Inet("127.0.0.1".parse().unwrap())],
                vec![MessageValue::Inet("10.123.56.1".parse().unwrap())],
            ],
        );

        rewrite_port(
            &mut original,
            &[
                Identifier::parse("native_port"),
                Identifier::parse("alias_port"),
            ],
            9043,
        );

        assert_eq!(original, expected);
    }

    #[test]
    fn test_rewrite_port_alias() {
        let col_spec = &[
            ColSpec {
                table_spec: None,
                name: "native_port".into(),
                col_type: ColTypeOption {
                    id: ColType::Int,
                    value: None,
                },
            },
            ColSpec {
                table_spec: None,
                name: "some_text".into(),
                col_type: ColTypeOption {
                    id: ColType::Varchar,
                    value: None,
                },
            },
            ColSpec {
                table_spec: None,
                name: "alias_port".into(),
                col_type: ColTypeOption {
                    id: ColType::Int,
                    value: None,
                },
            },
        ];

        let mut original = create_response_message(
            col_spec,
            vec![
                vec![
                    MessageValue::Integer(9042, IntSize::I32),
                    MessageValue::Strings("Hello".into()),
                    MessageValue::Integer(9042, IntSize::I32),
                ],
                vec![
                    MessageValue::Integer(9042, IntSize::I32),
                    MessageValue::Strings("World".into()),
                    MessageValue::Integer(9042, IntSize::I32),
                ],
            ],
        );

        let expected = create_response_message(
            col_spec,
            vec![
                vec![
                    MessageValue::Integer(9043, IntSize::I32),
                    MessageValue::Strings("Hello".into()),
                    MessageValue::Integer(9043, IntSize::I32),
                ],
                vec![
                    MessageValue::Integer(9043, IntSize::I32),
                    MessageValue::Strings("World".into()),
                    MessageValue::Integer(9043, IntSize::I32),
                ],
            ],
        );

        rewrite_port(
            &mut original,
            &[
                Identifier::parse("native_port"),
                Identifier::parse("alias_port"),
            ],
            9043,
        );

        assert_eq!(original, expected);
    }

    #[test]
    fn test_is_selecting_peers_table() {
        let peers = &FQName::new("system", "peers");
        let peers_v2 = &FQName::new("system", "peers_v2");

        let selecting = [
            create_query_message("SELECT * FROM system.peers"),
            create_query_message("SELECT * FROM system.peers_v2"),
            create_query_message("SELECT native_port from system.peers"),
            create_query_message("SELECT native_port from system.peers_v2"),
        ];

        let not_selecting = [
            create_query_message("SELECT * FROM system.not_peers"),
            create_query_message("SELECT * FROM system.not_peers_v2"),
            create_query_message("SELECT * FROM not_system.peers"),
            create_query_message("SEELCT * FROM not_system.peers_v2"),
            create_response_message(
                &[ColSpec {
                    table_spec: None,
                    name: "native_port".into(),
                    col_type: ColTypeOption {
                        id: ColType::Int,
                        value: None,
                    },
                }],
                vec![
                    vec![MessageValue::Integer(9042, IntSize::I32)],
                    vec![MessageValue::Integer(9042, IntSize::I32)],
                ],
            ),
        ];

        for mut msg in selecting {
            assert!(
                is_selecting_peers_table(&mut msg, peers, peers_v2),
                "{msg:?}"
            );
        }

        for mut msg in not_selecting {
            assert!(!is_selecting_peers_table(&mut msg, peers, peers_v2));
        }
    }

    #[test]
    fn test_clear_rows() {
        let col_spec = &[ColSpec {
            table_spec: None,
            name: "native_port".into(),
            col_type: ColTypeOption {
                id: ColType::Int,
                value: None,
            },
        }];

        {
            let mut original = create_response_message(
                col_spec,
                vec![
                    vec![MessageValue::Integer(9042, IntSize::I32)],
                    vec![MessageValue::Integer(9042, IntSize::I32)],
                ],
            );
            let expected = create_response_message(col_spec, vec![]);
            clear_rows(&mut original);
            assert_eq!(original, expected);
        }

        {
            let mut original = create_response_message(col_spec, vec![]);
            let expected = create_response_message(col_spec, vec![]);
            clear_rows(&mut original);
            assert_eq!(original, expected);
        }

        {
            let mut original = create_query_message("SELECT * FROM system.peers");
            let expected = original.clone();
            clear_rows(&mut original);
            assert_eq!(original, expected);
        }
    }
}
