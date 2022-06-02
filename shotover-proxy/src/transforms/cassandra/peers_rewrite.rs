use crate::frame::{CassandraOperation, CassandraResult, Frame};
use crate::message::{IntSize, Message, MessageValue};
use crate::{
    error::ChainResponse,
    transforms::{Transform, Transforms, Wrapper},
};
use anyhow::Result;
use async_trait::async_trait;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{FQName, Identifier};
use cql3_parser::select::SelectElement;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraPeersRewriteConfig {
    pub port: u16,
}

impl CassandraPeersRewriteConfig {
    pub async fn get_transform(&self) -> Result<Transforms> {
        Ok(Transforms::CassandraPeersRewrite(
            CassandraPeersRewrite::new(self.port),
        ))
    }
}

#[derive(Clone)]
pub struct CassandraPeersRewrite {
    port: u16,
    peer_table: FQName,
}

impl CassandraPeersRewrite {
    pub fn new(port: u16) -> Self {
        CassandraPeersRewrite {
            port,
            peer_table: FQName::new("system", "peers_v2"),
        }
    }
}

#[async_trait]
impl Transform for CassandraPeersRewrite {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        // Find the indices of queries to system.peers & system.peers_v2
        // we need to know which columns in which CQL queries in which messages have system peers
        let column_names: Vec<(usize, Vec<Identifier>)> = message_wrapper
            .messages
            .iter_mut()
            .enumerate()
            .filter_map(|(i, m)| {
                let sys_peers = extract_native_port_column(&self.peer_table, m);
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

        Ok(response)
    }

    async fn transform_rev<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        tracing::info!("{:?}", message_wrapper);
        let response = message_wrapper.call_next_transform_rev().await?;
        Ok(response)
    }
}

/// determine if the message contains a SELECT from `system.peers_v2` that includes the `native_port` column
/// return a list of column names (or their alias) for each `native_port`.
fn extract_native_port_column(peer_table: &FQName, message: &mut Message) -> Vec<Identifier> {
    let mut result = vec![];
    let native_port = Identifier::parse("native_port");
    if let Some(Frame::Cassandra(cassandra)) = message.frame() {
        // No need to handle Batch as selects can only occur on Query
        if let CassandraOperation::Query { query, .. } = &cassandra.operation {
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
    }
    result
}

/// Rewrite the `native_port` field in the results from a query to `system.peers_v2` table
/// Only Cassandra queries to the `system.peers` table found via the `is_system_peers` function should be passed to this
fn rewrite_port(message: &mut Message, column_names: &[Identifier], new_port: u16) {
    if let Some(Frame::Cassandra(frame)) = message.frame() {
        // CassandraOperation::Error(_) is another possible case, we should silently ignore such cases
        if let CassandraOperation::Result(CassandraResult::Rows {
            value: MessageValue::Rows(rows),
            metadata,
        }) = &mut frame.operation
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
        let col_spec = vec![ColSpec {
            table_spec: None,
            name: "native_port".into(),
            col_type: ColTypeOption {
                id: ColType::Int,
                value: None,
            },
        }];

        let mut message = create_response_message(
            &col_spec,
            vec![
                vec![MessageValue::Integer(9042, IntSize::I32)],
                vec![MessageValue::Integer(9042, IntSize::I32)],
            ],
        );

        let expected = create_response_message(
            &col_spec,
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
        let col_spec = vec![ColSpec {
            table_spec: None,
            name: "peer".into(),
            col_type: ColTypeOption {
                id: ColType::Inet,
                value: None,
            },
        }];

        let mut original = create_response_message(
            &col_spec,
            vec![
                vec![MessageValue::Inet("127.0.0.1".parse().unwrap())],
                vec![MessageValue::Inet("10.123.56.1".parse().unwrap())],
            ],
        );

        let expected = create_response_message(
            &col_spec,
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
        let col_spec = vec![
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
            &col_spec,
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
            &col_spec,
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
}
