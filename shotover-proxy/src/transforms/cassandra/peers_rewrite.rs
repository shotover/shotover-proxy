use crate::frame::{CassandraOperation, CassandraResult, Frame};
use crate::message::{IntSize, Message, MessageValue};
use crate::{
    config::topology::TopicHolder,
    error::ChainResponse,
    transforms::{Transform, Transforms, Wrapper},
};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraPeersRewriteConfig {
    pub port: u32,
}

impl CassandraPeersRewriteConfig {
    pub async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::CassandraPeersRewrite(CassandraPeersRewrite {
            port: self.port,
        }))
    }
}

#[derive(Clone)]
pub struct CassandraPeersRewrite {
    port: u32,
}

#[async_trait]
impl Transform for CassandraPeersRewrite {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        // Find the indices of queries to system.peers & system.peers_v2
        let system_peers = message_wrapper
            .messages
            .iter_mut()
            .enumerate()
            .filter_map(|(i, m)| if is_system_peers(m) { Some(i) } else { None })
            .collect::<Vec<_>>();

        let mut response = message_wrapper.call_next_transform().await?;

        for i in system_peers {
            rewrite_port(&mut response[i], self.port);
        }

        Ok(response)
    }
}

fn is_system_peers(message: &mut Message) -> bool {
    if let Some(Frame::Cassandra(_)) = message.frame() {
        if let Some(namespace) = message.namespace() {
            if namespace.len() > 1 {
                return namespace[0] == "system" && namespace[1] == "peers_v2";
            }
        }
    }

    false
}

/// Rewrite the `native_port` field in the results from a query to `system.peers_v2` table
/// Only Cassandra queries to the `system.peers` table found via the `is_system_peers` function should be passed to this
fn rewrite_port(message: &mut Message, new_port: u32) {
    if let Some(Frame::Cassandra(frame)) = message.frame() {
        if let CassandraOperation::Result(CassandraResult::Rows { value, metadata }) =
            &mut frame.operation
        {
            let port_column_index = metadata
                .col_specs
                .iter()
                .position(|col| col.name.as_str() == "native_port");

            if let Some(i) = port_column_index {
                if let MessageValue::Rows(rows) = &mut *value {
                    for row in rows.iter_mut() {
                        row[i] = MessageValue::Integer(new_port as i64, IntSize::I32);
                    }
                    message.invalidate_cache();
                }
            }
        } else {
            panic!(
                "Expected CassandraOperation::Result(CassandraResult::Rows), got {:?}",
                frame
            );
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frame::{CassandraFrame, CQL};
    use crate::transforms::cassandra::peers_rewrite::CassandraResult::Rows;
    use cassandra_protocol::{
        consistency::Consistency,
        frame::{
            frame_result::{
                ColSpec,
                ColType::{Inet, Int},
                ColTypeOption, RowsMetadata, RowsMetadataFlags, TableSpec,
            },
            Version,
        },
        query::QueryParams,
    };

    fn create_query_message(query: String) -> Message {
        let original = Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: CQL::parse_from_string(query),
                params: QueryParams {
                    consistency: Consistency::One,
                    with_names: false,
                    values: None,
                    page_size: Some(5000),
                    paging_state: None,
                    serial_consistency: None,
                    timestamp: Some(1643855761086585),
                },
            },
        });

        Message::from_frame(original)
    }

    fn create_response_message(rows: Vec<Vec<MessageValue>>) -> Message {
        let original = Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Result(Rows {
                value: MessageValue::Rows(rows),
                metadata: RowsMetadata {
                    flags: RowsMetadataFlags::GLOBAL_TABLE_SPACE,
                    columns_count: 1,
                    paging_state: None,
                    global_table_spec: Some(TableSpec {
                        ks_name: "system".into(),
                        table_name: "peers_v2".into(),
                    }),
                    col_specs: vec![ColSpec {
                        table_spec: None,
                        name: "native_port".into(),
                        col_type: ColTypeOption {
                            id: Int,
                            value: None,
                        },
                    }],
                },
            }),
        });

        Message::from_frame(original)
    }

    #[test]
    fn test_is_system_peers_v2() {
        assert!(is_system_peers(&mut create_query_message(
            "SELECT * FROM system.peers_v2;".into()
        )));

        assert!(!is_system_peers(&mut create_query_message(
            "SELECT * FROM not_system.peers_v2;".into()
        )));

        assert!(!is_system_peers(&mut create_query_message("".into())));
    }

    #[test]
    fn test_rewrite_port() {
        //Test rewrites `native_port` column when included
        {
            let mut message = create_response_message(vec![
                vec![MessageValue::Integer(9042, IntSize::I32)],
                vec![MessageValue::Integer(9042, IntSize::I32)],
            ]);

            rewrite_port(&mut message, 9043);

            let expected = create_response_message(vec![
                vec![MessageValue::Integer(9043, IntSize::I32)],
                vec![MessageValue::Integer(9043, IntSize::I32)],
            ]);

            assert_eq!(message, expected);
        }

        // Test does not rewrite anything when `native_port` column not included
        {
            let frame = Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                stream_id: 0,
                tracing_id: None,
                warnings: vec![],
                operation: CassandraOperation::Result(Rows {
                    value: MessageValue::Rows(vec![vec![MessageValue::Inet(
                        "127.0.0.1".parse().unwrap(),
                    )]]),
                    metadata: RowsMetadata {
                        flags: RowsMetadataFlags::GLOBAL_TABLE_SPACE,
                        columns_count: 1,
                        paging_state: None,
                        global_table_spec: Some(TableSpec {
                            ks_name: "system".into(),
                            table_name: "peers_v2".into(),
                        }),
                        col_specs: vec![ColSpec {
                            table_spec: None,
                            name: "peer".into(),
                            col_type: ColTypeOption {
                                id: Inet,
                                value: None,
                            },
                        }],
                    },
                }),
            });

            let mut original = Message::from_frame(frame);

            let expected = original.clone();

            rewrite_port(&mut original, 9043);

            assert_eq!(original, expected);
        }
    }
}
