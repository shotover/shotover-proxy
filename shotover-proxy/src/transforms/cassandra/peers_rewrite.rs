use crate::frame::{CassandraOperation, CassandraResult, Frame};
use crate::message::{IntSize, Message, MessageValue};
use crate::{
    error::ChainResponse,
    transforms::{Transform, Transforms, Wrapper},
};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use std::net::IpAddr;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraPeersRewriteConfig {
    pub port: Option<u32>,
    pub ip: Option<IpAddr>,
}

impl CassandraPeersRewriteConfig {
    pub async fn get_transform(&self) -> Result<Transforms> {
        Ok(Transforms::CassandraPeersRewrite(CassandraPeersRewrite {
            port: self.port,
            ip: self.ip,
        }))
    }
}

#[derive(Clone)]
pub struct CassandraPeersRewrite {
    port: Option<u32>,
    ip: Option<IpAddr>,
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
            if let Some(port) = self.port {
                rewrite_port(&mut response[i], port);
            }

            if let Some(ip) = self.ip {
                rewrite_ip(&mut response[i], ip);
            }
        }

        Ok(response)
    }
}

fn is_system_peers(message: &mut Message) -> bool {
    if let Some(Frame::Cassandra(_)) = message.frame() {
        if let Some(namespace) = message.namespace() {
            if namespace.len() > 1 {
                return namespace[0] == "system"
                    && (namespace[1] == "peers" || namespace[1] == "peers_v2");
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
            let columns = ["native_port", "rpc_port", "broadcast_port", "peer_port"];

            let port_column_indices = columns
                .iter()
                .map(|col| {
                    metadata
                        .col_specs
                        .iter()
                        .position(|wanted_col| &wanted_col.name.as_str() == col)
                })
                .flatten()
                .collect::<Vec<_>>();

            if let MessageValue::Rows(rows) = &mut *value {
                for i in &port_column_indices {
                    for row in rows.iter_mut() {
                        row[*i] = MessageValue::Integer(new_port as i64, IntSize::I32);
                    }
                }
                message.invalidate_cache();
            }
        } else {
            panic!(
                "Expected CassandraOperation::Result(CassandraResult::Rows), got {:?}",
                frame
            );
        }
    }
}

fn rewrite_ip(message: &mut Message, ip: IpAddr) {
    if let Some(Frame::Cassandra(frame)) = message.frame() {
        if let CassandraOperation::Result(CassandraResult::Rows { value, metadata }) =
            &mut frame.operation
        {
            let columns = ["rpc_address", "native_address", "broadcast_address", "peer"];

            let ip_column_indices = columns
                .iter()
                .map(|col| {
                    metadata
                        .col_specs
                        .iter()
                        .position(|wanted_col| &wanted_col.name.as_str() == col)
                })
                .flatten()
                .collect::<Vec<_>>();

            if !ip_column_indices.is_empty() {
                if let MessageValue::Rows(rows) = &mut *value {
                    for row in rows.iter_mut() {
                        for i in &ip_column_indices {
                            row[*i] = MessageValue::Inet(ip);
                        }
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
mod cassandra_peers_rewrite_tests {
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
                    col_specs: vec![
                        ColSpec {
                            table_spec: None,
                            name: "native_port".into(),
                            col_type: ColTypeOption {
                                id: Int,
                                value: None,
                            },
                        },
                        ColSpec {
                            table_spec: None,
                            name: "peer".into(),
                            col_type: ColTypeOption {
                                id: Inet,
                                value: None,
                            },
                        },
                    ],
                },
            }),
        });

        Message::from_frame(original)
    }

    #[test]
    fn test_is_system_peers() {
        assert!(is_system_peers(&mut create_query_message(
            "SELECT * FROM system.peers;".into()
        )));

        assert!(!is_system_peers(&mut create_query_message(
            "SELECT * FROM not_system.peers;".into()
        )));

        assert!(!is_system_peers(&mut create_query_message("".into())));
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
                vec![
                    MessageValue::Integer(9042, IntSize::I32),
                    MessageValue::Inet("127.0.0.1".parse().unwrap()),
                ],
                vec![
                    MessageValue::Integer(9042, IntSize::I32),
                    MessageValue::Inet("127.0.0.1".parse().unwrap()),
                ],
            ]);

            rewrite_port(&mut message, 9043);

            let expected = create_response_message(vec![
                vec![
                    MessageValue::Integer(9043, IntSize::I32),
                    MessageValue::Inet("127.0.0.1".parse().unwrap()),
                ],
                vec![
                    MessageValue::Integer(9043, IntSize::I32),
                    MessageValue::Inet("127.0.0.1".parse().unwrap()),
                ],
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

    #[test]
    fn test_rewrite_ip() {
        {
            let mut message = create_response_message(vec![
                vec![
                    MessageValue::Integer(9042, IntSize::I32),
                    MessageValue::Inet("127.0.0.1".parse().unwrap()),
                ],
                vec![
                    MessageValue::Integer(9042, IntSize::I32),
                    MessageValue::Inet("127.0.0.1".parse().unwrap()),
                ],
            ]);

            rewrite_ip(&mut message, "127.0.0.2".parse().unwrap());

            let expected = create_response_message(vec![
                vec![
                    MessageValue::Integer(9042, IntSize::I32),
                    MessageValue::Inet("127.0.0.2".parse().unwrap()),
                ],
                vec![
                    MessageValue::Integer(9042, IntSize::I32),
                    MessageValue::Inet("127.0.0.2".parse().unwrap()),
                ],
            ]);

            assert_eq!(message, expected);
        }
    }
}
