use crate::frame::{CassandraOperation, CassandraResult, Frame};
use crate::message::{IntSize, Message, MessageValue};
use crate::{
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
    pub async fn get_transform(&self) -> Result<Transforms> {
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
    let keyspace = "system";
    let table = "peers_v2";
    let column = "native_port";

    // If the message isn't parsed only seek the table and column it for better performance
    if !message.is_parsed() {
        tracing::info!("using seek function");
        let bytes = message.get_raw_bytes();
        return seek_frame::is_reading_table_and_column(bytes, keyspace, table, column);
    }

    tracing::info!("using frame method");
    // otherwise if it's parsed we can just use frame
    if let Some(Frame::Cassandra(_)) = message.frame() {
        if let Some(namespace) = message.namespace() {
            if namespace.len() > 1 {
                return namespace[0] == keyspace && namespace[1] == table;
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

mod seek_frame {
    use cassandra_protocol::frame::Opcode;
    use cassandra_protocol::types::INT_LEN;
    use cql3_parser::{
        cassandra_ast::CassandraParser,
        select::{Named, SelectElement},
    };
    use std::io::{Cursor, Read};

    pub fn is_reading_table_and_column(
        bytes: &[u8],
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> bool {
        let opcode = Opcode::try_from(bytes[4]).unwrap();
        match opcode {
            Opcode::Query => {
                let mut body_cursor = Cursor::new(bytes);
                body_cursor.set_position(9);
                let query = {
                    let mut buff = [0; INT_LEN];
                    body_cursor.read_exact(&mut buff).unwrap();

                    let len = i32::from_be_bytes(buff) as usize;
                    let body_bytes = {
                        let start = body_cursor.position() as usize;
                        let result = &body_cursor.get_ref()[start..start + len];
                        body_cursor.set_position(body_cursor.position() + len as u64);

                        if result.len() != len {
                            panic!("cursor_next_value_ref could not retrieve a full slice")
                        } else {
                            result
                        }
                    };

                    std::str::from_utf8(body_bytes).unwrap()
                };

                let result = check_query(query, keyspace, table, column);
                tracing::info!("parsed query: {:?}; result: {:?}", query, result);
                result
            }
            _ => false,
        }
    }

    fn check_query(
        query: &str,
        search_keyspace: &str,
        search_table: &str,
        search_column: &str,
    ) -> bool {
        let language = tree_sitter_cql::language();
        let mut parser = tree_sitter::Parser::new();
        if parser.set_language(language).is_err() {
            panic!("language version mismatch");
        }

        let tree = parser.parse(query, None).unwrap();

        let mut cursor = tree.root_node().walk();
        let _ = cursor.goto_first_child();

        if cursor.node().kind() != "select_statement" {
            return false;
        }
        // skip past "SELECT" node
        cursor.goto_first_child();

        // Check if these nodes exist and skip past them
        cursor.goto_next_sibling();
        if cursor.node().kind().eq("DISTINCT") {
            cursor.goto_next_sibling();
        }

        if cursor.node().kind().eq("JSON") {
            cursor.goto_next_sibling();
        }

        // skip parsing the columns until we know the table
        let columns_start_node = {
            let mut cursor = cursor.node().walk();

            // record where the columns start so we can return
            let start_node = cursor.node();

            let mut process = cursor.goto_first_child();

            while process {
                match cursor.node().kind() {
                    "select_element" => {}
                    "*" => {}
                    _ => {}
                }
                process = cursor.goto_next_sibling();
            }
            start_node
        };

        let (keyspace, table) = {
            cursor.goto_next_sibling();
            let table = CassandraParser::parse_from_spec(&cursor.node(), query);
            (table.keyspace, table.name)
        };

        // check keyspace and table
        if Some(search_keyspace.to_string()) == keyspace && search_table == table {
            // parse columns now
            let mut result = vec![];
            cursor.reset(columns_start_node);
            cursor.node().walk();
            let mut process = cursor.goto_first_child();

            while process {
                match cursor.node().kind() {
                    "select_element" => {
                        result.push(CassandraParser::parse_select_element(&cursor.node(), query))
                    }
                    "*" => result.push(SelectElement::Star),
                    _ => {}
                }
                process = cursor.goto_next_sibling();
            }

            // check if the columns contain the column we are looking for or a "*"
            for x in result {
                match x {
                    SelectElement::Column(Named { name, .. }) => {
                        if search_column == name {
                            return true;
                        }
                    }
                    SelectElement::Star => {
                        return true;
                    }
                    _ => {
                        return false;
                    }
                };
            }
        };
        false
    }
}

#[cfg(test)]
mod test_seek_frame {
    use super::*;
    use cassandra_protocol::compression::Compression;

    fn get_cassandra_bytes(query: &str) -> Vec<u8> {
        let original = test_peers_rewrite::create_cassandra_frame(query);
        original.encode().encode_with(Compression::None).unwrap()
    }

    fn call_function(query: &str) -> bool {
        seek_frame::is_reading_table_and_column(
            &get_cassandra_bytes(query),
            "system",
            "peers_v2",
            "native_port",
        )
    }

    #[test]
    fn test_positive_cases() {
        assert!(call_function("SELECT * FROM system.peers_v2"));

        assert!(call_function(
            "SELECT data_center, native_port, rack FROM system.peers_v2;"
        ));

        assert!(call_function("SELECT native_port FROM system.peers_v2;"));
        assert!(call_function(
            "SELECT native_port, data_center, rack FROM system.peers_v2;"
        ));
    }

    #[test]
    fn test_negative_cases() {
        assert!(!call_function(
            "SELECT native_port FROM not_system.peers_v2"
        ));

        assert!(!call_function(
            "SELECT native_port FROM system.not_peers_v2"
        ));

        assert!(!call_function(
            "SELECT not_native_port FROM system.peers_v2"
        ));
    }
}

#[cfg(test)]
mod test_peers_rewrite {
    use super::*;
    use crate::frame::{CassandraFrame, Frame, CQL};
    use crate::transforms::cassandra::peers_rewrite::CassandraResult::Rows;
    use cassandra_protocol::{
        consistency::Consistency,
        frame::{
            frame_result::ColType::{Inet, Int},
            frame_result::{ColSpec, ColTypeOption, RowsMetadata, RowsMetadataFlags, TableSpec},
            Version,
        },
        query::QueryParams,
    };

    #[test]
    fn test_is_system_peers_v2() {
        assert!(is_system_peers(&mut create_query_message(
            "SELECT * FROM system.peers_v2;"
        )));

        assert!(!is_system_peers(&mut create_query_message(
            "SELECT * FROM not_system.peers_v2;"
        )));

        assert!(!is_system_peers(&mut create_query_message("")));
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

    pub fn create_query_message(query: &str) -> Message {
        let original = Frame::Cassandra(create_cassandra_frame(query));
        Message::from_frame(original)
    }

    pub fn create_cassandra_frame(query: &str) -> CassandraFrame {
        CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: CQL::parse_from_string(query.to_string()),
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
        }
    }

    pub fn create_response_message(rows: Vec<Vec<MessageValue>>) -> Message {
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
}
