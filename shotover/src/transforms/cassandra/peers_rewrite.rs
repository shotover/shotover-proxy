use crate::frame::MessageType;
use crate::message::{Message, MessageIdMap};
use crate::transforms::cassandra::peers_rewrite::CassandraOperation::Event;
use crate::transforms::{
    DownChainProtocol, Responses, Transform, TransformBuilder, TransformConfig,
    TransformContextBuilder, UpChainProtocol, Wrapper,
};
use crate::{
    frame::{
        value::{GenericValue, IntSize},
        CassandraOperation, CassandraResult, Frame,
    },
    transforms::TransformContextConfig,
};
use anyhow::Result;
use async_trait::async_trait;
use cassandra_protocol::frame::events::{ServerEvent, StatusChange};
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{FQName, Identifier};
use cql3_parser::select::SelectElement;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct CassandraPeersRewriteConfig {
    pub port: u16,
}

const NAME: &str = "CassandraPeersRewrite";
#[typetag::serde(name = "CassandraPeersRewrite")]
#[async_trait(?Send)]
impl TransformConfig for CassandraPeersRewriteConfig {
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(CassandraPeersRewrite::new(self.port)))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::MustBeOneOf(vec![MessageType::Cassandra])
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

#[derive(Clone)]
pub struct CassandraPeersRewrite {
    port: u16,
    peer_table: FQName,
    column_names_to_rewrite: MessageIdMap<Vec<Identifier>>,
}

impl CassandraPeersRewrite {
    pub fn new(port: u16) -> Self {
        CassandraPeersRewrite {
            port,
            peer_table: FQName::new("system", "peers_v2"),
            column_names_to_rewrite: Default::default(),
        }
    }
}

impl TransformBuilder for CassandraPeersRewrite {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(self.clone())
    }

    fn get_name(&self) -> &'static str {
        NAME
    }
}

#[async_trait]
impl Transform for CassandraPeersRewrite {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Responses> {
        // Find the indices of queries to system.peers & system.peers_v2
        // we need to know which columns in which CQL queries in which messages have system peers
        for request in &mut requests_wrapper.requests {
            let sys_peers = extract_native_port_column(&self.peer_table, request);
            self.column_names_to_rewrite.insert(request.id(), sys_peers);
        }

        let mut responses = requests_wrapper.call_next_transform().await?;

        for response in &mut responses.responses {
            if let Some(Frame::Cassandra(frame)) = response.frame() {
                if let Event(ServerEvent::StatusChange(StatusChange { addr, .. })) =
                    &mut frame.operation
                {
                    addr.set_port(self.port);
                    response.invalidate_cache();
                }
            }

            if let Some(id) = response.request_id() {
                let name_list = self.column_names_to_rewrite.remove(&id).unwrap();
                rewrite_port(response, &name_list, self.port);
            }
        }

        Ok(responses)
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
        if let CassandraOperation::Result(CassandraResult::Rows { rows, metadata }) =
            &mut frame.operation
        {
            for (i, col) in metadata.col_specs.iter().enumerate() {
                if column_names.contains(&Identifier::parse(&col.name)) {
                    for row in rows.iter_mut() {
                        row[i] = GenericValue::Integer(new_port as i64, IntSize::I32);
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
    use crate::frame::cassandra::{parse_statement_single, Tracing};
    use crate::frame::CassandraFrame;
    use crate::transforms::cassandra::peers_rewrite::CassandraResult::Rows;
    use cassandra_protocol::consistency::Consistency;
    use cassandra_protocol::frame::message_result::{
        ColSpec, ColType, ColTypeOption, RowsMetadata, RowsMetadataFlags, TableSpec,
    };
    use cassandra_protocol::frame::Version;
    use cassandra_protocol::query::QueryParams;
    use pretty_assertions::assert_eq;

    fn create_query_message(query: &str) -> Message {
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing: Tracing::Request(false),
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

    fn create_response_message(col_specs: &[ColSpec], rows: Vec<Vec<GenericValue>>) -> Message {
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing: Tracing::Response(None),
            warnings: vec![],
            operation: CassandraOperation::Result(Rows {
                rows,
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
                vec![GenericValue::Integer(9042, IntSize::I32)],
                vec![GenericValue::Integer(9042, IntSize::I32)],
            ],
        );

        let expected = create_response_message(
            &col_spec,
            vec![
                vec![GenericValue::Integer(9043, IntSize::I32)],
                vec![GenericValue::Integer(9043, IntSize::I32)],
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
                vec![GenericValue::Inet("127.0.0.1".parse().unwrap())],
                vec![GenericValue::Inet("10.123.56.1".parse().unwrap())],
            ],
        );

        let expected = create_response_message(
            &col_spec,
            vec![
                vec![GenericValue::Inet("127.0.0.1".parse().unwrap())],
                vec![GenericValue::Inet("10.123.56.1".parse().unwrap())],
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
                    GenericValue::Integer(9042, IntSize::I32),
                    GenericValue::Strings("Hello".into()),
                    GenericValue::Integer(9042, IntSize::I32),
                ],
                vec![
                    GenericValue::Integer(9042, IntSize::I32),
                    GenericValue::Strings("World".into()),
                    GenericValue::Integer(9042, IntSize::I32),
                ],
            ],
        );

        let expected = create_response_message(
            &col_spec,
            vec![
                vec![
                    GenericValue::Integer(9043, IntSize::I32),
                    GenericValue::Strings("Hello".into()),
                    GenericValue::Integer(9043, IntSize::I32),
                ],
                vec![
                    GenericValue::Integer(9043, IntSize::I32),
                    GenericValue::Strings("World".into()),
                    GenericValue::Integer(9043, IntSize::I32),
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
