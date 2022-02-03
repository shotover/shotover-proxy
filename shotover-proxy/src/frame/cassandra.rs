use anyhow::{anyhow, Result};
use bytes::Bytes;
use cassandra_protocol::compression::Compression;
use cassandra_protocol::events::SchemaChange;
use cassandra_protocol::frame::frame_error::ErrorBody;
use cassandra_protocol::frame::frame_query::BodyReqQuery;
use cassandra_protocol::frame::frame_request::RequestBody;
use cassandra_protocol::frame::frame_response::ResponseBody;
use cassandra_protocol::frame::frame_result::{
    BodyResResultPrepared, BodyResResultRows, BodyResResultSetKeyspace, ResResultBody,
    RowsMetadata, RowsMetadataFlags,
};
use cassandra_protocol::frame::{
    Direction, Flags, Frame as RawCassandraFrame, Opcode, Serialize, StreamId, Version,
};
use cassandra_protocol::query::QueryParams;
use cassandra_protocol::types::{CBytes, CInt};
use itertools::Itertools;
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use uuid::Uuid;

use crate::message::{MessageValue, QueryType};

#[derive(PartialEq, Debug, Clone)]
pub struct CassandraFrame {
    pub version: Version,
    pub stream_id: StreamId,
    pub tracing_id: Option<Uuid>,
    pub warnings: Vec<String>,
    /// Contains the message body
    pub operation: CassandraOperation,
}

impl CassandraFrame {
    pub fn get_query_type(&self) -> QueryType {
        match &self.operation {
            CassandraOperation::Query { query, .. } => match query.get(0) {
                Some(Statement::Query(_x)) => QueryType::Read,
                Some(Statement::Insert { .. }) => QueryType::Write,
                Some(Statement::Update { .. }) => QueryType::Write,
                Some(Statement::Delete { .. }) => QueryType::Write,
                _ => QueryType::Read,
            },
            _ => QueryType::Read,
        }
    }

    pub fn namespace(&self) -> Vec<String> {
        match &self.operation {
            CassandraOperation::Query { query, .. } => match query.first() {
                Some(Statement::Query(query)) => match &query.body {
                    SetExpr::Select(select) => {
                        if let TableFactor::Table { name, .. } =
                            &select.from.get(0).unwrap().relation
                        {
                            name.0.iter().map(|a| a.value.clone()).collect()
                        } else {
                            vec![]
                        }
                    }
                    _ => vec![],
                },
                Some(Statement::Insert { table_name, .. })
                | Some(Statement::Delete { table_name, .. }) => {
                    table_name.0.iter().map(|a| a.value.clone()).collect()
                }
                Some(Statement::Update { table, .. }) => match &table.relation {
                    TableFactor::Table { name, .. } => {
                        name.0.iter().map(|a| a.value.clone()).collect()
                    }
                    _ => {
                        tracing::error!(
                            "The cassandra query language does not support `update`s with table of {:?}",
                            table
                        );
                        vec![]
                    }
                },
                _ => vec![],
            },
            _ => vec![],
        }
    }

    pub fn from_bytes(bytes: Bytes) -> Result<Self> {
        let frame = RawCassandraFrame::from_buffer(&bytes, Compression::None)
            .map_err(|e| anyhow!("{e:?}"))?
            .frame;
        let operation = match frame.opcode {
            Opcode::Query => {
                if let RequestBody::Query(body) = frame.request_body()? {
                    CassandraOperation::Query {
                        query: Parser::parse_sql(&GenericDialect, body.query.as_str())?,
                        params: body.query_params,
                    }
                } else {
                    unreachable!()
                }
            }
            Opcode::Result => {
                if let ResponseBody::Result(result) = frame.response_body()? {
                    match result {
                        ResResultBody::Rows(rows) => {
                            if rows.metadata.flags.contains(RowsMetadataFlags::NO_METADATA) {
                                let converted_rows = rows
                                    .rows_content
                                    .into_iter()
                                    .map(|row| {
                                        row.into_iter()
                                            .map(|row_content| {
                                                MessageValue::Bytes(
                                                    row_content.into_bytes().unwrap().into(),
                                                )
                                            })
                                            .collect()
                                    })
                                    .collect();
                                CassandraOperation::Result(CassandraResult::Rows {
                                    value: MessageValue::Rows(converted_rows),
                                    metadata: rows.metadata,
                                })
                            } else {
                                let converted_rows = rows
                                    .rows_content
                                    .into_iter()
                                    .map(|row| {
                                        row.into_iter()
                                            .enumerate()
                                            .map(|(i, row_content)| {
                                                let col_spec = &rows.metadata.col_specs[i];
                                                MessageValue::build_value_from_cstar_col_type(
                                                    col_spec,
                                                    &row_content,
                                                )
                                            })
                                            .collect()
                                    })
                                    .collect();
                                CassandraOperation::Result(CassandraResult::Rows {
                                    value: MessageValue::Rows(converted_rows),
                                    metadata: rows.metadata,
                                })
                            }
                        }
                        ResResultBody::SetKeyspace(set_keyspace) => CassandraOperation::Result(
                            CassandraResult::SetKeyspace(Box::new(set_keyspace)),
                        ),
                        ResResultBody::Prepared(prepared) => CassandraOperation::Result(
                            CassandraResult::Prepared(Box::new(prepared)),
                        ),
                        ResResultBody::SchemaChange(schema_change) => {
                            CassandraOperation::Result(CassandraResult::SchemaChange(schema_change))
                        }
                        ResResultBody::Void => CassandraOperation::Result(CassandraResult::Void),
                    }
                } else {
                    unreachable!()
                }
            }
            Opcode::Error => {
                if let ResponseBody::Error(body) = frame.response_body()? {
                    CassandraOperation::Error(body)
                } else {
                    unreachable!()
                }
            }
            Opcode::Startup => CassandraOperation::Startup(frame.body),
            Opcode::Ready => CassandraOperation::Ready(frame.body),
            Opcode::Authenticate => CassandraOperation::Authenticate(frame.body),
            Opcode::Options => CassandraOperation::Options(frame.body),
            Opcode::Supported => CassandraOperation::Supported(frame.body),
            Opcode::Prepare => CassandraOperation::Prepare(frame.body),
            Opcode::Execute => CassandraOperation::Execute(frame.body),
            Opcode::Register => CassandraOperation::Register(frame.body),
            Opcode::Event => CassandraOperation::Event(frame.body),
            Opcode::Batch => CassandraOperation::Batch(frame.body),
            Opcode::AuthChallenge => CassandraOperation::AuthChallenge(frame.body),
            Opcode::AuthResponse => CassandraOperation::AuthResponse(frame.body),
            Opcode::AuthSuccess => CassandraOperation::AuthSuccess(frame.body),
        };

        Ok(CassandraFrame {
            version: frame.version,
            stream_id: frame.stream_id,
            tracing_id: frame.tracing_id,
            warnings: frame.warnings,
            operation,
        })
    }

    pub fn encode(self) -> RawCassandraFrame {
        RawCassandraFrame {
            direction: self.operation.to_direction(),
            version: self.version,
            flags: Flags::default(),
            opcode: self.operation.to_opcode(),
            stream_id: self.stream_id,
            body: self.operation.into_body(),
            tracing_id: self.tracing_id,
            warnings: self.warnings,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum CassandraOperation {
    Query {
        query: Vec<Statement>,
        params: QueryParams,
    },
    Result(CassandraResult),
    Error(ErrorBody),
    // operations for protocol negotiation, should be ignored by transforms
    Startup(Vec<u8>),
    Ready(Vec<u8>),
    Authenticate(Vec<u8>),
    Options(Vec<u8>),
    Supported(Vec<u8>),
    Prepare(Vec<u8>),
    Execute(Vec<u8>),
    Register(Vec<u8>),
    Event(Vec<u8>),
    Batch(Vec<u8>),
    AuthChallenge(Vec<u8>),
    AuthResponse(Vec<u8>),
    AuthSuccess(Vec<u8>),
}

impl CassandraOperation {
    fn to_direction(&self) -> Direction {
        match self {
            CassandraOperation::Query { .. } => Direction::Request,
            CassandraOperation::Result { .. } => Direction::Request,
            CassandraOperation::Error(_) => Direction::Response,
            CassandraOperation::Startup(_) => Direction::Request,
            CassandraOperation::Ready(_) => Direction::Response,
            CassandraOperation::Authenticate(_) => Direction::Response,
            CassandraOperation::Options(_) => Direction::Request,
            CassandraOperation::Supported(_) => Direction::Response,
            CassandraOperation::Prepare(_) => Direction::Request,
            CassandraOperation::Execute(_) => Direction::Request,
            CassandraOperation::Register(_) => Direction::Request,
            CassandraOperation::Event(_) => Direction::Response,
            CassandraOperation::Batch(_) => Direction::Request,
            CassandraOperation::AuthChallenge(_) => Direction::Response,
            CassandraOperation::AuthResponse(_) => Direction::Request,
            CassandraOperation::AuthSuccess(_) => Direction::Response,
        }
    }
    fn to_opcode(&self) -> Opcode {
        match self {
            CassandraOperation::Query { .. } => Opcode::Query,
            CassandraOperation::Result { .. } => Opcode::Result,
            CassandraOperation::Error(_) => Opcode::Error,
            CassandraOperation::Startup(_) => Opcode::Startup,
            CassandraOperation::Ready(_) => Opcode::Ready,
            CassandraOperation::Authenticate(_) => Opcode::Authenticate,
            CassandraOperation::Options(_) => Opcode::Options,
            CassandraOperation::Supported(_) => Opcode::Supported,
            CassandraOperation::Prepare(_) => Opcode::Prepare,
            CassandraOperation::Execute(_) => Opcode::Execute,
            CassandraOperation::Register(_) => Opcode::Register,
            CassandraOperation::Event(_) => Opcode::Event,
            CassandraOperation::Batch(_) => Opcode::Batch,
            CassandraOperation::AuthChallenge(_) => Opcode::AuthChallenge,
            CassandraOperation::AuthResponse(_) => Opcode::AuthResponse,
            CassandraOperation::AuthSuccess(_) => Opcode::AuthSuccess,
        }
    }

    fn into_body(self) -> Vec<u8> {
        match self {
            CassandraOperation::Query { query, params } => BodyReqQuery {
                query: query.iter().map(|x| x.to_string()).join(""),
                query_params: params,
            }
            .serialize_to_vec(),
            CassandraOperation::Result(CassandraResult::Rows { value, metadata }) => {
                Self::build_cassandra_result_body(value, metadata)
            }
            CassandraOperation::Result(CassandraResult::SetKeyspace(set_keyspace)) => {
                set_keyspace.serialize_to_vec()
            }
            CassandraOperation::Result(CassandraResult::Prepared(prepared)) => {
                prepared.serialize_to_vec()
            }
            CassandraOperation::Result(CassandraResult::SchemaChange(schema_change)) => {
                schema_change.serialize_to_vec()
            }
            CassandraOperation::Result(CassandraResult::Void) => vec![0, 0, 0, 1],
            CassandraOperation::Error(error) => error.serialize_to_vec(),
            CassandraOperation::Startup(bytes) => bytes.to_vec(),
            CassandraOperation::Ready(bytes) => bytes.to_vec(),
            CassandraOperation::Authenticate(bytes) => bytes.to_vec(),
            CassandraOperation::Options(bytes) => bytes.to_vec(),
            CassandraOperation::Supported(bytes) => bytes.to_vec(),
            CassandraOperation::Prepare(bytes) => bytes.to_vec(),
            CassandraOperation::Execute(bytes) => bytes.to_vec(),
            CassandraOperation::Register(bytes) => bytes.to_vec(),
            CassandraOperation::Event(bytes) => bytes.to_vec(),
            CassandraOperation::Batch(bytes) => bytes.to_vec(),
            CassandraOperation::AuthChallenge(bytes) => bytes.to_vec(),
            CassandraOperation::AuthResponse(bytes) => bytes.to_vec(),
            CassandraOperation::AuthSuccess(bytes) => bytes.to_vec(),
        }
    }

    pub fn build_cassandra_result_body(result: MessageValue, metadata: RowsMetadata) -> Vec<u8> {
        if let MessageValue::Rows(rows) = result {
            let rows_count = rows.len() as CInt;
            let rows_content = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|value| {
                            CBytes::new(
                                cassandra_protocol::types::value::Bytes::from(value).into_inner(),
                            )
                        })
                        .collect()
                })
                .collect();

            let response = ResResultBody::Rows(BodyResResultRows {
                metadata,
                rows_count,
                rows_content,
            });

            return response.serialize_to_vec();
        }
        unreachable!()
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum CassandraResult {
    Rows {
        value: MessageValue,
        metadata: RowsMetadata,
    },
    SetKeyspace(Box<BodyResResultSetKeyspace>),
    Prepared(Box<BodyResResultPrepared>),
    SchemaChange(SchemaChange),
    Void,
}
