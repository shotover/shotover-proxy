use anyhow::{anyhow, Result};
use bytes::Bytes;
use cassandra_protocol::compression::Compression;
use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::events::SchemaChange;
use cassandra_protocol::frame::frame_batch::{BatchQuery, BatchQuerySubj, BatchType, BodyReqBatch};
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
use cassandra_protocol::query::{QueryParams, QueryValues};
use cassandra_protocol::types::{CBytes, CBytesShort, CInt, CLong};
use nonzero_ext::nonzero;
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::convert::TryInto;
use std::num::NonZeroU32;
use uuid::Uuid;

use crate::message::{MessageValue, QueryType};

/// Extract the length of a BATCH statement (count of requests) from the body bytes
fn get_batch_len(bytes: &[u8]) -> Result<NonZeroU32> {
    let len = bytes.len();
    if len < 2 {
        return Err(anyhow!("BATCH statement body is not long enough"));
    }

    let short_bytes = &bytes[1..3];
    let short = u16::from_be_bytes(short_bytes.try_into()?);

    // it is valid for a batch statement to have 0 statements,
    // but for the purposes of shotover throttling we can count it as one query
    Ok(NonZeroU32::new(short.into()).unwrap_or(nonzero!(1u32)))
}

pub(crate) struct CassandraMetadata {
    pub version: Version,
    pub stream_id: StreamId,
    pub tracing_id: Option<Uuid>,
    // missing `warnings` field because we are not using it currently
}

/// Parse metadata only from an unparsed Cassandra frame
pub(crate) fn metadata(bytes: &[u8]) -> Result<CassandraMetadata> {
    let frame = RawCassandraFrame::from_buffer(bytes, Compression::None)
        .map_err(|e| anyhow!("{e:?}"))?
        .frame;

    Ok(CassandraMetadata {
        version: frame.version,
        stream_id: frame.stream_id,
        tracing_id: frame.tracing_id,
    })
}

/// Count "cells" only from an unparsed Cassandra frame
pub(crate) fn cell_count(bytes: &[u8]) -> Result<NonZeroU32> {
    let frame = RawCassandraFrame::from_buffer(bytes, Compression::None)
        .map_err(|e| anyhow!("{e:?}"))?
        .frame;

    Ok(match frame.opcode {
        Opcode::Batch => get_batch_len(&frame.body)?,
        _ => nonzero!(1u32),
    })
}

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
    /// Return `CassandraMetadata` from this `CassandraFrame`
    pub(crate) fn metadata(&self) -> CassandraMetadata {
        CassandraMetadata {
            version: self.version,
            stream_id: self.stream_id,
            tracing_id: self.tracing_id,
        }
    }

    // Count the amount of cells in this `CassandraFrame`, this will either be the count of all queries in a BATCH statement or 1 for all other types of Cassandra queries
    pub(crate) fn cell_count(&self) -> Result<NonZeroU32> {
        Ok(match &self.operation {
            CassandraOperation::Batch(batch) => {
                // it doesnt make sense to say a message is 0 messages, so when the batch has no queries we round up to 1
                NonZeroU32::new(batch.queries.len() as u32).unwrap_or(nonzero!(1u32))
            }
            _ => nonzero!(1u32),
        })
    }

    pub fn from_bytes(bytes: Bytes) -> Result<Self> {
        let frame = RawCassandraFrame::from_buffer(&bytes, Compression::None)
            .map_err(|e| anyhow!("{e:?}"))?
            .frame;
        let operation = match frame.opcode {
            Opcode::Query => {
                if let RequestBody::Query(body) = frame.request_body()? {
                    CassandraOperation::Query {
                        query: CQL::parse_from_string(body.query),
                        params: body.query_params,
                    }
                } else {
                    unreachable!("We already know the operation is a query")
                }
            }
            Opcode::Result => {
                if let ResponseBody::Result(result) = frame.response_body()? {
                    match result {
                        ResResultBody::Rows(rows) => {
                            let converted_rows =
                                if rows.metadata.flags.contains(RowsMetadataFlags::NO_METADATA) {
                                    rows.rows_content
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
                                        .collect()
                                } else {
                                    rows.rows_content
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
                                        .collect()
                                };
                            CassandraOperation::Result(CassandraResult::Rows {
                                value: MessageValue::Rows(converted_rows),
                                metadata: rows.metadata,
                            })
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
                    unreachable!("We already know the operation is a result")
                }
            }
            Opcode::Error => {
                if let ResponseBody::Error(body) = frame.response_body()? {
                    CassandraOperation::Error(body)
                } else {
                    unreachable!("We already know the operation is an error")
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
            Opcode::Batch => {
                if let RequestBody::Batch(body) = frame.request_body()? {
                    CassandraOperation::Batch(CassandraBatch {
                        ty: body.batch_type,
                        queries: body
                            .queries
                            .into_iter()
                            .map(|query| BatchStatement {
                                ty: match query.subject {
                                    BatchQuerySubj::QueryString(query) => {
                                        BatchStatementType::Statement(CQL::parse_from_string(query))
                                    }
                                    BatchQuerySubj::PreparedId(id) => {
                                        BatchStatementType::PreparedId(id)
                                    }
                                },
                                values: query.values,
                            })
                            .collect(),
                        consistency: body.consistency,
                        serial_consistency: body.serial_consistency,
                        timestamp: body.timestamp,
                    })
                } else {
                    unreachable!("We already know the operation is a batch")
                }
            }
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

    pub fn get_query_type(&self) -> QueryType {
        match &self.operation {
            CassandraOperation::Query {
                query: CQL::Parsed(query),
                ..
            } => match query.as_ref() {
                Statement::Query(_) => QueryType::Read,
                Statement::Insert { .. } => QueryType::Write,
                Statement::Update { .. } => QueryType::Write,
                Statement::Delete { .. } => QueryType::Write,
                // TODO: handle prepared, execute and schema change query types
                _ => QueryType::Read,
            },
            _ => QueryType::Read,
        }
    }

    pub fn namespace(&self) -> Vec<String> {
        match &self.operation {
            CassandraOperation::Query {
                query: CQL::Parsed(query),
                ..
            } => match query.as_ref() {
                Statement::Query(query) => match &query.body {
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
                Statement::Insert { table_name, .. } | Statement::Delete { table_name, .. } => {
                    table_name.0.iter().map(|a| a.value.clone()).collect()
                }
                Statement::Update { table, .. } => match &table.relation {
                    TableFactor::Table { name, .. } => {
                        name.0.iter().map(|a| a.value.clone()).collect()
                    }
                    _ => vec![],
                },
                _ => vec![],
            },
            _ => vec![],
        }
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
    Query { query: CQL, params: QueryParams },
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
    Batch(CassandraBatch),
    AuthChallenge(Vec<u8>),
    AuthResponse(Vec<u8>),
    AuthSuccess(Vec<u8>),
}

impl CassandraOperation {
    /// Return all queries contained within CassandaOperation::Query and CassandraOperation::Batch
    /// An Err is returned if the operation cannot contain queries or the queries failed to parse.
    ///
    /// TODO: This will return a custom iterator type when BATCH support is added
    pub fn queries(&mut self) -> Result<std::iter::Once<&mut Statement>> {
        match self {
            CassandraOperation::Query {
                query: CQL::Parsed(query),
                ..
            } => Ok(std::iter::once(query)),
            CassandraOperation::Query {
                query: CQL::FailedToParse(_),
                ..
            } => Err(anyhow!("Couldnt parse query")),
            // TODO: Return CassandraOperation::Batch queries once we add BATCH parsing to cassandra-protocol
            _ => Err(anyhow!("This operation cannot contain queries")),
        }
    }

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
                query: query.to_query_string(),
                query_params: params,
            }
            .serialize_to_vec(),
            CassandraOperation::Result(result) => match result {
                CassandraResult::Rows { value, metadata } => {
                    Self::build_cassandra_result_body(value, metadata)
                }
                CassandraResult::SetKeyspace(set_keyspace) => {
                    ResResultBody::SetKeyspace(*set_keyspace)
                }
                CassandraResult::Prepared(prepared) => ResResultBody::Prepared(*prepared),
                CassandraResult::SchemaChange(schema_change) => {
                    ResResultBody::SchemaChange(schema_change)
                }
                CassandraResult::Void => ResResultBody::Void,
            }
            .serialize_to_vec(),
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
            CassandraOperation::Batch(batch) => BodyReqBatch {
                batch_type: batch.ty,
                consistency: batch.consistency,
                queries: batch
                    .queries
                    .into_iter()
                    .map(|query| BatchQuery {
                        subject: match query.ty {
                            BatchStatementType::PreparedId(id) => BatchQuerySubj::PreparedId(id),
                            BatchStatementType::Statement(statement) => {
                                BatchQuerySubj::QueryString(statement.to_query_string())
                            }
                        },
                        values: query.values,
                    })
                    .collect(),
                serial_consistency: batch.serial_consistency,
                timestamp: batch.timestamp,
            }
            .serialize_to_vec(),
            CassandraOperation::AuthChallenge(bytes) => bytes.to_vec(),
            CassandraOperation::AuthResponse(bytes) => bytes.to_vec(),
            CassandraOperation::AuthSuccess(bytes) => bytes.to_vec(),
        }
    }

    fn build_cassandra_result_body(result: MessageValue, metadata: RowsMetadata) -> ResResultBody {
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

            return ResResultBody::Rows(BodyResResultRows {
                metadata,
                rows_count,
                rows_content,
            });
        }
        unreachable!()
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum CQL {
    // Box is used because Statement is very large
    Parsed(Box<Statement>),
    FailedToParse(String),
}

impl CQL {
    pub fn to_query_string(&self) -> String {
        match self {
            CQL::Parsed(ast) => ast.to_string(),
            CQL::FailedToParse(str) => str.clone(),
        }
    }

    pub fn parse_from_string(sql: String) -> Self {
        match Parser::parse_sql(&GenericDialect, &sql) {
            _ if sql.contains("ALTER TABLE") || sql.contains("CREATE TABLE") => {
                tracing::error!("Failed to parse CQL for frame {:?}\nError: Blacklisted query as sqlparser crate cant round trip it", sql);
                CQL::FailedToParse(sql)
            }
            Ok(ast) if ast.is_empty() => {
                tracing::error!(
                    "Failed to parse CQL for frame {:?}\nResulted in no statements.",
                    sql
                );
                CQL::FailedToParse(sql)
            }
            Ok(mut ast) => CQL::Parsed(Box::new(ast.remove(0))),
            Err(err) => {
                tracing::error!("Failed to parse CQL for frame {:?}\nError: {:?}", sql, err);
                CQL::FailedToParse(sql)
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum CassandraResult {
    Rows {
        value: MessageValue,
        metadata: RowsMetadata,
    },
    // SetKeyspace and Prepared are boxed because they take up a lot more stack space than Void.
    SetKeyspace(Box<BodyResResultSetKeyspace>),
    Prepared(Box<BodyResResultPrepared>),
    SchemaChange(SchemaChange),
    Void,
}

#[derive(PartialEq, Debug, Clone)]
pub enum BatchStatementType {
    Statement(CQL),
    PreparedId(CBytesShort),
}

#[derive(PartialEq, Debug, Clone)]
pub struct BatchStatement {
    ty: BatchStatementType,
    values: QueryValues,
}

#[derive(PartialEq, Debug, Clone)]
pub struct CassandraBatch {
    ty: BatchType,
    queries: Vec<BatchStatement>,
    consistency: Consistency,
    serial_consistency: Option<Consistency>,
    timestamp: Option<CLong>,
}
