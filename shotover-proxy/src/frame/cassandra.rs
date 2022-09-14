use crate::message::{MessageValue, QueryType};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use cassandra_protocol::compression::Compression;
use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::events::SchemaChange;
use cassandra_protocol::frame::events::ServerEvent;
use cassandra_protocol::frame::message_batch::{
    BatchQuery, BatchQuerySubj, BatchType, BodyReqBatch,
};
use cassandra_protocol::frame::message_error::ErrorBody;
use cassandra_protocol::frame::message_event::BodyResEvent;
use cassandra_protocol::frame::message_execute::BodyReqExecuteOwned;
use cassandra_protocol::frame::message_query::BodyReqQuery;
use cassandra_protocol::frame::message_request::RequestBody;
use cassandra_protocol::frame::message_response::ResponseBody;
use cassandra_protocol::frame::message_result::{
    BodyResResultPrepared, BodyResResultRows, BodyResResultSetKeyspace, ResResultBody,
    RowsMetadata, RowsMetadataFlags,
};
use cassandra_protocol::frame::{
    Direction, Envelope as RawCassandraFrame, Flags, Opcode, Serialize, StreamId, Version,
};
use cassandra_protocol::query::{QueryParams, QueryValues};
use cassandra_protocol::types::blob::Blob;
use cassandra_protocol::types::cassandra_type::CassandraType;
use cassandra_protocol::types::{CBytes, CBytesShort, CInt, CLong};
use cql3_parser::begin_batch::{BatchType as ParserBatchType, BeginBatch};
use cql3_parser::cassandra_ast::CassandraAST;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::Operand;
use nonzero_ext::nonzero;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::str::FromStr;
use uuid::Uuid;

/// Functions for operations on an unparsed Cassandra frame
pub mod raw_frame {
    use super::{CassandraMetadata, RawCassandraFrame};
    use anyhow::{anyhow, bail, Result};
    use cassandra_protocol::{compression::Compression, frame::Opcode};
    use nonzero_ext::nonzero;
    use std::convert::TryInto;
    use std::num::NonZeroU32;

    /// Extract the length of a BATCH statement (count of requests) from the body bytes
    fn get_batch_len(bytes: &[u8]) -> Result<NonZeroU32> {
        if bytes.len() < 2 {
            bail!("BATCH statement body is not long enough");
        }

        let short_bytes = &bytes[1..3];
        let short = u16::from_be_bytes(short_bytes.try_into()?);

        // it is valid for a batch statement to have 0 statements,
        // but for the purposes of shotover throttling we can count it as one query
        Ok(NonZeroU32::new(short.into()).unwrap_or(nonzero!(1u32)))
    }

    /// Parse metadata only from an unparsed Cassandra frame
    pub(crate) fn metadata(bytes: &[u8]) -> Result<CassandraMetadata> {
        let frame = RawCassandraFrame::from_buffer(bytes, Compression::None)
            .map_err(|e| anyhow!("{e:?}"))?
            .envelope;

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
            .envelope;

        Ok(match frame.opcode {
            Opcode::Batch => get_batch_len(&frame.body)?,
            _ => nonzero!(1u32),
        })
    }

    pub(crate) fn get_opcode(bytes: &[u8]) -> Result<Opcode> {
        if bytes.len() < 9 {
            bail!("Cassandra frame too short, needs at least 9 bytes for header");
        }
        let opcode = Opcode::try_from(bytes[4])?;
        Ok(opcode)
    }
}

pub(crate) struct CassandraMetadata {
    pub version: Version,
    pub stream_id: StreamId,
    pub tracing_id: Option<Uuid>,
    // missing `warnings` field because we are not using it currently
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
            .envelope;
        let operation = match frame.opcode {
            Opcode::Query => {
                if let RequestBody::Query(body) = frame.request_body()? {
                    match parse_statement_query(&body.query) {
                        StatementResult::Query(query) => CassandraOperation::Query {
                            query,
                            params: Box::new(body.query_params),
                        },
                        StatementResult::Batch(statements, meta) => {
                            let ty = match meta.ty {
                                ParserBatchType::Logged => BatchType::Logged,
                                ParserBatchType::Unlogged => BatchType::Unlogged,
                                ParserBatchType::Counter => BatchType::Counter,
                            };
                            let queries = statements
                                .into_iter()
                                .map(|query| BatchStatement {
                                    ty: BatchStatementType::Statement(Box::new(query)),
                                    values: QueryValues::SimpleValues(vec![]),
                                })
                                .collect();
                            CassandraOperation::Batch(CassandraBatch {
                                ty,
                                queries,
                                consistency: body.query_params.consistency,
                                serial_consistency: body.query_params.serial_consistency,
                                timestamp: body.query_params.timestamp,
                            })
                        }
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
                                                        frame.version,
                                                        col_spec,
                                                        &row_content,
                                                    )
                                                })
                                                .collect()
                                        })
                                        .collect()
                                };
                            CassandraOperation::Result(CassandraResult::Rows {
                                rows: converted_rows,
                                metadata: Box::new(rows.metadata),
                            })
                        }
                        ResResultBody::SetKeyspace(set_keyspace) => CassandraOperation::Result(
                            CassandraResult::SetKeyspace(Box::new(set_keyspace)),
                        ),
                        ResResultBody::Prepared(prepared) => CassandraOperation::Result(
                            CassandraResult::Prepared(Box::new(prepared)),
                        ),
                        ResResultBody::SchemaChange(schema_change) => CassandraOperation::Result(
                            CassandraResult::SchemaChange(Box::new(schema_change)),
                        ),
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
            Opcode::Execute => {
                if let RequestBody::Execute(body) = frame.request_body()? {
                    CassandraOperation::Execute(Box::new(body))
                } else {
                    unreachable!("we already know this is an execute");
                }
            }
            Opcode::Register => CassandraOperation::Register(frame.body),
            Opcode::Event => {
                if let ResponseBody::Event(BodyResEvent { event }) = frame.response_body()? {
                    CassandraOperation::Event(event)
                } else {
                    unreachable!("we already know this is an event");
                }
            }
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
                                        BatchStatementType::Statement(Box::new(
                                            parse_statement_single(&query),
                                        ))
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
            CassandraOperation::Query { query, .. } => get_query_type(query),
            CassandraOperation::Batch { .. } => QueryType::Write,
            _ => QueryType::Read,
        }
    }

    pub fn encode(self) -> RawCassandraFrame {
        RawCassandraFrame {
            direction: self.operation.to_direction(),
            version: self.version,
            flags: Flags::default(),
            opcode: self.operation.to_opcode(),
            stream_id: self.stream_id,
            body: self.operation.into_body(self.version),
            tracing_id: self.tracing_id,
            warnings: self.warnings,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum CassandraOperation {
    Query {
        query: Box<CassandraStatement>,
        params: Box<QueryParams>,
    },
    Result(CassandraResult),
    Error(ErrorBody),
    Prepare(Vec<u8>),
    Execute(Box<BodyReqExecuteOwned>),
    Register(Vec<u8>),
    Event(ServerEvent),
    Batch(CassandraBatch),
    // operations for protocol negotiation, should be ignored by transforms
    Startup(Vec<u8>),
    Ready(Vec<u8>),
    Authenticate(Vec<u8>),
    Options(Vec<u8>),
    Supported(Vec<u8>),
    AuthChallenge(Vec<u8>),
    AuthResponse(Vec<u8>),
    AuthSuccess(Vec<u8>),
}

pub enum QueryIterator<'a> {
    Batch(std::iter::FilterMap<std::slice::IterMut<'a, BatchStatement>, FilterFn>),
    Query(std::iter::Once<&'a mut CassandraStatement>),
    None,
}

impl<'a> Iterator for QueryIterator<'a> {
    type Item = &'a mut CassandraStatement;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            QueryIterator::Batch(batch) => batch.next(),
            QueryIterator::Query(once) => once.next(),
            QueryIterator::None => None,
        }
    }
}

type FilterFn = fn(&mut BatchStatement) -> Option<&mut CassandraStatement>;

fn filter_batch_queries(batch: &mut BatchStatement) -> Option<&mut CassandraStatement> {
    match &mut batch.ty {
        BatchStatementType::Statement(cql) => Some(&mut *cql),
        BatchStatementType::PreparedId(_) => None,
    }
}

impl CassandraOperation {
    /// Return all queries contained within CassandaOperation::Query and CassandraOperation::Batch
    pub fn queries(&mut self) -> QueryIterator {
        match self {
            CassandraOperation::Query { query, .. } => QueryIterator::Query(std::iter::once(query)),
            CassandraOperation::Batch(batch) => {
                QueryIterator::Batch(batch.queries.iter_mut().filter_map(filter_batch_queries))
            }
            _ => QueryIterator::None,
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

    fn into_body(self, version: Version) -> Vec<u8> {
        match self {
            CassandraOperation::Query { query, params } => BodyReqQuery {
                query: query.to_string(),
                query_params: *params,
            }
            .serialize_to_vec(version),
            CassandraOperation::Result(result) => match result {
                CassandraResult::Rows { rows, metadata } => {
                    Self::build_cassandra_result_body(version, rows, *metadata)
                }
                CassandraResult::SetKeyspace(set_keyspace) => {
                    ResResultBody::SetKeyspace(*set_keyspace)
                }
                CassandraResult::Prepared(prepared) => ResResultBody::Prepared(*prepared),
                CassandraResult::SchemaChange(schema_change) => {
                    ResResultBody::SchemaChange(*schema_change)
                }
                CassandraResult::Void => ResResultBody::Void,
            }
            .serialize_to_vec(version),
            CassandraOperation::Error(error) => error.serialize_to_vec(version),
            CassandraOperation::Startup(bytes) => bytes.to_vec(),
            CassandraOperation::Ready(bytes) => bytes.to_vec(),
            CassandraOperation::Authenticate(bytes) => bytes.to_vec(),
            CassandraOperation::Options(bytes) => bytes.to_vec(),
            CassandraOperation::Supported(bytes) => bytes.to_vec(),
            CassandraOperation::Prepare(bytes) => bytes.to_vec(),
            CassandraOperation::Execute(execute) => execute.serialize_to_vec(version),
            CassandraOperation::Register(bytes) => bytes.to_vec(),
            CassandraOperation::Event(event) => event.serialize_to_vec(version),
            CassandraOperation::Batch(batch) => BodyReqBatch {
                batch_type: batch.ty,
                consistency: batch.consistency,
                keyspace: None,
                now_in_seconds: None,
                queries: batch
                    .queries
                    .into_iter()
                    .map(|query| BatchQuery {
                        subject: match query.ty {
                            BatchStatementType::PreparedId(id) => BatchQuerySubj::PreparedId(id),
                            BatchStatementType::Statement(statement) => {
                                BatchQuerySubj::QueryString(statement.to_string())
                            }
                        },
                        values: query.values,
                    })
                    .collect(),
                serial_consistency: batch.serial_consistency,
                timestamp: batch.timestamp,
            }
            .serialize_to_vec(version),
            CassandraOperation::AuthChallenge(bytes) => bytes.to_vec(),
            CassandraOperation::AuthResponse(bytes) => bytes.to_vec(),
            CassandraOperation::AuthSuccess(bytes) => bytes.to_vec(),
        }
    }

    fn build_cassandra_result_body(
        protocol_version: Version,
        rows: Vec<Vec<MessageValue>>,
        metadata: RowsMetadata,
    ) -> ResResultBody {
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

        ResResultBody::Rows(BodyResResultRows {
            protocol_version,
            metadata,
            rows_count,
            rows_content,
        })
    }
}

fn get_query_type(statement: &CassandraStatement) -> QueryType {
    match statement {
        CassandraStatement::AlterKeyspace(_) => QueryType::SchemaChange,
        CassandraStatement::AlterMaterializedView(_) => QueryType::SchemaChange,
        CassandraStatement::AlterRole(_) => QueryType::SchemaChange,
        CassandraStatement::AlterTable(_) => QueryType::SchemaChange,
        CassandraStatement::AlterType(_) => QueryType::SchemaChange,
        CassandraStatement::AlterUser(_) => QueryType::SchemaChange,
        CassandraStatement::ApplyBatch => QueryType::ReadWrite,
        CassandraStatement::CreateAggregate(_) => QueryType::SchemaChange,
        CassandraStatement::CreateFunction(_) => QueryType::SchemaChange,
        CassandraStatement::CreateIndex(_) => QueryType::SchemaChange,
        CassandraStatement::CreateKeyspace(_) => QueryType::SchemaChange,
        CassandraStatement::CreateMaterializedView(_) => QueryType::SchemaChange,
        CassandraStatement::CreateRole(_) => QueryType::SchemaChange,
        CassandraStatement::CreateTable(_) => QueryType::SchemaChange,
        CassandraStatement::CreateTrigger(_) => QueryType::SchemaChange,
        CassandraStatement::CreateType(_) => QueryType::SchemaChange,
        CassandraStatement::CreateUser(_) => QueryType::SchemaChange,
        CassandraStatement::Delete(_) => QueryType::Write,
        CassandraStatement::DropAggregate(_) => QueryType::SchemaChange,
        CassandraStatement::DropFunction(_) => QueryType::SchemaChange,
        CassandraStatement::DropIndex(_) => QueryType::SchemaChange,
        CassandraStatement::DropKeyspace(_) => QueryType::SchemaChange,
        CassandraStatement::DropMaterializedView(_) => QueryType::SchemaChange,
        CassandraStatement::DropRole(_) => QueryType::SchemaChange,
        CassandraStatement::DropTable(_) => QueryType::SchemaChange,
        CassandraStatement::DropTrigger(_) => QueryType::SchemaChange,
        CassandraStatement::DropType(_) => QueryType::SchemaChange,
        CassandraStatement::DropUser(_) => QueryType::SchemaChange,
        CassandraStatement::Grant(_) => QueryType::SchemaChange,
        CassandraStatement::Insert(_) => QueryType::Write,
        CassandraStatement::ListPermissions(_) => QueryType::Read,
        CassandraStatement::ListRoles(_) => QueryType::Read,
        CassandraStatement::Revoke(_) => QueryType::SchemaChange,
        CassandraStatement::Select(_) => QueryType::Read,
        CassandraStatement::Truncate(_) => QueryType::Write,
        CassandraStatement::Update(_) => QueryType::Write,
        CassandraStatement::Use(_) => QueryType::SchemaChange,
        CassandraStatement::Unknown(_) => QueryType::Read,
    }
}

pub enum StatementResult {
    Query(Box<CassandraStatement>),
    /// Since this is already specified as a batch, CassandraStatement batch values must not be used in the Vec.
    /// Specifically CassandraStatement::ApplyBatch and begin_batch fields must not be used.
    Batch(Vec<CassandraStatement>, BeginBatch),
}

/// Will parse both:
/// * a single statement
/// * a single BATCH statement containing multiple statements
pub fn parse_statement_query(cql: &str) -> StatementResult {
    let mut ast = CassandraAST::new(cql);

    if ast.has_error() || ast.statements.is_empty() {
        StatementResult::Query(Box::new(CassandraStatement::Unknown(cql.to_string())))
    } else if ast.statements.len() > 1 {
        let begin_batch = match &mut ast.statements[0].statement {
            CassandraStatement::Update(x) => x.begin_batch.take(),
            CassandraStatement::Insert(x) => x.begin_batch.take(),
            CassandraStatement::Delete(x) => x.begin_batch.take(),
            _ => None,
        };

        if let Some(begin_batch) = begin_batch {
            StatementResult::Batch(
                ast.statements
                    .into_iter()
                    .map(|x| x.statement)
                    .filter(|x| !matches!(x, CassandraStatement::ApplyBatch))
                    .collect(),
                begin_batch,
            )
        } else {
            // A batch statement with no `BEGIN BATCH`.
            // The parser will accept this but we should reject it because its not a valid statement.
            StatementResult::Query(Box::new(CassandraStatement::Unknown(cql.to_string())))
        }
    } else {
        StatementResult::Query(Box::new(ast.statements.remove(0).statement))
    }
}

/// Will only parse a single statement
/// BATCH statements are rejected
pub fn parse_statement_single(cql: &str) -> CassandraStatement {
    let mut ast = CassandraAST::new(cql);

    if ast.has_error() || ast.statements.len() != 1 {
        CassandraStatement::Unknown(cql.to_string())
    } else {
        ast.statements.remove(0).statement
    }
}

fn from_string_value(value: &str) -> CassandraType {
    if value.starts_with('\'') || value.starts_with("$$") {
        CassandraType::Varchar(Operand::unescape(value))
    } else if value.starts_with("0X") || value.starts_with("0x") {
        hex::decode(&value[2..])
            .map(|x| CassandraType::Blob(Blob::from(x)))
            .unwrap_or(CassandraType::Null)
    } else if let Ok(n) = i64::from_str(value) {
        CassandraType::Bigint(n)
    } else if let Ok(n) = f64::from_str(value) {
        CassandraType::Double(n)
    } else if let Ok(uuid) = Uuid::parse_str(value) {
        CassandraType::Uuid(uuid)
    } else if let Ok(ipaddr) = IpAddr::from_str(value) {
        CassandraType::Inet(ipaddr)
    } else {
        CassandraType::Null
    }
}

// TODO: bubble up error instead of panicking
pub fn to_cassandra_type(operand: &Operand) -> CassandraType {
    match operand {
        Operand::Const(value) => from_string_value(value),
        Operand::Map(values) => {
            let mapping = values
                .iter()
                .map(|(key, value)| (from_string_value(key), from_string_value(value)))
                .collect();
            CassandraType::Map(mapping)
        }
        Operand::Set(values) => CassandraType::Set(
            values
                .iter()
                .map(|value| from_string_value(value))
                .collect(),
        ),
        Operand::List(values) => CassandraType::List(
            values
                .iter()
                .map(|value| from_string_value(value))
                .collect(),
        ),
        Operand::Tuple(values) => {
            CassandraType::Tuple(values.iter().map(to_cassandra_type).collect())
        }
        Operand::Column(_) => todo!(),
        Operand::Func(_) => todo!(),
        Operand::Null => CassandraType::Null,
        Operand::Param(_) => todo!(),
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum CassandraResult {
    // values are boxed so that Void takes minimal stack space
    Rows {
        rows: Vec<Vec<MessageValue>>,
        metadata: Box<RowsMetadata>,
    },
    SetKeyspace(Box<BodyResResultSetKeyspace>),
    Prepared(Box<BodyResResultPrepared>),
    SchemaChange(Box<SchemaChange>),
    Void,
}

#[derive(PartialEq, Debug, Clone)]
pub enum BatchStatementType {
    Statement(Box<CassandraStatement>),
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

#[cfg(test)]
mod test {
    use crate::frame::cassandra::{parse_statement_single, to_cassandra_type};
    use cassandra_protocol::types::cassandra_type::CassandraType;
    use cassandra_protocol::types::prelude::Blob;
    use cql3_parser::cassandra_statement::CassandraStatement;
    use cql3_parser::common::{FQName, Identifier, Operand, RelationElement, RelationOperator};
    use cql3_parser::insert::{Insert, InsertValues};
    use cql3_parser::select::{Select, SelectElement};
    use std::net::IpAddr;
    use std::str::FromStr;
    use uuid::Uuid;

    #[test]
    fn cql_insert() {
        let query = r#"INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (1, 11, 'foo')"#;
        let cql = parse_statement_single(query);

        let intermediate = CassandraStatement::Insert(Insert {
            begin_batch: None,
            table_name: FQName {
                keyspace: Some(Identifier::Unquoted(
                    "test_cache_keyspace_batch_insert".to_string(),
                )),
                name: Identifier::Unquoted("test_table".to_string()),
            },
            columns: vec![
                Identifier::Unquoted("id".to_string()),
                Identifier::Unquoted("x".to_string()),
                Identifier::Unquoted("name".to_string()),
            ],
            values: InsertValues::Values(vec![
                Operand::Const("1".to_string()),
                Operand::Const("11".to_string()),
                Operand::Const("'foo'".to_string()),
            ]),
            using_ttl: None,
            if_not_exists: false,
        });
        assert_eq!(cql, intermediate);
        assert_eq!(cql.to_string(), query)
    }

    #[test]
    fn cql_select() {
        let query = r#"SELECT * FROM foo WHERE bar ( baz ) = 1"#;
        let cql = parse_statement_single(query);

        let intermediate = CassandraStatement::Select(Select {
            table_name: FQName {
                keyspace: None,
                name: Identifier::Unquoted("foo".to_string()),
            },
            columns: vec![SelectElement::Star],
            distinct: false,
            filtering: false,
            json: false,
            limit: None,
            order: None,
            where_clause: vec![RelationElement {
                obj: Operand::Func("bar ( baz )".to_string()),
                oper: RelationOperator::Equal,
                value: Operand::Const("1".to_string()),
            }],
        });
        assert_eq!(cql, intermediate);
        assert_eq!(cql.to_string(), query)
    }

    #[test]
    fn cql_batch() {
        // TODO: this test demonstrates a valid BATCH case but currently shotover does not support it and instead returns an Unknown.
        // I believe the correct fix here would be https://github.com/shotover/shotover-proxy/issues/575

        assert_unknown(
            r#"BEGIN BATCH
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (1, 11, 'foo');
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (2, 12, 'bar');
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (3, 13, 'baz');
            APPLY BATCH;"#,
        );
    }

    fn assert_unknown(query: &str) {
        assert_eq!(
            parse_statement_single(query),
            CassandraStatement::Unknown(query.to_string())
        );
    }

    #[test]
    fn cql_unknown_statement() {
        assert_unknown("");
        assert_unknown("     ");
        assert_unknown("INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name)  (2, 12, 'bar');");
        assert_unknown(
            r#"BEGIN BATCH INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (3, 13, 'baz');
            INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name)  (2, 12, 'bar');
            EXECUTE BATCH"#,
        );
        assert_unknown(
            r#"INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (3, 13, 'baz');
            INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name)  (2, 12, 'bar');"#,
        );
        assert_unknown(
            r#"INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (1, 11, 'foo');
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (2, 12, 'bar');
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (3, 13, 'baz');"#,
        );
    }

    #[test]
    pub fn test_to_cassandra_type_for_const_operand() {
        assert_eq!(
            CassandraType::Bigint(55),
            to_cassandra_type(&Operand::Const("55".to_string()))
        );
        assert_eq!(
            CassandraType::Double(5.5),
            to_cassandra_type(&Operand::Const("5.5".to_string()))
        );
        assert_eq!(
            CassandraType::Uuid(Uuid::parse_str("123e4567-e89b-12d3-a456-426655440000").unwrap()),
            to_cassandra_type(&Operand::Const(
                "123e4567-e89b-12d3-a456-426655440000".to_string()
            ))
        );
        assert_eq!(
            CassandraType::Inet(IpAddr::from_str("192.168.0.1").unwrap()),
            to_cassandra_type(&Operand::Const("192.168.0.1".to_string()))
        );
        assert_eq!(
            CassandraType::Inet(
                IpAddr::from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap()
            ),
            to_cassandra_type(&Operand::Const(
                "2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_string()
            ))
        );
        assert_eq!(
            CassandraType::Blob(Blob::from(vec![255_u8, 234_u8, 1_u8, 13_u8])),
            to_cassandra_type(&Operand::Const("0xFFEA010D".to_string()))
        );
        let tests = [
            (
                "'Women''s Tour of New Zealand'",
                "Women's Tour of New Zealand",
            ),
            (
                "$$Women's Tour of New Zealand$$",
                "Women's Tour of New Zealand",
            ),
            (
                "$$Women''s Tour of New Zealand$$",
                "Women''s Tour of New Zealand",
            ),
        ];
        for (txt, expected) in tests {
            assert_eq!(
                CassandraType::Varchar(expected.to_string()),
                to_cassandra_type(&Operand::Const(txt.to_string()))
            );
        }
        assert_eq!(
            CassandraType::Null,
            to_cassandra_type(&Operand::Const("not a valid const".to_string()))
        );
        assert_eq!(
            CassandraType::Null,
            to_cassandra_type(&Operand::Const("0xnot a hex".to_string()))
        );
    }

    #[test]
    pub fn test_to_cassandra_type_for_string_collection_operands() {
        let args = vec![
            "55".to_string(),
            "5.5".to_string(),
            "123e4567-e89b-12d3-a456-426655440000".to_string(),
            "192.168.0.1".to_string(),
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_string(),
            "0xFFEA010D".to_string(),
            "'Women''s Tour of New Zealand'".to_string(),
            "$$Women's Tour of New Zealand$$".to_string(),
            "$$Women''s Tour of New Zealand$$".to_string(),
            "invalid text".to_string(),
            "0xinvalid hex".to_string(),
        ];

        let expected = vec![
            CassandraType::Bigint(55),
            CassandraType::Double(5.5),
            CassandraType::Uuid(Uuid::parse_str("123e4567-e89b-12d3-a456-426655440000").unwrap()),
            CassandraType::Inet(IpAddr::from_str("192.168.0.1").unwrap()),
            CassandraType::Inet(
                IpAddr::from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap(),
            ),
            CassandraType::Blob(Blob::from(vec![255_u8, 234_u8, 1_u8, 13_u8])),
            CassandraType::Varchar("Women's Tour of New Zealand".to_string()),
            CassandraType::Varchar("Women's Tour of New Zealand".to_string()),
            CassandraType::Varchar("Women''s Tour of New Zealand".to_string()),
            CassandraType::Null,
            CassandraType::Null,
        ];

        assert_eq!(
            CassandraType::List(expected.clone()),
            to_cassandra_type(&Operand::List(args.clone()))
        );
        assert_eq!(
            CassandraType::Set(expected),
            to_cassandra_type(&Operand::Set(args))
        );
    }

    #[test]
    pub fn test_to_cassandra_type_for_map_operand() {
        let args = vec![
            ("1".to_string(), "55".to_string()),
            ("2".to_string(), "5.5".to_string()),
            (
                "3".to_string(),
                "123e4567-e89b-12d3-a456-426655440000".to_string(),
            ),
            ("4".to_string(), "192.168.0.1".to_string()),
            (
                "5".to_string(),
                "2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_string(),
            ),
            ("6".to_string(), "0xFFEA010D".to_string()),
            (
                "7".to_string(),
                "'Women''s Tour of New Zealand'".to_string(),
            ),
            (
                "8".to_string(),
                "$$Women's Tour of New Zealand$$".to_string(),
            ),
            (
                "9".to_string(),
                "$$Women''s Tour of New Zealand$$".to_string(),
            ),
            ("'A'".to_string(), "invalid text".to_string()),
            ("'B'".to_string(), "0xinvalid hex".to_string()),
        ];
        let expected = vec![
            (CassandraType::Bigint(1), CassandraType::Bigint(55)),
            (CassandraType::Bigint(2), CassandraType::Double(5.5)),
            (
                CassandraType::Bigint(3),
                CassandraType::Uuid(
                    Uuid::parse_str("123e4567-e89b-12d3-a456-426655440000").unwrap(),
                ),
            ),
            (
                CassandraType::Bigint(4),
                CassandraType::Inet(IpAddr::from_str("192.168.0.1").unwrap()),
            ),
            (
                CassandraType::Bigint(5),
                CassandraType::Inet(
                    IpAddr::from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap(),
                ),
            ),
            (
                CassandraType::Bigint(6),
                CassandraType::Blob(Blob::from(vec![255_u8, 234_u8, 1_u8, 13_u8])),
            ),
            (
                CassandraType::Bigint(7),
                CassandraType::Varchar("Women's Tour of New Zealand".to_string()),
            ),
            (
                CassandraType::Bigint(8),
                CassandraType::Varchar("Women's Tour of New Zealand".to_string()),
            ),
            (
                CassandraType::Bigint(9),
                CassandraType::Varchar("Women''s Tour of New Zealand".to_string()),
            ),
            (CassandraType::Varchar("A".to_string()), CassandraType::Null),
            (CassandraType::Varchar("B".to_string()), CassandraType::Null),
        ];

        assert_eq!(
            CassandraType::Map(expected),
            to_cassandra_type(&Operand::Map(args))
        )
    }

    #[test]
    pub fn test_to_cassandra_type_for_tuple_operand() {
        let args = vec![
            Operand::Const("55".to_string()),
            Operand::Const("5.5".to_string()),
            Operand::Const("123e4567-e89b-12d3-a456-426655440000".to_string()),
            Operand::Const("192.168.0.1".to_string()),
            Operand::Const("2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_string()),
            Operand::Const("0xFFEA010D".to_string()),
            Operand::Const("'Women''s Tour of New Zealand'".to_string()),
            Operand::Const("$$Women's Tour of New Zealand$$".to_string()),
            Operand::Const("$$Women''s Tour of New Zealand$$".to_string()),
            Operand::Const("invalid text".to_string()),
            Operand::Const("0xinvalid hex".to_string()),
        ];

        let expected = vec![
            CassandraType::Bigint(55),
            CassandraType::Double(5.5),
            CassandraType::Uuid(Uuid::parse_str("123e4567-e89b-12d3-a456-426655440000").unwrap()),
            CassandraType::Inet(IpAddr::from_str("192.168.0.1").unwrap()),
            CassandraType::Inet(
                IpAddr::from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap(),
            ),
            CassandraType::Blob(Blob::from(vec![255_u8, 234_u8, 1_u8, 13_u8])),
            CassandraType::Varchar("Women's Tour of New Zealand".to_string()),
            CassandraType::Varchar("Women's Tour of New Zealand".to_string()),
            CassandraType::Varchar("Women''s Tour of New Zealand".to_string()),
            CassandraType::Null,
            CassandraType::Null,
        ];

        assert_eq!(
            CassandraType::Tuple(expected),
            to_cassandra_type(&Operand::Tuple(args))
        );
    }

    #[test]
    pub fn test_to_cassandra_type_for_misc_operands() {
        assert_eq!(CassandraType::Null, to_cassandra_type(&Operand::Null));
    }
}
