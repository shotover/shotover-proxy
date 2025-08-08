use crate::frame::value::GenericValue;
use crate::frame::value::cassandra::{serialize_len, serialize_with_length_prefix};
use crate::message::QueryType;
use anyhow::{Result, anyhow};
use bytes::Bytes;
use cassandra_protocol::compression::Compression;
use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::events::SchemaChange;
use cassandra_protocol::frame::events::ServerEvent;
use cassandra_protocol::frame::message_batch::{
    BatchQuery, BatchQuerySubj, BatchType, BodyReqBatch,
};
use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType};
use cassandra_protocol::frame::message_event::BodyResEvent;
use cassandra_protocol::frame::message_execute::BodyReqExecuteOwned;
use cassandra_protocol::frame::message_query::BodyReqQuery;
use cassandra_protocol::frame::message_register::BodyReqRegister;
use cassandra_protocol::frame::message_request::RequestBody;
use cassandra_protocol::frame::message_response::ResponseBody;
use cassandra_protocol::frame::message_result::{
    BodyResResultPrepared, BodyResResultSetKeyspace, ColSpec, ColTypeOption, ResResultBody,
    ResultKind, RowsMetadata, RowsMetadataFlags,
};
use cassandra_protocol::frame::message_startup::BodyReqStartup;
use cassandra_protocol::frame::message_supported::BodyResSupported;
use cassandra_protocol::frame::{
    Direction, Envelope as RawCassandraFrame, Flags, Opcode, Serialize, StreamId, Version,
};
use cassandra_protocol::query::{QueryParams, QueryValues};
use cassandra_protocol::types::blob::Blob;
use cassandra_protocol::types::cassandra_type::CassandraType;
use cassandra_protocol::types::{CBytesShort, CLong};
use cql3_parser::begin_batch::{BatchType as ParserBatchType, BeginBatch};
use cql3_parser::cassandra_ast::CassandraAST;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::Operand;
use nonzero_ext::nonzero;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::io::{Cursor, Write};
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::str::FromStr;
use uuid::Uuid;

/// Functions for operations on an unparsed Cassandra frame
pub mod raw_frame {
    use super::{CassandraMetadata, RawCassandraFrame};
    use anyhow::{Result, anyhow, bail};
    use cassandra_protocol::frame::Version;
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
        if bytes.len() < 9 {
            return Err(anyhow!("Not enough bytes for cassandra frame"));
        }
        Ok(CassandraMetadata {
            version: Version::try_from(bytes[0])?,
            stream_id: i16::from_be_bytes(bytes[2..4].try_into()?),
            opcode: Opcode::try_from(bytes[4])?,
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
}

/// Only includes data within the header
/// Data within the body may require decompression which is too expensive
pub struct CassandraMetadata {
    pub version: Version,
    pub stream_id: StreamId,
    pub opcode: Opcode,
}

impl CassandraMetadata {
    pub fn backpressure_response(&self) -> CassandraFrame {
        let body = CassandraOperation::Error(ErrorBody {
            message: "Server overloaded".into(),
            ty: ErrorType::Overloaded,
        });

        CassandraFrame {
            version: self.version,
            stream_id: self.stream_id,
            tracing: Tracing::Response(None),
            warnings: vec![],
            operation: body,
        }
    }

    pub fn to_error_response(&self, error: String) -> CassandraFrame {
        CassandraFrame {
            version: self.version,
            stream_id: self.stream_id,
            operation: CassandraOperation::Error(ErrorBody {
                message: error,
                ty: ErrorType::Server,
            }),
            tracing: Tracing::Response(None),
            warnings: vec![],
        }
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum Tracing {
    Request(bool),
    Response(Option<Uuid>),
}

impl Tracing {
    fn enabled(&self) -> bool {
        match self {
            Self::Request(enabled) => *enabled,
            Self::Response(uuid) => uuid.is_some(),
        }
    }
}

impl From<Tracing> for Option<Uuid> {
    fn from(tracing: Tracing) -> Self {
        match tracing {
            Tracing::Request(_) => None,
            Tracing::Response(uuid) => uuid,
        }
    }
}

impl Tracing {
    fn from_frame(frame: &RawCassandraFrame) -> Self {
        match frame.direction {
            Direction::Request => Self::Request(frame.flags.contains(Flags::TRACING)),
            Direction::Response => Self::Response(frame.tracing_id),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct CassandraFrame {
    pub version: Version,
    pub stream_id: StreamId,
    pub tracing: Tracing,
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
            opcode: self.operation.to_opcode(),
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

    pub fn shotover_error(stream_id: i16, version: Version, message: &str) -> Self {
        CassandraFrame {
            version,
            stream_id,
            operation: CassandraOperation::Error(ErrorBody {
                message: format!("Internal shotover error: {message}"),
                ty: ErrorType::Server,
            }),
            tracing: Tracing::Response(None),
            warnings: vec![],
        }
    }

    pub fn from_bytes(bytes: Bytes, compression: Compression) -> Result<Self> {
        let frame = RawCassandraFrame::from_buffer(&bytes, compression)
            .map_err(|e| anyhow!("{e:?}"))?
            .envelope;

        let tracing = Tracing::from_frame(&frame);
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
                                                .map(|row_content| match row_content.into_bytes() {
                                                    None => GenericValue::Null,
                                                    Some(value) => {
                                                        GenericValue::Bytes(value.into())
                                                    }
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
                                                    GenericValue::build_value_from_cstar_col_type(
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
                        _ => unreachable!(),
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
            Opcode::Startup => {
                if let RequestBody::Startup(body) = frame.request_body()? {
                    CassandraOperation::Startup(body)
                } else {
                    unreachable!("We already know the operation is a startup")
                }
            }
            Opcode::Ready => CassandraOperation::Ready(frame.body),
            Opcode::Authenticate => CassandraOperation::Authenticate(frame.body),
            Opcode::Options => CassandraOperation::Options(frame.body),
            Opcode::Supported => {
                if let ResponseBody::Supported(body) = frame.response_body()? {
                    CassandraOperation::Supported(body)
                } else {
                    unreachable!("we already know this is a supported");
                }
            }
            Opcode::Prepare => CassandraOperation::Prepare(frame.body),
            Opcode::Execute => {
                if let RequestBody::Execute(body) = frame.request_body()? {
                    CassandraOperation::Execute(Box::new(body))
                } else {
                    unreachable!("we already know this is an execute");
                }
            }
            Opcode::Register => {
                if let RequestBody::Register(register) = frame.request_body()? {
                    CassandraOperation::Register(register)
                } else {
                    unreachable!("we already know this is a register");
                }
            }
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
            _ => unreachable!(),
        };

        Ok(CassandraFrame {
            version: frame.version,
            stream_id: frame.stream_id,
            tracing,
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

    pub fn encode(self, compression: Compression) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        let mut cursor = Cursor::new(&mut buf);

        let combined_version_byte =
            u8::from(self.version) | u8::from(self.operation.to_direction());

        let mut flags = Flags::default();
        flags.set(Flags::COMPRESSION, compression != Compression::None);
        flags.set(Flags::WARNING, !self.warnings.is_empty());
        flags.set(Flags::TRACING, self.tracing.enabled());

        cursor.write_all(&[combined_version_byte]).ok();
        cursor.write_all(&[flags.bits()]).ok();
        cursor.write_all(&self.stream_id.to_be_bytes()).ok();
        cursor.write_all(&[self.operation.to_opcode().into()]).ok();

        serialize_with_length_prefix(&mut cursor, |cursor| {
            // Special case None to avoid large copies
            if Compression::None == compression {
                self.write_tracing_and_warnings(cursor);

                self.operation.serialize(cursor, self.version)
            } else {
                // TODO: While compression is obviously going to cost more than no compression, I suspect it doesnt have to be quite this bad
                let mut body_buf = Vec::with_capacity(128);
                let mut body_cursor = Cursor::new(&mut body_buf);

                self.write_tracing_and_warnings(&mut body_cursor);

                self.operation.serialize(&mut body_cursor, self.version);
                cursor
                    .write_all(&compression.encode(&body_buf).unwrap())
                    .ok();
            }
        });

        buf
    }

    fn write_tracing_and_warnings(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        if let Tracing::Response(Some(uuid)) = self.tracing {
            cursor.write_all(uuid.as_bytes()).ok();
        }

        if !self.warnings.is_empty() {
            let warnings_len = self.warnings.len() as i16;
            cursor.write_all(&warnings_len.to_be_bytes()).ok();

            for warning in &self.warnings {
                let warning_len = warning.len() as i16;
                cursor.write_all(&warning_len.to_be_bytes()).ok();
                cursor.write_all(warning.as_bytes()).ok();
            }
        }
    }
}
pub(crate) fn operation_name(operation: &CassandraOperation) -> &'static str {
    match operation {
        CassandraOperation::Query { .. } => "Query",
        CassandraOperation::Result(_) => "Result",
        CassandraOperation::Error(_) => "Error",
        CassandraOperation::Prepare(_) => "Prepare",
        CassandraOperation::Execute(_) => "Execute",
        CassandraOperation::Register(_) => "Register",
        CassandraOperation::Event(_) => "Event",
        CassandraOperation::Batch(_) => "Batch",
        CassandraOperation::Startup(_) => "Startup",
        CassandraOperation::Ready(_) => "Ready",
        CassandraOperation::Authenticate(_) => "Authenticate",
        CassandraOperation::Options(_) => "Options",
        CassandraOperation::Supported(_) => "Supported",
        CassandraOperation::AuthChallenge(_) => "AuthChallenge",
        CassandraOperation::AuthResponse(_) => "AuthResponse",
        CassandraOperation::AuthSuccess(_) => "AuthSuccess",
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
    Register(BodyReqRegister),
    Event(ServerEvent),
    Batch(CassandraBatch),
    // operations for protocol negotiation, should be ignored by transforms
    Startup(BodyReqStartup),
    Ready(Vec<u8>),
    Authenticate(Vec<u8>),
    Options(Vec<u8>),
    Supported(BodyResSupported),
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
    pub fn queries(&mut self) -> QueryIterator<'_> {
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
            CassandraOperation::Result { .. } => Direction::Response,
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

    fn serialize(self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        match self {
            CassandraOperation::Query { query, params } => BodyReqQuery {
                query: query.to_string(),
                query_params: *params,
            }
            .serialize(cursor, version),
            CassandraOperation::Result(result) => match result {
                CassandraResult::Rows { rows, metadata } => {
                    ResultKind::Rows.serialize(cursor, version);

                    metadata.serialize(cursor, version);
                    serialize_len(cursor, rows.len());
                    for row in rows {
                        for col in row {
                            col.cassandra_serialize(cursor);
                        }
                    }
                }
                CassandraResult::SetKeyspace(set_keyspace) => {
                    ResResultBody::SetKeyspace(*set_keyspace).serialize(cursor, version)
                }
                CassandraResult::Prepared(prepared) => {
                    ResResultBody::Prepared(*prepared).serialize(cursor, version)
                }
                CassandraResult::SchemaChange(schema_change) => {
                    ResResultBody::SchemaChange(*schema_change).serialize(cursor, version)
                }
                CassandraResult::Void => ResResultBody::Void.serialize(cursor, version),
            },
            CassandraOperation::Error(error) => error.serialize(cursor, version),
            CassandraOperation::Startup(bytes) => bytes.serialize(cursor, version),
            CassandraOperation::Ready(bytes) => bytes.serialize(cursor, version),
            CassandraOperation::Authenticate(bytes) => bytes.serialize(cursor, version),
            CassandraOperation::Options(bytes) => bytes.serialize(cursor, version),
            CassandraOperation::Supported(bytes) => bytes.serialize(cursor, version),
            CassandraOperation::Prepare(bytes) => bytes.serialize(cursor, version),
            CassandraOperation::Execute(execute) => execute.serialize(cursor, version),
            CassandraOperation::Register(register) => register.serialize(cursor, version),
            CassandraOperation::Event(event) => event.serialize(cursor, version),
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
            .serialize(cursor, version),
            CassandraOperation::AuthChallenge(bytes) => bytes.serialize(cursor, version),
            CassandraOperation::AuthResponse(bytes) => bytes.serialize(cursor, version),
            CassandraOperation::AuthSuccess(bytes) => bytes.serialize(cursor, version),
        }
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
        rows: Vec<Vec<GenericValue>>,
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

impl Display for CassandraFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{} stream:{}", self.version, self.stream_id)?;

        match self.tracing {
            Tracing::Request(request) => {
                if request {
                    write!(f, " request_tracing_id:{}", request)?;
                }
            }
            Tracing::Response(response) => {
                if let Some(tracing_id) = response {
                    write!(f, " tracing_id:{}", tracing_id)?;
                }
            }
        }

        if !self.warnings.is_empty() {
            write!(f, " warnings:{:?}", self.warnings)?;
        }
        match &self.operation {
            CassandraOperation::Query { query, params } => {
                let QueryParams {
                    consistency,
                    with_names,
                    values,
                    page_size,
                    paging_state,
                    serial_consistency,
                    timestamp,
                    keyspace,
                    now_in_seconds,
                } = params.as_ref();

                write!(
                    f,
                    " Query consistency:{} with_names:{:?}",
                    consistency, with_names,
                )?;

                if let Some(values) = values {
                    write!(f, " values:{:?}", values)?;
                }
                if let Some(page_size) = page_size {
                    write!(f, " page_size:{:?}", page_size)?;
                }
                if let Some(paging_state) = paging_state {
                    write!(f, " paging_state:{:?}", paging_state)?;
                }
                if let Some(serial_consistency) = serial_consistency {
                    write!(f, " serial_consistency:{:?}", serial_consistency)?;
                }
                if let Some(timestamp) = timestamp {
                    write!(f, " timestamp:{:?}", timestamp)?;
                }
                if let Some(keyspace) = keyspace {
                    write!(f, " keyspace:{:?}", keyspace)?;
                }
                if let Some(now_in_seconds) = now_in_seconds {
                    write!(f, " now_in_seconds:{:?}", now_in_seconds)?;
                }
                write!(f, " {}", query)
            }
            CassandraOperation::Register(BodyReqRegister { events }) => {
                write!(f, " Register {:?}", events)
            }
            CassandraOperation::Error(ErrorBody { message, ty }) => {
                write!(f, " Error {:?} {:?}", ty, message)
            }
            CassandraOperation::Result(result) => match result {
                CassandraResult::Rows { rows, metadata } => {
                    let RowsMetadata {
                        flags,
                        columns_count,
                        paging_state,
                        new_metadata_id,
                        global_table_spec,
                        col_specs,
                    } = metadata.as_ref();

                    write!(
                        f,
                        " Result Rows {:?} columns_count:{}",
                        flags, columns_count,
                    )?;
                    if let Some(paging_state) = paging_state {
                        write!(f, " paging_state:{:?}", paging_state)?;
                    }
                    if let Some(new_metadata_id) = new_metadata_id {
                        write!(f, " new_metadata_id:{:?}", new_metadata_id)?;
                    }
                    if let Some(global_table_spec) = global_table_spec {
                        write!(
                            f,
                            " global_name:{}.{}",
                            global_table_spec.ks_name, global_table_spec.table_name
                        )?;
                    }
                    write!(f, " cols:[")?;
                    let mut need_comma = false;
                    for col_spec in col_specs {
                        let ColSpec {
                            table_spec,
                            name,
                            col_type,
                        } = col_spec;

                        let ColTypeOption { id, value } = col_type;

                        if need_comma {
                            write!(f, ", ")?;
                        }
                        need_comma = true;
                        write!(f, "{}:{:?}", name, id)?;
                        if let Some(value) = value {
                            write!(f, " of {:?}", value)?;
                        }
                        if let Some(table_spec) = table_spec {
                            write!(f, " table_spec:{:?}", table_spec)?;
                        }
                    }
                    write!(f, "]")?;
                    for row in rows {
                        write!(f, "\n    {:?}", row)?;
                    }
                    Ok(())
                }
                CassandraResult::Void => write!(f, " Result Void"),
                _ => write!(f, " Result {:?}", result),
            },
            CassandraOperation::Ready(_) => write!(f, " Ready"),
            _ => write!(f, " {:?}", self.operation),
        }
    }
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
    use pretty_assertions::assert_eq;
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
        assert_unknown(
            "INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name)  (2, 12, 'bar');",
        );
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
