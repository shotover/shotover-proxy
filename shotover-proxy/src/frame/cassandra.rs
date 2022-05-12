use crate::message::{MessageValue, QueryType};
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
use cassandra_protocol::types::blob::Blob;
use cassandra_protocol::types::cassandra_type::CassandraType;
use cassandra_protocol::types::{CBytes, CBytesShort, CInt, CLong};
use cql3_parser::cassandra_ast::CassandraAST;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{FQName, Operand, RelationElement};
use cql3_parser::insert::InsertValues;
use cql3_parser::update::AssignmentOperator;
use itertools::Itertools;
use nonzero_ext::nonzero;
use sodiumoxide::hex;
use std::io::Cursor;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::str::FromStr;
use tracing::debug;
use uuid::Uuid;

/// Functions for operations on an unparsed Cassandra frame
pub mod raw_frame {
    use super::{CassandraMetadata, RawCassandraFrame};
    use anyhow::{anyhow, Result};
    use cassandra_protocol::{compression::Compression, frame::Opcode};
    use nonzero_ext::nonzero;
    use std::convert::TryInto;
    use std::num::NonZeroU32;

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

    pub(crate) fn get_opcode(bytes: &[u8]) -> Result<Opcode> {
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
            .frame;
        let operation = match frame.opcode {
            Opcode::Query => {
                if let RequestBody::Query(body) = frame.request_body()? {
                    CassandraOperation::Query {
                        query: CQL::parse_from_string(&body.query),
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
                                        BatchStatementType::Statement(CQL::parse_from_string(
                                            &query,
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

    /// returns the query type for the current statement.
    /// Query type is calculated by scanning the query types of the enclosed statements with the
    /// highest valued result being returned.
    ///
    /// Statements, in descending order, are:
    ///
    /// * ReadWrite
    /// * Write
    /// * Read
    /// * SchemaChange
    /// * PubSubMessage
    pub fn get_query_type(&self) -> QueryType {
        match &self.operation {
            CassandraOperation::Query { query: cql, .. } => {
                // set to lowest type
                let mut result = QueryType::PubSubMessage;
                for cql_statement in &cql.statements {
                    result = match (CQLStatement::get_query_type(cql_statement), &result) {
                        (QueryType::ReadWrite, _) => QueryType::ReadWrite,
                        (QueryType::Write, QueryType::Read | QueryType::ReadWrite) => {
                            QueryType::ReadWrite
                        }
                        (QueryType::Write, QueryType::SchemaChange | QueryType::PubSubMessage) => {
                            QueryType::Write
                        }
                        (QueryType::Read, QueryType::ReadWrite | QueryType::Write) => {
                            QueryType::ReadWrite
                        }
                        (QueryType::Read, QueryType::SchemaChange | QueryType::PubSubMessage) => {
                            QueryType::Read
                        }
                        (QueryType::SchemaChange, QueryType::PubSubMessage) => {
                            QueryType::SchemaChange
                        }
                        _ => result,
                    }
                }
                result
            }
            _ => QueryType::Read,
        }
    }

    /// returns a list of table names from the CassandraOperation
    pub fn get_table_names(&mut self) -> Vec<&FQName> {
        self.operation
            .queries()
            .into_iter()
            .filter_map(|stmt| CQLStatement::get_table_name(stmt))
            .collect()
    }

    /// returns a list of unique keyspace (namespace) from the table names in the statement(s).
    pub fn namespace(&mut self) -> Vec<String> {
        self.get_table_names()
            .into_iter()
            .filter_map(|fq_name| fq_name.keyspace.clone())
            .unique()
            .collect()
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
    ///
    /// TODO: This will return a custom iterator type when BATCH support is added
    pub fn queries(&mut self) -> Vec<&mut CassandraStatement> {
        if let CassandraOperation::Query { query: cql, .. } = self {
            cql.statements.iter_mut().collect()
        } else {
            Vec::<&mut CassandraStatement>::new()
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
pub struct CQLStatement {}

impl CQLStatement {
    pub fn is_begin_batch(statement: &CassandraStatement) -> bool {
        match statement {
            CassandraStatement::Delete(delete) => delete.begin_batch.is_some(),
            CassandraStatement::Insert(insert) => insert.begin_batch.is_some(),
            CassandraStatement::Update(update) => update.begin_batch.is_some(),
            _ => false,
        }
    }

    /// returns the query type for the current statement.
    pub fn get_query_type(statement: &CassandraStatement) -> QueryType {
        /* the query types in descending order, are:
        ReadWrite
        Write
        Read
        SchemaChange
        PubSubMessage
            */
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

    /// returns the table name specified in the command if one is present.
    pub fn get_table_name(statement: &CassandraStatement) -> Option<&FQName> {
        match statement {
            CassandraStatement::AlterTable(t) => Some(&t.name),
            CassandraStatement::CreateIndex(i) => Some(&i.table),
            CassandraStatement::CreateMaterializedView(m) => Some(&m.table),
            CassandraStatement::CreateTable(t) => Some(&t.name),
            CassandraStatement::Delete(d) => Some(&d.table_name),
            CassandraStatement::DropTable(t) => Some(&t.name),
            CassandraStatement::DropTrigger(t) => Some(&t.table),
            CassandraStatement::Insert(i) => Some(&i.table_name),
            CassandraStatement::Select(s) => Some(&s.table_name),
            CassandraStatement::Truncate(t) => Some(t),
            CassandraStatement::Update(u) => Some(&u.table_name),
            _ => None,
        }
    }

    fn has_params_in_operand(operand: &Operand) -> bool {
        match operand {
            Operand::Tuple(vec) | Operand::Collection(vec) => {
                for oper in vec {
                    if CQLStatement::has_params_in_operand(oper) {
                        return true;
                    }
                }
                false
            }
            Operand::Param(_) => true,
            _ => false,
        }
    }

    fn has_params_in_relation_elements(where_clause: &[RelationElement]) -> bool {
        for relation_idx in where_clause {
            if CQLStatement::has_params_in_operand(&relation_idx.value) {
                return true;
            }
        }
        false
    }

    /// Returns true if there are any parameters in the query
    pub fn has_params(statement: &CassandraStatement) -> bool {
        match statement {
            CassandraStatement::Delete(delete) => {
                if CQLStatement::has_params_in_relation_elements(&delete.where_clause) {
                    return true;
                }
                if CQLStatement::has_params_in_relation_elements(&delete.if_clause) {
                    return true;
                }
            }
            CassandraStatement::Insert(insert) => {
                if let InsertValues::Values(operands) = &insert.values {
                    for operand in operands {
                        if let Operand::Param(_) = operand {
                            return true;
                        }
                    }
                }
            }
            CassandraStatement::Select(select) => {
                return CQLStatement::has_params_in_relation_elements(&select.where_clause);
            }
            CassandraStatement::Update(update) => {
                for assignment_element in &update.assignments {
                    if let Operand::Param(_) = &assignment_element.value {
                        return true;
                    }
                    if let Some(assignment_operator) = &assignment_element.operator {
                        match assignment_operator {
                            AssignmentOperator::Plus(operand) => {
                                if let Operand::Param(_) = operand {
                                    return true;
                                }
                            }
                            AssignmentOperator::Minus(operand) => {
                                if let Operand::Param(_) = operand {
                                    return true;
                                }
                            }
                        }
                    }
                }
                if CQLStatement::has_params_in_relation_elements(&update.where_clause) {
                    return true;
                }
                if CQLStatement::has_params_in_relation_elements(&update.if_clause) {
                    return true;
                }
            }
            _ => {}
        }
        false
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct CQL {
    pub statements: Vec<CassandraStatement>,
}

impl CQL {
    fn to_query_string(&self) -> String {
        self.statements.iter().map(|c| c.to_string()).join("; ")
    }

    /// the CassandraAST handles multiple queries in a string separated by semi-colons: `;` however
    /// CQL only stores one query so this method only returns the first one if there are multiples.
    pub fn parse_from_string(cql_query_str: &str) -> Self {
        debug!("parse_from_string: {}", cql_query_str);
        let ast = CassandraAST::new(cql_query_str);

        let statements = if ast.has_error() {
            vec![CassandraStatement::Unknown(cql_query_str.to_string())]
        } else {
            ast.statements
                .into_iter()
                .map(|parsed_statement| parsed_statement.statement)
                .collect()
        };
        CQL { statements }
    }
}

pub trait ToCassandraType {
    fn as_cassandra_type(&self) -> CassandraType;
}

impl ToCassandraType for Operand {
    fn as_cassandra_type(&self) -> CassandraType {
        // function to convert string to CassandraType
        let from_string_value = |value: &str| {
            // check for string types
            if value.starts_with('\'') || value.starts_with("$$") {
                /* to convert to a VarChar type we have to strip the delimiters off the front and back
                of the string.  Soe remove one char (front and back) in the case of `'` and two in the case of `$$`
                 */
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
        };
        match self {
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
            Operand::Tuple(values) => CassandraType::Tuple(
                values
                    .iter()
                    .map(|value| value.as_cassandra_type())
                    .collect(),
            ),
            Operand::Column(value) => CassandraType::Ascii(value.to_string()),
            Operand::Func(value) => CassandraType::Ascii(value.to_string()),
            Operand::Null => CassandraType::Null,
            Operand::Param(_) => CassandraType::Null,
            Operand::Collection(values) => CassandraType::List(
                values
                    .iter()
                    .map(|value| value.as_cassandra_type())
                    .collect(),
            ),
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

impl Serialize for CassandraResult {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        let res_result_body: ResResultBody = match self {
            CassandraResult::Rows { value, metadata } => match value {
                MessageValue::Rows(rows) => {
                    let mut rows_content: Vec<Vec<CBytes>> = Vec::with_capacity(rows.len());
                    for row in rows {
                        let mut row_data = Vec::with_capacity(row.len());
                        for element in row {
                            let b = cassandra_protocol::types::value::Bytes::from(element.clone());
                            row_data.push(CBytes::new(b.into_inner()));
                        }
                        rows_content.push(row_data);
                    }
                    let body_res_result_rows = BodyResResultRows {
                        metadata: metadata.clone(),
                        rows_count: rows.len() as CInt,
                        rows_content,
                    };
                    ResResultBody::Rows(body_res_result_rows)
                }
                _ => ResResultBody::Void,
            },
            CassandraResult::SetKeyspace(keyspace) => ResResultBody::SetKeyspace(*keyspace.clone()),
            CassandraResult::Prepared(prepared) => ResResultBody::Prepared(*prepared.clone()),
            CassandraResult::SchemaChange(schema_change) => {
                ResResultBody::SchemaChange(schema_change.clone())
            }
            CassandraResult::Void => ResResultBody::Void,
        };
        res_result_body.serialize(cursor);
    }
}

impl CassandraResult {
    pub fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> Result<CassandraResult> {
        let res_result_body = ResResultBody::from_cursor(cursor, version)?;
        Ok(match res_result_body {
            ResResultBody::Void => CassandraResult::Void,

            ResResultBody::Rows(body_res_result_rows) => {
                let mut value: Vec<Vec<MessageValue>> =
                    Vec::with_capacity(body_res_result_rows.rows_content.len());
                for row in &body_res_result_rows.rows_content {
                    let mut row_values =
                        Vec::with_capacity(body_res_result_rows.metadata.col_specs.len());
                    for (cbytes, colspec) in row
                        .iter()
                        .zip(body_res_result_rows.metadata.col_specs.iter())
                    {
                        row_values.push(MessageValue::build_value_from_cstar_col_type(
                            colspec, cbytes,
                        ));
                    }
                    value.push(row_values);
                }
                CassandraResult::Rows {
                    value: MessageValue::Rows(value),
                    metadata: body_res_result_rows.metadata,
                }
            }
            ResResultBody::SetKeyspace(keyspace) => {
                CassandraResult::SetKeyspace(Box::new(keyspace))
            }
            ResResultBody::Prepared(prepared) => CassandraResult::Prepared(Box::new(prepared)),
            ResResultBody::SchemaChange(schema_change) => {
                CassandraResult::SchemaChange(schema_change)
            }
        })
    }
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

#[cfg(test)]
mod test {
    use crate::frame::cassandra::ToCassandraType;
    use crate::frame::CQL;
    use cassandra_protocol::types::cassandra_type::CassandraType;
    use cassandra_protocol::types::prelude::Blob;
    use cql3_parser::cassandra_statement::CassandraStatement;
    use cql3_parser::common::Operand;
    use std::net::IpAddr;
    use std::str::FromStr;
    use uuid::Uuid;

    #[test]
    fn cql_round_trip_test() {
        let query = r#"BEGIN BATCH
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (1, 11, 'foo');
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (2, 12, 'bar');
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (3, 13, 'baz');
            APPLY BATCH;"#;

        let expected = "BEGIN BATCH INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (1, 11, 'foo'); INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (2, 12, 'bar'); INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (3, 13, 'baz'); APPLY BATCH";
        let cql = CQL::parse_from_string(query);
        let result = cql.to_query_string();
        assert_eq!(expected, result)
    }

    #[test]
    fn cql_parse_multiple_test() {
        let query = r#"INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (1, 11, 'foo');
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (2, 12, 'bar');
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (3, 13, 'baz');"#;

        let cql = CQL::parse_from_string(query);
        assert_eq!(3, cql.statements.len());
        for stmt in cql.statements {
            if let CassandraStatement::Insert(_x) = stmt {
                // do nothing
            } else {
                panic!("{:?}  should have been CassandraStatement::Insert", stmt);
            }
        }
    }

    #[test]
    fn cql_bad_statement_test() {
        let queries = [
            "INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name)  (2, 12, 'bar');",
            r#"BEGIN BATCH INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (3, 13, 'baz');
            INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name)  (2, 12, 'bar');
            EXECUTE BATCH"#,
            r#"INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (3, 13, 'baz');
            INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name)  (2, 12, 'bar');"#];
        for query in queries {
            let cql = CQL::parse_from_string(query);
            let result = cql.to_query_string();
            assert_eq!(1, cql.statements.len());
            if let CassandraStatement::Unknown(txt) = &cql.statements[0] {
                assert_eq!(query, txt, "failed at test {}", query);
            } else {
                panic!("Should be Unknown type, failed at test {}", query);
            }
            assert_eq!(query, result);
        }
    }

    #[test]
    pub fn test_to_cassandra_type_for_const_operand() {
        assert_eq!(
            CassandraType::Bigint(55),
            Operand::Const("55".to_string()).as_cassandra_type()
        );
        assert_eq!(
            CassandraType::Double(5.5),
            Operand::Const("5.5".to_string()).as_cassandra_type()
        );
        let uuid = Uuid::parse_str("123e4567-e89b-12d3-a456-426655440000").unwrap();
        assert_eq!(
            CassandraType::Uuid(uuid),
            Operand::Const("123e4567-e89b-12d3-a456-426655440000".to_string()).as_cassandra_type()
        );
        let ipaddr = IpAddr::from_str("192.168.0.1").unwrap();
        assert_eq!(
            CassandraType::Inet(ipaddr),
            Operand::Const("192.168.0.1".to_string()).as_cassandra_type()
        );
        let ipaddr = IpAddr::from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap();
        assert_eq!(
            CassandraType::Inet(ipaddr),
            Operand::Const("2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_string())
                .as_cassandra_type()
        );
        assert_eq!(
            CassandraType::Blob(Blob::from(vec![255_u8, 234_u8, 1_u8, 13_u8])),
            Operand::Const("0xFFEA010D".to_string()).as_cassandra_type()
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
                Operand::Const(txt.to_string()).as_cassandra_type()
            );
        }
        assert_eq!(
            CassandraType::Null,
            Operand::Const("not a valid const".to_string()).as_cassandra_type()
        );
        assert_eq!(
            CassandraType::Null,
            Operand::Const("0xnot a hex".to_string()).as_cassandra_type()
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
            Operand::List(args.clone()).as_cassandra_type()
        );
        assert_eq!(
            CassandraType::Set(expected),
            Operand::Set(args).as_cassandra_type()
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
            Operand::Map(args).as_cassandra_type()
        )
    }

    #[test]
    pub fn test_to_cassandra_type_for_collection_operands() {
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
            CassandraType::Tuple(expected.clone()),
            Operand::Tuple(args.clone()).as_cassandra_type()
        );
        assert_eq!(
            CassandraType::List(expected),
            Operand::Collection(args).as_cassandra_type()
        );
    }

    #[test]
    pub fn test_to_cassandra_type_for_misc_operands() {
        assert_eq!(
            CassandraType::Ascii("Hello".to_string()),
            Operand::Column("Hello".to_string()).as_cassandra_type()
        );
        assert_eq!(
            CassandraType::Ascii("Hello".to_string()),
            Operand::Func("Hello".to_string()).as_cassandra_type()
        );
        assert_eq!(CassandraType::Null, Operand::Null.as_cassandra_type());
        assert_eq!(
            CassandraType::Null,
            Operand::Param("Hello".to_string()).as_cassandra_type()
        );
    }
}
