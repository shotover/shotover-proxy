use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::ops::DerefMut;

use anyhow::{anyhow, Result};
use byteorder::{BigEndian, WriteBytesExt};
use bytes::{BufMut, BytesMut};
use cassandra_protocol::compression::Compression;
use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::frame::frame_error::{AdditionalErrorInfo, ErrorBody};
use cassandra_protocol::frame::frame_request::RequestBody;
use cassandra_protocol::frame::frame_response::ResponseBody;
use cassandra_protocol::frame::frame_result::{
    BodyResResultRows, ColSpec, ColType, ColTypeOption, ResResultBody, RowsMetadata,
    RowsMetadataFlags, TableSpec,
};
use cassandra_protocol::frame::{
    Direction, Flags, Frame, Opcode, ParseFrameError, Serialize, Version,
};
use cassandra_protocol::query::QueryValues;
use cassandra_protocol::types::value::Value as CValue;
use cassandra_protocol::types::{to_int, CBytes, CInt};
use itertools::Itertools;
use sqlparser::ast::Expr::{BinaryOp, Identifier};
use sqlparser::ast::Statement::{Delete, Insert, Update};
use sqlparser::ast::{
    BinaryOperator, Expr, ObjectName, Select, SelectItem, SetExpr, Statement, TableFactor,
    Value as SQLValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, error, info, warn};

use crate::message::{
    ASTHolder, IntSize, Message, MessageDetails, Messages, QueryMessage, QueryResponse, QueryType,
    Value,
};
use crate::protocols::RawFrame;

#[derive(Debug, Clone)]
pub struct CassandraCodec {
    compressor: Compression,
    pk_col_map: HashMap<String, Vec<String>>,
    bypass: bool,
    /// if force_close is Some then the connection will be closed the next time the
    /// system attempts to read data from it.  This is used in protocol errors where we
    /// need to return a message to the client so we can not immediately close the connection
    /// but we also do not know the state of the input stream.  For example if the protocol
    /// number does not match there may be too much or too little data in the buffer so we need
    /// to discard the connection.  The string is used in the error message.
    force_close: Option<String>,
}

pub(crate) struct ParsedCassandraQueryString {
    namespace: Option<Vec<String>>,
    pub(crate) colmap: Option<HashMap<String, Value>>,
    projection: Option<Vec<String>>,
    primary_key: HashMap<String, Value>,
    pub(crate) ast: Option<Statement>,
}

impl CassandraCodec {
    pub fn new(pk_col_map: HashMap<String, Vec<String>>, bypass: bool) -> CassandraCodec {
        CassandraCodec {
            compressor: Compression::None,
            pk_col_map,
            bypass,
            force_close: None,
        }
    }

    pub fn build_cassandra_query_frame(
        mut query: QueryMessage,
        default_consistency: Consistency,
    ) -> Frame {
        CassandraCodec::rebuild_ast_in_message(&mut query);
        CassandraCodec::rebuild_query_string_from_ast(&mut query);
        let QueryMessage {
            query_string,
            query_values,
            ..
        } = query;

        let values = Some(QueryValues::SimpleValues(
            query_values
                .unwrap()
                .values()
                .map(|x| CValue::new(x.clone()))
                .collect(),
        ));
        let with_names = false;
        let page_size = None;
        let paging_state = None;
        let serial_consistency = None;
        let timestamp = None;
        Frame::new_req_query(
            query_string,
            default_consistency,
            values,
            with_names,
            page_size,
            paging_state,
            serial_consistency,
            timestamp,
            Flags::empty(),
            Version::V4,
        )
    }

    pub fn build_cassandra_response_frame(resp: QueryResponse, query_frame: Frame) -> Frame {
        if let Some(Value::Rows(rows)) = resp.result {
            if let Some(ref query) = resp.matching_query {
                if let Some(ref proj) = query.projection {
                    let col_spec = proj
                        .iter()
                        .map(|x| {
                            ColSpec {
                                table_spec: Some(TableSpec {
                                    ks_name: query.namespace.get(0).unwrap().clone(),
                                    table_name: query.namespace.get(1).unwrap().clone(),
                                }),
                                name: x.clone(),
                                col_type: ColTypeOption {
                                    id: ColType::Ascii, // TODO: get types working
                                    value: None,
                                },
                            }
                        })
                        .collect();
                    let count = rows.get(0).unwrap().len() as i32;
                    let metadata = RowsMetadata {
                        flags: RowsMetadataFlags::empty(),
                        columns_count: count,
                        paging_state: None,
                        global_table_spec: None,
                        col_specs: col_spec,
                    };

                    let result_bytes = rows
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(|value| {
                                    CBytes::new(match value {
                                        Value::NULL => to_int(-1_i32),
                                        Value::Bytes(x) => x.to_vec(),
                                        Value::Strings(x) => Vec::from(x.as_bytes()),
                                        Value::Integer(x, size) => {
                                            let mut temp: Vec<u8> = Vec::new();

                                            match size {
                                                IntSize::I64 => {
                                                    temp.write_i64::<BigEndian>(*x).unwrap();
                                                }
                                                IntSize::I32 => {
                                                    temp.write_i32::<BigEndian>(*x as i32).unwrap();
                                                }
                                                IntSize::I16 => {
                                                    temp.write_i16::<BigEndian>(*x as i16).unwrap();
                                                }
                                                IntSize::I8 => {
                                                    temp.write_i8(*x as i8).unwrap();
                                                }
                                            }

                                            temp
                                        }
                                        Value::Float(x) => {
                                            let mut temp: Vec<u8> = Vec::new();
                                            let _ = temp
                                                .write_f32::<BigEndian>(x.into_inner())
                                                .unwrap();
                                            temp
                                        }
                                        Value::Boolean(x) => {
                                            let mut temp: Vec<u8> = Vec::new();
                                            let _ = temp.write_i32::<BigEndian>(*x as i32).unwrap();
                                            temp
                                        }
                                        _ => unreachable!(),
                                    })
                                })
                                .collect()
                        })
                        .collect();

                    let response = ResResultBody::Rows(BodyResResultRows {
                        metadata,
                        rows_count: rows.len() as CInt,
                        rows_content: result_bytes,
                    });

                    return Frame {
                        version: Version::V4,
                        direction: Direction::Response,
                        flags: query_frame.flags,
                        opcode: Opcode::Result,
                        stream_id: query_frame.stream_id,
                        body: response.serialize_to_vec(),
                        tracing_id: query_frame.tracing_id,
                        warnings: Vec::new(),
                    };
                }
            }
        }
        unreachable!()
    }
}

impl CassandraCodec {
    fn value_to_expr(v: &Value) -> SQLValue {
        match v {
            Value::NULL => SQLValue::Null,
            Value::Bytes(b) => SQLValue::SingleQuotedString(String::from_utf8(b.to_vec()).unwrap()), // TODO: this is definitely wrong
            Value::Strings(s) => SQLValue::SingleQuotedString(s.clone()),
            Value::Integer(i, _) => SQLValue::Number(i.to_string(), false),
            Value::Float(f) => SQLValue::Number(f.to_string(), false),
            Value::Boolean(b) => SQLValue::Boolean(*b),
            _ => SQLValue::Null,
        }
    }

    fn value_to_bind(_v: &Value) -> SQLValue {
        //TODO fix bind handling
        SQLValue::SingleQuotedString("XYz-1-zYX".to_string())
    }

    fn expr_to_value(v: &SQLValue) -> Value {
        match v {
            SQLValue::Number(v, false)
            | SQLValue::SingleQuotedString(v)
            | SQLValue::NationalStringLiteral(v) => Value::Strings(v.clone()),
            SQLValue::HexStringLiteral(v) => Value::Strings(v.to_string()),
            SQLValue::Boolean(v) => Value::Boolean(*v),
            _ => Value::Strings("NULL".to_string()),
        }
    }

    fn expr_to_string(v: &SQLValue) -> String {
        match v {
            SQLValue::Number(v, false) => v.to_string(),
            SQLValue::SingleQuotedString(v)
            | SQLValue::NationalStringLiteral(v)
            | SQLValue::HexStringLiteral(v) => v.to_string(),
            SQLValue::Boolean(v) => v.to_string(),
            _ => "NULL".to_string(),
        }
    }

    fn rebuild_binops_tree(node: &mut Expr, map: &mut HashMap<String, Value>, use_bind: bool) {
        if let BinaryOp { left, op, right } = node {
            match op {
                BinaryOperator::And => {
                    CassandraCodec::rebuild_binops_tree(left, map, use_bind);
                    CassandraCodec::rebuild_binops_tree(right, map, use_bind);
                }
                BinaryOperator::Eq => {
                    if let Identifier(i) = left.borrow_mut() {
                        if let Expr::Value(v) = right.borrow_mut() {
                            if let Some((_, new_v)) = map.get_key_value(&i.to_string()) {
                                *v = if use_bind {
                                    CassandraCodec::value_to_bind(new_v)
                                } else {
                                    CassandraCodec::value_to_expr(new_v)
                                };
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn binary_ops_to_hashmap(node: &Expr, map: &mut HashMap<String, Value>) {
        if let BinaryOp { left, op, right } = node {
            match op {
                BinaryOperator::And => {
                    CassandraCodec::binary_ops_to_hashmap(left, map);
                    CassandraCodec::binary_ops_to_hashmap(right, map);
                }
                BinaryOperator::Eq => {
                    if let Identifier(i) = left.borrow() {
                        if let Expr::Value(v) = right.borrow() {
                            map.insert(i.to_string(), CassandraCodec::expr_to_value(v));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn get_column_values(expr: &SetExpr) -> Vec<String> {
        let mut cumulator: Vec<String> = Vec::new();
        if let SetExpr::Values(v) = expr {
            for value in &v.0 {
                for ex in value {
                    if let Expr::Value(v) = ex {
                        cumulator.push(CassandraCodec::expr_to_string(v));
                    }
                }
            }
        }
        cumulator
    }

    pub fn rebuild_query_string_from_ast(message: &mut QueryMessage) {
        if let QueryMessage {
            query_string,
            ast: Some(ASTHolder::SQL(ast)),
            ..
        } = message
        {
            *query_string = format!("{}", ast);
        }
    }

    pub fn rebuild_ast_in_message(message: &mut QueryMessage) {
        if let QueryMessage {
            namespace,
            query_values: Some(query_values),
            projection: Some(qm_projection),
            ast: Some(ASTHolder::SQL(ast)),
            ..
        } = message
        {
            match &mut **ast {
                Statement::Query(query) => {
                    if let SetExpr::Select(select) = &mut query.body {
                        let Select {
                            projection,
                            from,
                            selection,
                            ..
                        } = select.deref_mut();

                        // Rebuild projection
                        *projection = qm_projection
                            .iter()
                            .map(|x| {
                                SelectItem::UnnamedExpr(Expr::Value(SQLValue::SingleQuotedString(
                                    x.clone(),
                                )))
                            })
                            .collect();

                        // Rebuild namespace
                        if let Some(table_ref) = from.get_mut(0) {
                            if let TableFactor::Table { name, .. } = &mut table_ref.relation {
                                *name = ObjectName(
                                    namespace
                                        .iter()
                                        .cloned()
                                        .map(sqlparser::ast::Ident::new)
                                        .collect(),
                                );
                            }
                        }

                        //Rebuild selection
                        // TODO allow user control of bind
                        if let Some(selection) = selection {
                            CassandraCodec::rebuild_binops_tree(selection, query_values, true);
                        }
                    }
                }
                Statement::Insert { .. } => {}
                Statement::Update { .. } => {}
                Statement::Delete { .. } => {}
                _ => {}
            }
        }
    }

    pub(crate) fn parse_query_string(
        query_string: &str,
        pk_col_map: &HashMap<String, Vec<String>>,
    ) -> ParsedCassandraQueryString {
        let dialect = GenericDialect {}; //TODO write CQL dialect
        let mut namespace: Vec<String> = Vec::new();
        let mut colmap: HashMap<String, Value> = HashMap::new();
        let mut projection: Vec<String> = Vec::new();
        let mut primary_key: HashMap<String, Value> = HashMap::new();
        let mut ast: Option<Statement> = None;
        let parsed_sql = Parser::parse_sql(&dialect, query_string);

        //TODO handle pks
        match parsed_sql {
            Ok(ast_list) => {
                //TODO: We absolutely don't handle multiple statements despite this loop indicating otherwise
                // for statement in ast_list.iter() {
                if let Some(statement) = ast_list.get(0) {
                    ast = Some(statement.clone());
                    match statement {
                        Statement::Query(q) => {
                            if let SetExpr::Select(s) = &q.body {
                                projection = s.projection.iter().map(|s| s.to_string()).collect();
                                if let TableFactor::Table { name, .. } =
                                    &s.from.get(0).unwrap().relation
                                {
                                    namespace = name.0.iter().map(|a| a.value.clone()).collect();
                                }
                                if let Some(sel) = &s.selection {
                                    CassandraCodec::binary_ops_to_hashmap(sel, &mut colmap);
                                }
                                if let Some(pk_col_names) = pk_col_map.get(&namespace.join(".")) {
                                    for pk_component in pk_col_names {
                                        if let Some(value) = colmap.get(pk_component) {
                                            primary_key.insert(pk_component.clone(), value.clone());
                                        } else {
                                            primary_key.insert(pk_component.clone(), Value::NULL);
                                        }
                                    }
                                }
                            }
                        }
                        Insert {
                            table_name,
                            columns,
                            source,
                            ..
                        } => {
                            namespace = table_name.0.iter().map(|a| a.value.clone()).collect();
                            let values = CassandraCodec::get_column_values(&source.body);
                            for (i, c) in columns.iter().enumerate() {
                                projection.push(c.value.clone());
                                if let Some(v) = values.get(i) {
                                    colmap.insert(c.to_string(), Value::Strings(v.clone()));
                                }
                            }

                            if let Some(pk_col_names) = pk_col_map.get(&namespace.join(".")) {
                                for pk_component in pk_col_names {
                                    if let Some(value) = colmap.get(pk_component) {
                                        primary_key.insert(pk_component.clone(), value.clone());
                                    } else {
                                        primary_key.insert(pk_component.clone(), Value::NULL);
                                    }
                                }
                            }
                        }
                        Update {
                            table,
                            assignments,
                            selection,
                        } => {
                            match &table.relation {
                            TableFactor::Table { name, .. } => {
                                namespace = name.0.iter().map(|a| a.value.clone()).collect();
                            }
                            _ => error!(
                                "The cassandra query language does not support `update`s with table of {:?}",
                                table
                            ),
                        };
                            for assignment in assignments {
                                if let Expr::Value(v) = assignment.clone().value {
                                    let converted_value = CassandraCodec::expr_to_value(&v);
                                    colmap.insert(
                                        assignment.id.iter().map(|x| &x.value).join("."),
                                        converted_value,
                                    );
                                }
                            }
                            if let Some(s) = selection {
                                CassandraCodec::binary_ops_to_hashmap(s, &mut primary_key);
                            }
                        }
                        Delete {
                            table_name,
                            selection,
                        } => {
                            namespace = table_name.0.iter().map(|a| a.value.clone()).collect();
                            if let Some(s) = selection {
                                CassandraCodec::binary_ops_to_hashmap(s, &mut primary_key);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(err) => {
                // TODO: We should handle this error better but for now we can at least log it
                error!("Failed to parse csql: {}\nError: {:?}", query_string, err)
            }
        }

        ParsedCassandraQueryString {
            namespace: Some(namespace),
            colmap: Some(colmap),
            projection: Some(projection),
            primary_key,
            ast,
        }
    }

    fn build_response_message(frame: Frame, matching_query: Option<QueryMessage>) -> Messages {
        let mut result: Option<Value> = None;
        let mut error: Option<Value> = None;
        match frame.response_body().unwrap() {
            ResponseBody::Error(e) => error = Some(Value::Strings(e.message)),
            ResponseBody::Result(ResResultBody::Rows(rows)) => {
                let converted_rows = rows
                    .rows_content
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .enumerate()
                            .map(|(i, row_content)| {
                                let col_spec = &rows.metadata.col_specs[i];
                                let data =
                                    Value::build_value_from_cstar_col_type(col_spec, &row_content);

                                (col_spec.name.clone(), data)
                            })
                            .collect()
                    })
                    .collect();
                result = Some(Value::NamedRows(converted_rows));
            }
            _ => {}
        }

        vec![Message::new_response(
            QueryResponse {
                matching_query,
                result,
                error,
                response_meta: None,
            },
            false,
            RawFrame::Cassandra(frame),
        )]
    }

    pub fn process_cassandra_frame(&self, frame: Frame) -> Messages {
        if self.bypass {
            return vec![Message::new_raw(RawFrame::Cassandra(frame))];
        }

        match frame.opcode {
            Opcode::Query => {
                if let Ok(RequestBody::Query(body)) = frame.request_body() {
                    let parsed_query =
                        CassandraCodec::parse_query_string(body.query.as_str(), &self.pk_col_map);
                    if parsed_query.ast.is_none() {
                        // TODO: Currently this will probably catch schema changes that don't match
                        // what the SQL parser expects
                        return vec![Message::new_raw(RawFrame::Cassandra(frame))];
                    }
                    return vec![Message::new(
                        MessageDetails::Query(QueryMessage {
                            query_string: body.query,
                            namespace: parsed_query.namespace.unwrap(),
                            primary_key: parsed_query.primary_key,
                            query_values: parsed_query.colmap,
                            projection: parsed_query.projection,
                            query_type: QueryType::Read,
                            ast: parsed_query.ast.map(|x| ASTHolder::SQL(Box::new(x))),
                        }),
                        false,
                        RawFrame::Cassandra(frame),
                    )];
                }
                vec![Message::new_raw(RawFrame::Cassandra(frame))]
            }
            Opcode::Result => CassandraCodec::build_response_message(frame, None),
            Opcode::Error => {
                if let Ok(ResponseBody::Error(body)) = frame.response_body() {
                    return vec![Message::new_response(
                        QueryResponse {
                            matching_query: None,
                            result: None,
                            error: Some(Value::Strings(body.message)),
                            response_meta: None,
                        },
                        false,
                        RawFrame::Cassandra(frame),
                    )];
                }

                vec![Message::new_raw(RawFrame::Cassandra(frame))]
            }
            _ => vec![Message::new_raw(RawFrame::Cassandra(frame))],
        }
    }

    fn encode_raw(&mut self, item: Frame, dst: &mut BytesMut) {
        let buffer = item.encode_with(self.compressor).unwrap();
        if buffer.is_empty() {
            info!("trying to send 0 length frame");
        }
        dst.put(buffer.as_slice());
    }
}

impl Decoder for CassandraCodec {
    type Item = Messages;
    type Error = anyhow::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        // if we need to close the connection return an error.
        if let Some(result) = self.force_close.take() {
            debug!("Closing errored connection: {:?}", &result);
            return Err(anyhow!(result));
        }

        match Frame::from_buffer(src, self.compressor) {
            Ok(parsed_frame) => {
                // Clear the read bytes from the FramedReader
                let _ = src.split_to(parsed_frame.frame_len);

                Ok(Some(self.process_cassandra_frame(parsed_frame.frame)))
            }
            Err(ParseFrameError::NotEnoughBytes) => Ok(None),
            Err(ParseFrameError::UnsupportedVersion(version)) => {
                // if we got an error force the close on the next read.
                // We can not immediately close as we are gong to queue a message
                // back to the client and we have to allow time for the message
                // to be sent.  We can not reuse the connection as it may/does contain excess
                // data from the failed parse.
                self.force_close = Some(format!(
                    "Received frame with unknown protocol version: {}",
                    version
                ));

                let message = Message::new(
                    MessageDetails::ReturnToSender,
                    false,
                    RawFrame::Cassandra(Frame {
                        version: Version::V4,
                        direction: Direction::Response,
                        flags: Flags::empty(),
                        opcode: Opcode::Error,
                        stream_id: 0,
                        body: ErrorBody {
                            error_code: 0xA,
                            message: "Invalid or unsupported protocol version".into(),
                            additional_info: AdditionalErrorInfo::Server,
                        }
                        .serialize_to_vec(),
                        tracing_id: None,
                        warnings: vec![],
                    }),
                );
                Ok(Some(vec![message]))
            }
            err => Err(anyhow!("Failed to parse frame {:?}", err)),
        }
    }
}

fn get_cassandra_frame(rf: RawFrame) -> Result<Frame> {
    if let RawFrame::Cassandra(frame) = rf {
        Ok(frame)
    } else {
        warn!("Unsupported Frame detected - Dropping Frame {:?}", rf);
        Err(anyhow!("Unsupported frame found, not sending"))
    }
}

impl CassandraCodec {
    fn encode_message(&mut self, item: Message) -> Result<Frame> {
        let frame = if !item.modified {
            get_cassandra_frame(item.original)?
        } else {
            match item.details {
                MessageDetails::Query(qm) => {
                    CassandraCodec::build_cassandra_query_frame(qm, Consistency::LocalQuorum)
                }
                MessageDetails::Response(qr) => CassandraCodec::build_cassandra_response_frame(
                    qr,
                    get_cassandra_frame(item.original)?,
                ),
                MessageDetails::Unknown => get_cassandra_frame(item.original)?,
                MessageDetails::ReturnToSender => get_cassandra_frame(item.original)?,
            }
        };
        debug!("Encoded message as {:?}", &frame);
        Ok(frame)
    }
}

impl Encoder<Messages> for CassandraCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: Messages,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        for m in item {
            debug!("Encoding {:?}", &m);
            match self.encode_message(m) {
                Ok(frame) => {
                    self.encode_raw(frame, dst);
                    debug!("Encoded frame as {:?}", dst);
                }
                Err(e) => {
                    warn!("Couldn't encode frame {:?}", e);
                }
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod cassandra_protocol_tests {
    use crate::message::{
        ASTHolder, Message, MessageDetails, QueryMessage, QueryResponse, QueryType, Value,
    };
    use crate::protocols::cassandra_codec::CassandraCodec;
    use crate::protocols::RawFrame;
    use bytes::BytesMut;
    use cassandra_protocol::frame::{Direction, Flags, Frame, Opcode, Version};
    use hex_literal::hex;
    use sqlparser::ast::Expr::BinaryOp;
    use sqlparser::ast::{
        BinaryOperator, Expr, Ident, ObjectName, Query, Select, SelectItem, SetExpr, Statement,
        TableFactor, TableWithJoins, Value as SQLValue,
    };
    use std::collections::HashMap;
    use tokio_util::codec::{Decoder, Encoder};

    fn test_frame_codec_roundtrip(
        codec: &mut CassandraCodec,
        raw_frame: &[u8],
        expected_messages: Vec<Message>,
    ) {
        // test decode
        let decoded_messages = codec
            .decode(&mut BytesMut::from(raw_frame))
            .unwrap()
            .unwrap();
        assert_eq!(decoded_messages, expected_messages);

        // test encode round trip
        let mut dest = BytesMut::new();
        codec.encode(decoded_messages, &mut dest).unwrap();
        assert_eq!(raw_frame, &dest);
    }

    fn new_codec() -> CassandraCodec {
        let mut pk_map = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );
        CassandraCodec::new(pk_map, false)
    }

    #[test]
    fn test_codec_startup() {
        let mut codec = new_codec();
        let bytes = hex!("0400000001000000160001000b43514c5f56455253494f4e0005332e302e30");
        let messages = vec![Message {
            details: MessageDetails::Unknown,
            modified: false,
            original: RawFrame::Cassandra(Frame {
                version: Version::V4,
                direction: Direction::Request,
                flags: Flags::empty(),
                opcode: Opcode::Startup,
                stream_id: 0,
                body: hex!("0001000b43514c5f56455253494f4e0005332e302e30").to_vec(),
                tracing_id: None,
                warnings: vec![],
            }),
        }];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_options() {
        let mut codec = new_codec();
        let bytes = hex!("040000000500000000");
        let messages = vec![Message {
            details: MessageDetails::Unknown,
            modified: false,
            original: RawFrame::Cassandra(Frame {
                version: Version::V4,
                direction: Direction::Request,
                flags: Flags::empty(),
                opcode: Opcode::Options,
                stream_id: 0,
                body: vec![],
                tracing_id: None,
                warnings: vec![],
            }),
        }];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_ready() {
        let mut codec = new_codec();
        let bytes = hex!("840000000200000000");
        let messages = vec![Message {
            details: MessageDetails::Unknown,
            modified: false,
            original: RawFrame::Cassandra(Frame {
                version: Version::V4,
                direction: Direction::Response,
                flags: Flags::empty(),
                opcode: Opcode::Ready,
                stream_id: 0,
                body: vec![],
                tracing_id: None,
                warnings: vec![],
            }),
        }];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_register() {
        let mut codec = new_codec();
        let bytes = hex!(
            "040000010b000000310003000f544f504f4c4f47595f4348414e4745
            000d5354415455535f4348414e4745000d534348454d415f4348414e4745"
        );
        let messages = vec![Message {
            details: MessageDetails::Unknown,
            modified: false,
            original: RawFrame::Cassandra(Frame {
                version: Version::V4,
                direction: Direction::Request,
                flags: Flags::empty(),
                opcode: Opcode::Register,
                stream_id: 1,
                body: hex!(
                    "0003000f544f504f4c4f47595f4348414e4745
                    000d5354415455535f4348414e4745000d534348454d415f4348414e4745"
                )
                .to_vec(),
                tracing_id: None,
                warnings: vec![],
            }),
        }];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_result() {
        let mut codec = new_codec();
        let bytes = hex!(
            "840000020800000099000000020000000100000009000673797374656
            d000570656572730004706565720010000b646174615f63656e746572000d0007686f73745f6964000c000c70726566
            65727265645f6970001000047261636b000d000f72656c656173655f76657273696f6e000d000b7270635f616464726
            573730010000e736368656d615f76657273696f6e000c0006746f6b656e730022000d00000000"
        );
        let messages = vec![Message {
            details: MessageDetails::Response(QueryResponse {
                matching_query: None,
                result: Some(Value::NamedRows(vec![])),
                error: None,
                response_meta: None,
            }),
            modified: false,
            original: RawFrame::Cassandra(Frame {
                version: Version::V4,
                direction: Direction::Response,
                flags: Flags::empty(),
                opcode: Opcode::Result,
                stream_id: 2,
                body: hex!(
                    "000000020000000100000009000673797374656
                    d000570656572730004706565720010000b646174615f63656e746572000d0007686f73745f6964000c000c70726566
                    65727265645f6970001000047261636b000d000f72656c656173655f76657273696f6e000d000b7270635f616464726
                    573730010000e736368656d615f76657273696f6e000c0006746f6b656e730022000d00000000"
                ).to_vec(),
                tracing_id: None,
                warnings: vec![],
            }),
        }];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_query() {
        let mut codec = new_codec();
        let bytes = hex!(
            "0400000307000000330000002c53454c454354202a2046524f4d20737973
            74656d2e6c6f63616c205748455245206b65793d276c6f63616c27000100"
        );
        let messages = vec![Message {
            details: MessageDetails::Query(QueryMessage {
                query_string: "SELECT * FROM system.local WHERE key='local'".into(),
                namespace: vec!["system".into(), "local".into()],
                primary_key: HashMap::new(),
                query_values: Some(HashMap::from([(
                    "key".into(),
                    Value::Strings("local".into()),
                )])),
                projection: Some(vec!["*".into()]),
                query_type: QueryType::Read,
                ast: Some(ASTHolder::SQL(Box::new(Statement::Query(Box::new(
                    Query {
                        with: None,
                        body: SetExpr::Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            projection: vec![SelectItem::Wildcard],
                            from: vec![TableWithJoins {
                                relation: TableFactor::Table {
                                    name: ObjectName(vec![
                                        Ident {
                                            value: "system".into(),
                                            quote_style: None,
                                        },
                                        Ident {
                                            value: "local".into(),
                                            quote_style: None,
                                        },
                                    ]),
                                    alias: None,
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![],
                            }],
                            lateral_views: vec![],
                            selection: Some(BinaryOp {
                                left: Box::new(Expr::Identifier(Ident {
                                    value: "key".into(),
                                    quote_style: None,
                                })),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::Value(SQLValue::SingleQuotedString(
                                    "local".into(),
                                ))),
                            }),
                            group_by: vec![],
                            cluster_by: vec![],
                            distribute_by: vec![],
                            sort_by: vec![],
                            having: None,
                        })),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                ))))),
            }),
            modified: false,
            original: RawFrame::Cassandra(Frame {
                version: Version::V4,
                direction: Direction::Request,
                flags: Flags::empty(),
                opcode: Opcode::Query,
                stream_id: 3,
                body: hex!(
                    "0000002c53454c454354202a2046524f4d20737973
                    74656d2e6c6f63616c205748455245206b65793d276c6f63616c27000100"
                )
                .to_vec(),
                tracing_id: None,
                warnings: vec![],
            }),
        }];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_parse_insert_string() {
        let query_str = "INSERT into tbl(col1,col2,col3) values('one', 2, 3);";
        let hash_map: HashMap<String,Vec<String>> = HashMap::new();
        let query = CassandraCodec::parse_query_string( query_str, &hash_map);
        let colmap = query.colmap.unwrap();
        let namespace = query.namespace.unwrap();
        let projection = query.projection.unwrap();
        assert_eq!( colmap.len(), 3 );
        assert_eq!( colmap.get( "col1").unwrap(), &Value::Strings("one".to_string()));
        assert_eq!( colmap.get( "col2").unwrap(), &Value::Strings("2".to_string()));
        assert_eq!( colmap.get( "col3").unwrap(), &Value::Strings("3".to_string()));
        assert_eq!( namespace.len(), 1);
        assert_eq!( namespace[0], "tbl");
        assert_eq!( projection.len(), 3 );
        assert_eq!( projection[0], "col1");
        assert_eq!( projection[1], "col2");
        assert_eq!( projection[2], "col3");

    }

    #[test]
    fn test_parse_update_string() {
        let query_str = "UPDATE keyspace.tbl Set col1='one', col2='2'  Where col3=3";
        let hash_map: HashMap<String,Vec<String>> = HashMap::new();
        let query = CassandraCodec::parse_query_string( query_str, &hash_map);
        let colmap = query.colmap.unwrap();
        let namespace = query.namespace.unwrap();
        let projection = query.projection.unwrap();
        assert_eq!( colmap.len(), 2  );
        assert_eq!( colmap.get( "col1").unwrap(), &Value::Strings("one".to_string()));
        assert_eq!( colmap.get( "col2").unwrap(), &Value::Strings("2".to_string()));
        assert_eq!( namespace.len(), 2);
        assert_eq!( namespace[0], "keyspace");
        assert_eq!( namespace[1], "tbl");
        assert_eq!( projection.len(), 0 );
        print!( "{:?}", query.ast );
    }

    #[test]
    fn test_parse_delete_column_string() {
        let query_str = "DELETE col1 from keyspace.tbl WHERE col3=3";
        let hash_map: HashMap<String,Vec<String>> = HashMap::new();
        let query = CassandraCodec::parse_query_string( query_str, &hash_map);
        let colmap = query.colmap.unwrap();
        let namespace = query.namespace.unwrap();
        let projection = query.projection.unwrap();
        println!( "colmap {:?}", colmap );
        assert_eq!( colmap.len(), 0  );
//        assert_eq!( colmap.get( "col1").unwrap(), &Value::Strings("one".to_string()));
//        assert_eq!( colmap.get( "col2").unwrap(), &Value::Strings("2".to_string()));
        println!( "namespace {:?}", namespace );
        assert_eq!( namespace.len(), 2);
        assert_eq!( namespace[0], "keyspace");
        assert_eq!( namespace[1], "tbl");
        println!( "projection {:?}", projection );
        assert_eq!( projection.len(), 0 );

        print!( "ast {:?}", query.ast );
    }

    #[test]
    fn test_parse_delete_string() {
        let query_str = "DELETE from keyspace.tbl Where col3=3";
        let hash_map: HashMap<String,Vec<String>> = HashMap::new();
        let query = CassandraCodec::parse_query_string( query_str, &hash_map);
        let colmap = query.colmap.unwrap();
        let namespace = query.namespace.unwrap();
        let projection = query.projection.unwrap();
        let primary_key = query.primary_key;
        println!( "primary_key {:?}", primary_key );
        assert_eq!( primary_key.len(), 1 );
        assert_eq!( primary_key.get( "col3").unwrap(), &Value::Strings("3".to_string()));
        println!( "colmap {:?}", colmap );
        assert_eq!( colmap.len(), 0  );
        println!( "namespace {:?}", namespace );
        assert_eq!( namespace.len(), 2);
        assert_eq!( namespace[0], "keyspace");
        assert_eq!( namespace[1], "tbl");
        println!( "projection {:?}", projection );
        assert_eq!( projection.len(), 0 );

        print!( "{:?}", query.ast );
    }

}
