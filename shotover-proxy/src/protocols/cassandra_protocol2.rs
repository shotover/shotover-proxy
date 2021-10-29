use byteorder::{BigEndian, WriteBytesExt};
use bytes::{BufMut, BytesMut};
use cassandra_proto::compressors::no_compression::NoCompression;
use cassandra_proto::consistency::Consistency;
use cassandra_proto::frame::frame_result::{
    BodyResResultRows, ColSpec, ColType, ColTypeOption, ResResultBody, RowsMetadata,
};
use cassandra_proto::frame::parser::FrameHeader;
use cassandra_proto::frame::{parser, Flag, Frame, IntoBytes, Opcode, Version};
use cassandra_proto::query::QueryValues;
use cassandra_proto::types::value::Value as CValue;
use cassandra_proto::types::{CBytes, CInt, CString};
use tokio_util::codec::{Decoder, Encoder};

use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::str::FromStr;
use tracing::{info, trace, debug, warn};

use crate::message::{
    ASTHolder, Message, MessageDetails, Messages, QueryMessage, QueryResponse, QueryType, Value,
};
use crate::protocols::RawFrame;
use cassandra_proto::frame::frame_response::ResponseBody;
use sqlparser::ast::Expr::{BinaryOp, Identifier};
use sqlparser::ast::Statement::{Delete, Insert, Update};
use sqlparser::ast::{
    BinaryOperator, Expr, ObjectName, Select, SelectItem, SetExpr, Statement, TableFactor,
    Value as SQLValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::ops::DerefMut;

use anyhow::{anyhow, Result};
use crate::server::CodecErrorFixup;
use cassandra_proto::frame::frame_error::CDRSError;
use std::thread;

#[derive(Debug, Clone)]
pub struct CassandraCodec2 {
    compressor: NoCompression,
    current_head: Option<FrameHeader>,
    current_frames: Vec<Frame>,
    pk_col_map: HashMap<String, Vec<String>>,
    bypass: bool,
}

pub(crate) struct ParsedCassandraQueryString {
    namespace: Option<Vec<String>>,
    pub(crate) colmap: Option<HashMap<String, Value>>,
    projection: Option<Vec<String>>,
    primary_key: HashMap<String, Value>,
    pub(crate) ast: Option<Statement>,
}

impl CassandraCodec2 {
    pub fn new(pk_col_map: HashMap<String, Vec<String>>, bypass: bool) -> CassandraCodec2 {
        CassandraCodec2 {
            compressor: NoCompression::new(),
            current_head: None,
            current_frames: Vec::new(),
            pk_col_map,
            bypass,
        }
    }

    pub fn build_cassandra_query_frame(
        mut query: QueryMessage,
        default_consistency: Consistency,
    ) -> Frame {
        CassandraCodec2::rebuild_ast_in_message(&mut query);
        CassandraCodec2::rebuild_query_string_from_ast(&mut query);
        let QueryMessage {
            query_string,
            namespace: _,
            primary_key: _,
            query_values,
            projection: _,
            query_type: _,
            ast: _,
        } = query;

        let values: Option<QueryValues> = Some(QueryValues::SimpleValues(
            query_values
                .unwrap()
                .values()
                .map(|x| CValue::new_normal(x.clone()))
                .collect(),
        ));
        let with_names: Option<bool> = Some(false);
        let page_size: Option<i32> = None;
        let paging_state: Option<CBytes> = None;
        let serial_consistency: Option<Consistency> = None;
        let timestamp: Option<i64> = None;
        let flags: Vec<Flag> = vec![];
        Frame::new_req_query(
            query_string,
            default_consistency,
            values,
            with_names,
            page_size,
            paging_state,
            serial_consistency,
            timestamp,
            flags,
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
                                ksname: Some(CString::new(query.namespace.get(0).unwrap().clone())),
                                tablename: Some(CString::new(
                                    query.namespace.get(1).unwrap().clone(),
                                )),
                                name: CString::new(x.clone()),
                                col_type: ColTypeOption {
                                    id: ColType::Ascii, // todo: get types working
                                    value: None,
                                },
                            }
                        })
                        .collect();
                    let count = rows.get(0).unwrap().len() as i32;
                    let metadata = RowsMetadata {
                        flags: 0,
                        columns_count: count,
                        paging_state: None,
                        // global_table_space: Some(query.namespace.iter()
                        //     .map(|x| CString::new(x.clone())).collect()),
                        global_table_space: None,
                        col_specs: col_spec,
                    };

                    let result_bytes = rows
                        .iter()
                        .map(|i| {
                            let rr: Vec<CBytes> = i
                                .iter()
                                .map(|j| {
                                    let rb: CBytes = CBytes::new(match j {
                                        Value::NULL => (-1_i32).into_cbytes(),
                                        Value::Bytes(x) => x.to_vec(),
                                        Value::Strings(x) => {
                                            Vec::from(x.as_bytes())
                                            // CString::new(x.clone()).into_cbytes()
                                        }
                                        Value::Integer(x) => {
                                            let mut temp: Vec<u8> = Vec::new();
                                            let _ = temp.write_i64::<BigEndian>(*x).unwrap();
                                            temp
                                            // Decimal::new(*x, 0).into_cbytes()
                                        }
                                        Value::Float(x) => {
                                            let mut temp: Vec<u8> = Vec::new();
                                            let _ = temp.write_f64::<BigEndian>(*x).unwrap();
                                            temp
                                        }
                                        Value::Boolean(x) => {
                                            let mut temp: Vec<u8> = Vec::new();
                                            let _ = temp.write_i32::<BigEndian>(*x as i32).unwrap();
                                            temp
                                            // (x.clone() as CInt).into_cbytes()
                                        }
                                        _ => unreachable!(),
                                    });
                                    rb
                                })
                                .collect();
                            rr
                        })
                        .collect();

                    let response = ResResultBody::Rows(BodyResResultRows {
                        metadata,
                        rows_count: rows.len() as CInt,
                        rows_content: result_bytes,
                    });

                    return Frame {
                        version: Version::Response,
                        flags: query_frame.flags,
                        opcode: Opcode::Result,
                        stream: query_frame.stream,
                        body: response.into_cbytes(),
                        tracing_id: query_frame.tracing_id,
                        warnings: Vec::new(),
                    };
                }
            }
        }
        unreachable!()
    }
}

impl CassandraCodec2 {
    fn value_to_expr(v: &Value) -> SQLValue {
        match v {
            Value::NULL => SQLValue::Null,
            Value::Bytes(b) => SQLValue::SingleQuotedString(String::from_utf8(b.to_vec()).unwrap()), // todo: this is definitely wrong
            Value::Strings(s) => SQLValue::SingleQuotedString(s.clone()),
            Value::Integer(i) => SQLValue::Number(i.to_string()),
            Value::Float(f) => SQLValue::Number(f.to_string()),
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
            SQLValue::Number(v)
            | SQLValue::SingleQuotedString(v)
            | SQLValue::NationalStringLiteral(v) => Value::Strings(v.clone()),
            SQLValue::HexStringLiteral(v) | SQLValue::Date(v) | SQLValue::Time(v) => {
                Value::Strings(v.to_string())
            }
            SQLValue::Boolean(v) => Value::Boolean(*v),
            _ => Value::Strings("NULL".to_string()),
        }
    }

    fn expr_to_string(v: &SQLValue) -> String {
        match v {
            SQLValue::Number(v) => v.to_string(),
            SQLValue::SingleQuotedString(v)
            | SQLValue::NationalStringLiteral(v)
            | SQLValue::HexStringLiteral(v)
            | SQLValue::Date(v)
            | SQLValue::Time(v) => v.to_string(),
            SQLValue::Boolean(v) => v.to_string(),
            _ => "NULL".to_string(),
        }
    }

    fn rebuild_binops_tree<'a>(
        node: &'a mut Expr,
        map: &'a mut HashMap<String, Value>,
        use_bind: bool,
    ) {
        if let BinaryOp { left, op, right } = node {
            match op {
                BinaryOperator::And => {
                    CassandraCodec2::rebuild_binops_tree(left, map, use_bind);
                    CassandraCodec2::rebuild_binops_tree(right, map, use_bind);
                }
                BinaryOperator::Eq => {
                    if let Identifier(i) = left.borrow_mut() {
                        if let Expr::Value(v) = right.borrow_mut() {
                            if let Some((_, new_v)) = map.get_key_value(&i.to_string()) {
                                if use_bind {
                                    let _ =
                                        std::mem::replace(v, CassandraCodec2::value_to_bind(new_v));
                                } else {
                                    let _ =
                                        std::mem::replace(v, CassandraCodec2::value_to_expr(new_v));
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn binary_ops_to_hashmap<'a>(node: &'a Expr, map: &'a mut HashMap<String, Value>) {
        if let BinaryOp { left, op, right } = node {
            match op {
                BinaryOperator::And => {
                    CassandraCodec2::binary_ops_to_hashmap(left, map);
                    CassandraCodec2::binary_ops_to_hashmap(right, map);
                }
                BinaryOperator::Eq => {
                    if let Identifier(i) = left.borrow() {
                        if let Expr::Value(v) = right.borrow() {
                            map.insert(i.to_string(), CassandraCodec2::expr_to_value(v));
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
                        cumulator.push(CassandraCodec2::expr_to_string(v));
                    }
                }
            }
        }
        cumulator
    }

    pub fn rebuild_query_string_from_ast(message: &mut QueryMessage) {
        if let QueryMessage {
            query_string,
            namespace: _,
            primary_key: _,
            query_values: _,
            projection: _,
            query_type: _,
            ast: Some(ASTHolder::SQL(ast)),
        } = message
        {
            let new_query_string = format!("{}", ast);
            let _ = std::mem::replace(query_string, new_query_string);
        }
    }

    pub fn rebuild_ast_in_message(message: &mut QueryMessage) {
        if let QueryMessage {
            query_string: _,
            namespace,
            primary_key: _,
            query_values: Some(query_values),
            projection: Some(qm_projection),
            query_type: _,
            ast: Some(ASTHolder::SQL(ast)),
        } = message
        {
            match ast {
                Statement::Query(query) => {
                    if let SetExpr::Select(select) = &mut query.body {
                        let Select {
                            distinct: _,
                            projection,
                            from,
                            selection,
                            group_by: _,
                            having: _,
                        } = select.deref_mut();

                        // Rebuild projection
                        let new_projection: Vec<SelectItem> = qm_projection
                            .iter()
                            .map(|x| {
                                SelectItem::UnnamedExpr(Expr::Value(SQLValue::SingleQuotedString(
                                    x.clone(),
                                )))
                            })
                            .collect();
                        let _ = std::mem::replace(projection, new_projection);

                        // Rebuild namespace
                        if let Some(table_ref) = from.get_mut(0) {
                            if let TableFactor::Table {
                                name,
                                alias: _,
                                args: _,
                                with_hints: _,
                            } = &mut table_ref.relation
                            {
                                let _ = std::mem::replace(
                                    name,
                                    ObjectName {
                                        0: namespace.clone(),
                                    },
                                );
                            }
                        }

                        //Rebuild selection
                        // TODO allow user control of bind
                        if let Some(selection) = selection {
                            CassandraCodec2::rebuild_binops_tree(selection, query_values, true);
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
        query_string: String,
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
        // println!("{:#?}", foo);

        //TODO: We absolutely don't handle multiple statements despite this loop indicating otherwise
        // for statement in ast_list.iter() {
        if let Ok(ast_list) = parsed_sql {
            if let Some(statement) = ast_list.get(0) {
                ast = Some(statement.clone());
                match statement {
                    Statement::Query(q) => {
                        if let SetExpr::Select(s) = q.body.borrow() {
                            projection = s.projection.iter().map(|s| s.to_string()).collect();
                            if let TableFactor::Table {
                                name,
                                alias: _,
                                args: _,
                                with_hints: _,
                            } = &s.from.get(0).unwrap().relation
                            {
                                namespace = name.0.clone();
                            }
                            if let Some(sel) = &s.selection {
                                CassandraCodec2::binary_ops_to_hashmap(sel, colmap.borrow_mut());
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
                    } => {
                        namespace = table_name.0.clone();
                        let values = CassandraCodec2::get_column_values(&source.body);
                        for (i, c) in columns.iter().enumerate() {
                            projection.push(c.clone());
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
                        table_name,
                        assignments,
                        selection,
                    } => {
                        namespace = table_name.0.clone();
                        for assignment in assignments {
                            if let Expr::Value(v) = assignment.clone().value {
                                let converted_value = CassandraCodec2::expr_to_value(v.borrow());
                                colmap.insert(assignment.id.clone(), converted_value);
                            }
                        }
                        if let Some(s) = selection {
                            CassandraCodec2::binary_ops_to_hashmap(s, &mut primary_key);
                        }
                        // projection = ;
                    }
                    Delete {
                        table_name,
                        selection,
                    } => {
                        namespace = table_name.0.clone();
                        if let Some(s) = selection {
                            CassandraCodec2::binary_ops_to_hashmap(s, &mut primary_key);
                        }
                        // projection = None;
                    }
                    _ => {}
                }
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
        match frame.get_body().unwrap() {
            ResponseBody::Error(e) => error = Some(Value::Strings(e.message.into_plain())),
            ResponseBody::Result(r) => {
                if let Some(rows) = r.into_rows() {
                    let mut converted_rows: Vec<HashMap<String, Value>> = Vec::new();
                    for row in rows {
                        let x = row
                            .metadata
                            .col_specs
                            .iter()
                            .enumerate()
                            .map(|(i, _col)| {
                                let col_spec = &row.metadata.col_specs[i];
                                let data: Value = Value::build_value_from_cstar_col_type(
                                    col_spec,
                                    &row.row_content[i],
                                );

                                (col_spec.name.clone().into_plain(), data)
                            })
                            .collect::<HashMap<String, Value>>();
                        converted_rows.push(x);
                    }
                    result = Some(Value::NamedRows(converted_rows));
                }
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
                if let Ok(ResponseBody::Query(body)) = frame.get_body() {
                    let parsed_string = CassandraCodec2::parse_query_string(
                        body.query.clone().into_plain(),
                        &self.pk_col_map,
                    );
                    if parsed_string.ast.is_none() {
                        // TODO: Currently this will probably catch schema changes that don't match
                        // what the SQL parser expects
                        return vec![Message::new_raw(RawFrame::Cassandra(frame))];
                    }
                    return vec![Message::new(
                        MessageDetails::Query(QueryMessage {
                            query_string: body.query.into_plain(),
                            namespace: parsed_string.namespace.unwrap(),
                            primary_key: parsed_string.primary_key,
                            query_values: parsed_string.colmap,
                            projection: parsed_string.projection,
                            query_type: QueryType::Read,
                            ast: parsed_string.ast.map(ASTHolder::SQL),
                        }),
                        false,
                        RawFrame::Cassandra(frame),
                    )];
                }
                vec![Message::new_raw(RawFrame::Cassandra(frame))]
            }
            Opcode::Result => CassandraCodec2::build_response_message(frame, None),
            Opcode::Error => {
                if let Ok(ResponseBody::Error(body)) = frame.get_body() {
                    return vec![Message::new_response(
                        QueryResponse {
                            matching_query: None,
                            result: None,
                            error: Some(Value::Strings(body.message.as_plain())),
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

    fn decode_raw(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, CDRSError> {
        // while src.remaining() != 0 {
        //
        // }

        trace!("{:?} Parsing C* frame", thread::current().id());
        let v: Result<(Option<Frame>, Option<FrameHeader>), CDRSError> = parser::parse_frame(src, &self.compressor, self.current_head.as_ref());
         v.map( |(r,h)|  {
            self.current_head = h;
            r
        }
        )
    }

    fn encode_raw(&mut self, item: Frame, dst: &mut BytesMut) {
        let buffer = item.into_cbytes();
        if buffer.is_empty() {
            info!("{:?} trying to send 0 length frame", thread::current().id());
        }
        dst.put(buffer.as_slice());
    }
}

impl Decoder for CassandraCodec2 {
    type Item = Messages;
    type Error = anyhow::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        debug!("{:?} Decoding {:?}", thread::current().id(), src.to_vec() );
        match self.decode_raw(src) {
            Ok(Some(frame)) => {
                debug!( "{:?} Decoded {:?}", thread::current().id(), &frame );
                Ok(Some(self.process_cassandra_frame(frame)))
            },
            Ok(None) => Ok(None),
            Err(e) => {
                debug!( "{:?} CDRSError {:?}", thread::current().id(), &e );
                let error_frame = Frame {
                    version: Version::Response,
                    flags: vec![],
                    opcode: Opcode::Error,
                    stream: 0,
                    body: vec![ e.error_code.into_cbytes(), e.message.into_cbytes(), e.additional_info.into_cbytes()].concat(),
                    tracing_id: None,
                    warnings: vec![],

                };
                let mut message = Message::new(MessageDetails::Unknown, false,
                                               RawFrame::Cassandra(error_frame ));

                message.protocol_error = 0x10000 | e.error_code;
                info!( "{:?} CDRSError returning {:?}", thread::current().id(), &message );
                Ok(Some(Messages { messages: vec![message], }))
            }
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

impl CodecErrorFixup for CassandraCodec2
{
    fn fixup_err( &self, message: Message ) -> (Option<Message>, Option<Message>, Option<anyhow::Error>) {
        (Some(message), None, None)
    }
}
impl CassandraCodec2 {

    fn encode_message(&mut self, item: Message) -> Result<Frame> {
        debug!( "{:?} encode_message  {:?}", thread::current().id(), &item );
        let frame = if !item.modified {
            get_cassandra_frame(item.original)?
        } else {
            match item.details {
                MessageDetails::Bypass(message) => self.encode_message(Message {
                    details: *message,
                    modified: item.modified,
                    original: item.original,
                    protocol_error: item.protocol_error,
                })?,
                MessageDetails::Query(qm) => {
                    CassandraCodec2::build_cassandra_query_frame(qm, Consistency::LocalQuorum)
                }
                MessageDetails::Response(qr) => CassandraCodec2::build_cassandra_response_frame(
                    qr,
                    get_cassandra_frame(item.original)?,
                ),
                MessageDetails::Unknown => get_cassandra_frame(item.original)?,
            }
        };
        debug!( "{:?} Encoded message as {:?}", thread::current().id(),  &frame );
        Ok(frame)
    }
}

impl Encoder<Messages> for CassandraCodec2 {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: Messages,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        for m in item {
            debug!( "{:?} Encoding {:?}", thread::current().id(), &m );
            match self.encode_message(m) {
                Ok(frame) => {
                    self.encode_raw(frame, dst);
                    debug!( "{:?} Encoded frame as {:?}", thread::current().id(), dst);
                },
                Err(e) => {
                    warn!("{:?} Couldn't encode frame {:?}", thread::current().id(), e);
                }
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod cassandra_protocol_tests {
    use crate::message::{ASTHolder, MessageDetails, QueryMessage};
    use crate::protocols::cassandra_protocol2::CassandraCodec2;
    use bytes::BytesMut;
    use hex_literal::hex;
    use std::collections::HashMap;
    use tokio_util::codec::{Decoder, Encoder};
    use cassandra_proto::frame::{Frame, Opcode,Version};

    use crate::protocols::RawFrame::Cassandra;

    const STARTUP_BYTES: [u8; 31] =
        hex!("0400000001000000160001000b43514c5f56455253494f4e0005332e302e30");

    const READY_BYTES: [u8; 9] = hex!("840000000200000000");

    const REGISTER_BYTES: [u8; 58] = hex!(
        "040000010b000000310003000f544f504f4c4f47595f4348414e4745
    000d5354415455535f4348414e4745000d534348454d415f4348414e4745"
    );

    const QUERY_BYTES: [u8; 60] = hex!(
        "0400000307000000330000002c53454c454354202a2046524f4d20737973
    74656d2e6c6f63616c205748455245206b65793d276c6f63616c27000100"
    );

    const RESULT_BYTES: [u8; 162] = hex!(
        "840000020800000099000000020000000100000009000673797374656
    d000570656572730004706565720010000b646174615f63656e746572000d0007686f73745f6964000c000c70726566
    65727265645f6970001000047261636b000d000f72656c656173655f76657273696f6e000d000b7270635f616464726
    573730010000e736368656d615f76657273696f6e000c0006746f6b656e730022000d00000000"
    );

    fn test_frame(codec: &mut CassandraCodec2, raw_frame: &[u8]) {
        let message = codec
            .decode(&mut BytesMut::from(raw_frame))
            .unwrap()
            .unwrap();

        let mut dest = BytesMut::new();
        codec.encode(message, &mut dest).unwrap();
        assert_eq!(raw_frame, &dest);
    }

    #[test]
    fn test_startup_codec() {
        let mut pk_map = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );
        let mut codec = CassandraCodec2::new(pk_map, false);
        test_frame(&mut codec, &STARTUP_BYTES);
    }

    #[test]
    fn test_ready_codec() {
        let mut pk_map = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );
        let mut codec = CassandraCodec2::new(pk_map, false);
        test_frame(&mut codec, &READY_BYTES);
    }

    #[test]
    fn test_register_codec() {
        let mut pk_map = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );
        let mut codec = CassandraCodec2::new(pk_map, false);
        test_frame(&mut codec, &REGISTER_BYTES);
    }

    #[test]
    fn test_result_codec() {
        let mut pk_map = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );
        let mut codec = CassandraCodec2::new(pk_map, false);
        test_frame(&mut codec, &RESULT_BYTES);
    }

    #[test]
    fn test_query_codec() {
        let mut pk_map = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );
        let mut codec = CassandraCodec2::new(pk_map, false);
        test_frame(&mut codec, &QUERY_BYTES);
    }

    #[test]
    fn test_query_codec_ast_builder() {
        let mut pk_map = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );

        let mut codec = CassandraCodec2::new(pk_map, false);
        let mut bytes = BytesMut::from(QUERY_BYTES.as_ref());
        let messages = codec.decode(&mut bytes).unwrap().unwrap();
        for message in messages {
            match message.details {
                MessageDetails::Query(QueryMessage {
                    query_string,
                    ast: Some(ASTHolder::SQL(ast)),
                    ..
                }) => {
                    println!("{}", query_string);
                    println!("{}", ast);

                    assert_eq!(
                        query_string.replace(char::is_whitespace, ""),
                        ast.to_string().replace(char::is_whitespace, ""),
                    );
                }
                details => panic!("Unexpected details: {:?}", details),
            }
        }
    }

    #[test]
    fn test_process_cassandra_frame() {
        // Frame { version: Request, flags: [], opcode: Options, stream: 0, body: [], tracing_id: None, warnings: [] }
        let frame =  Frame {
            version: Version::Request,
            flags: vec![],
            opcode: Opcode::Options,
            stream: 0,
            body: vec![],
            tracing_id: None,
            warnings: vec![],
        };
        let mut pk_map = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );

        let codec = CassandraCodec2::new(pk_map, false);
        let messages = codec.process_cassandra_frame(frame);
        assert_eq!( 1, messages.messages.len());
        for message in messages {
            match message.details {
                MessageDetails::Unknown => {},
                details => panic!("Unexpected details: {:?}", details),
            };
            assert!( ! message.modified );
            match message.original {
                Cassandra(cframe) => {
                    assert_eq!(Version::Request, cframe.version);
                    assert_eq!(0, cframe.flags.len());
                    assert_eq!(Opcode::Options, cframe.opcode);
                    assert_eq!(0, cframe.stream);
                    assert_eq!(0, cframe.body.len());
                    assert_eq!(None, cframe.tracing_id);
                    assert_eq!(0, cframe.warnings.len());
                }
                original => panic!("Unexpected original: {:?}", original),
            }
        }
    }
}
