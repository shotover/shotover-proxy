use crate::message::{ASTHolder, QueryMessage, QueryResponse, Value};
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
use tracing::trace;

use crate::message::{Message, QueryType, RawMessage};
use crate::protocols::RawFrame;
use cassandra_proto::frame::frame_response::ResponseBody;
use chrono::DateTime;
use sqlparser::ast::Expr::{BinaryOp, Identifier};
use sqlparser::ast::Statement::{Delete, Insert, Update};
use sqlparser::ast::{
    BinaryOperator, Expr, ObjectName, Select, SelectItem, SetExpr, Statement, TableFactor,
    Value as SQLValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::ops::{Deref, DerefMut};

use anyhow::{anyhow, Result};

#[derive(Debug, Clone)]
pub struct CassandraCodec2 {
    compressor: NoCompression,
    current_head: Option<FrameHeader>,
    pk_col_map: HashMap<String, Vec<String>>,
    bypass: bool,
}

struct ParsedCassandraQueryString {
    namespace: Option<Vec<String>>,
    colmap: Option<HashMap<String, Value>>,
    projection: Option<Vec<String>>,
    primary_key: HashMap<String, Value>,
    ast: Option<Statement>,
}

impl CassandraCodec2 {
    pub fn new(pk_col_map: HashMap<String, Vec<String>>, bypass: bool) -> CassandraCodec2 {
        return CassandraCodec2 {
            compressor: NoCompression::new(),
            current_head: None,
            pk_col_map,
            bypass,
        };
    }

    pub fn build_cassandra_query_frame(
        mut query: QueryMessage,
        default_consistency: Consistency,
    ) -> Frame {
        CassandraCodec2::rebuild_ast_in_message(&mut query);
        CassandraCodec2::rebuild_query_string_from_ast(&mut query);
        let QueryMessage {
            original: _,
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
        return Frame::new_req_query(
            query_string.clone(),
            default_consistency,
            values,
            with_names,
            page_size,
            paging_state,
            serial_consistency,
            timestamp,
            flags,
        );
    }

    pub fn build_cassandra_response_frame(resp: QueryResponse) -> Frame {
        if let Some(Value::Rows(rows)) = resp.result {
            if let Some(ref query) = resp.matching_query {
                if let RawFrame::CASSANDRA(ref query_frame) = query.original {
                    if let Some(ref proj) = query.projection {
                        let col_spec = proj
                            .iter()
                            .map(|x| {
                                ColSpec {
                                    ksname: Some(CString::new(
                                        query.namespace.get(0).unwrap().clone(),
                                    )),
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
                                            Value::NULL => (-1 as CInt).into_cbytes(),
                                            Value::Bytes(x) => x.to_vec(),
                                            Value::Strings(x) => {
                                                Vec::from(x.clone().as_bytes())
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
                                                let _ =
                                                    temp.write_i32::<BigEndian>(*x as i32).unwrap();
                                                temp
                                                // (x.clone() as CInt).into_cbytes()
                                            }
                                            Value::Timestamp(x) => {
                                                Vec::from(x.to_rfc2822().as_bytes())
                                            }
                                            _ => unreachable!(),
                                        });
                                        return rb;
                                    })
                                    .collect();
                                return rr;
                            })
                            .collect();

                        let response = ResResultBody::Rows(BodyResResultRows {
                            metadata,
                            rows_count: rows.len() as CInt,
                            rows_content: result_bytes,
                        });

                        return Frame {
                            version: Version::Response,
                            flags: query_frame.flags.clone(),
                            opcode: Opcode::Result,
                            stream: query_frame.stream,
                            body: response.into_cbytes(),
                            tracing_id: query_frame.tracing_id,
                            warnings: Vec::new(),
                        };
                    }
                }
            }
        }
        unreachable!()
    }
}

impl CassandraCodec2 {
    fn decode_raw(&mut self, src: &mut BytesMut) -> Result<Option<Frame>> {
        trace!("Parsing C* frame");
        let v = parser::parse_frame(src, &self.compressor, &self.current_head);
        match v {
            Ok((r, h)) => {
                self.current_head = h;
                return Ok(r);
            }
            // Note these should be parse errors, not actual protocol errors
            Err(e) => {
                return Err(anyhow!(e));
            }
        }
    }

    fn encode_raw(&mut self, item: Frame, dst: &mut BytesMut) -> Result<()> {
        let buffer = item.into_cbytes();
        dst.put(buffer.as_slice());
        Ok(())
    }

    fn value_to_expr(v: &Value) -> SQLValue {
        return match v {
            Value::NULL => SQLValue::Null,
            Value::Bytes(b) => SQLValue::SingleQuotedString(String::from_utf8(b.to_vec()).unwrap()), // todo: this is definitely wrong
            Value::Strings(s) => SQLValue::SingleQuotedString(s.clone()),
            Value::Integer(i) => SQLValue::Number(i.to_string()),
            Value::Float(f) => SQLValue::Number(f.to_string()),
            Value::Boolean(b) => SQLValue::Boolean(*b),
            Value::Timestamp(t) => SQLValue::Timestamp(t.to_rfc2822()),
            _ => SQLValue::Null,
        };
    }

    fn value_to_bind(_v: &Value) -> SQLValue {
        //TODO fix bind handling
        SQLValue::SingleQuotedString("XYz-1-zYX".to_string())
    }

    fn expr_to_value(v: &SQLValue) -> Value {
        return match v {
            SQLValue::Number(v)
            | SQLValue::SingleQuotedString(v)
            | SQLValue::NationalStringLiteral(v)
            | SQLValue::HexStringLiteral(v)
            | SQLValue::Date(v)
            | SQLValue::Time(v) => Value::Strings(format!("{:?}", v)),
            SQLValue::Timestamp(v) => {
                if let Ok(r) = DateTime::from_str(v.as_str()) {
                    return Value::Timestamp(r);
                }
                Value::Strings(format!("{:?}", v))
            }
            SQLValue::Boolean(v) => Value::Boolean(*v),
            _ => Value::Strings("NULL".to_string()),
        };
    }

    fn expr_to_string<'a>(v: &'a SQLValue) -> String {
        return match v {
            SQLValue::Number(v)
            | SQLValue::SingleQuotedString(v)
            | SQLValue::NationalStringLiteral(v)
            | SQLValue::HexStringLiteral(v)
            | SQLValue::Date(v)
            | SQLValue::Time(v)
            | SQLValue::Timestamp(v) => format!("{:?}", v),
            SQLValue::Boolean(v) => format!("{:?}", v),
            _ => "NULL".to_string(),
        };
    }

    fn rebuild_binops_tree<'a>(
        node: &'a mut Expr,
        map: &'a mut HashMap<String, Value>,
        use_bind: bool,
    ) {
        match node {
            BinaryOp { left, op, right } => match op {
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
            },
            _ => {}
        }
    }

    fn binary_ops_to_hashmap<'a>(node: &'a Expr, map: &'a mut HashMap<String, Value>) {
        match node {
            BinaryOp { left, op, right } => match op {
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
            },
            _ => {}
        }
    }

    fn get_column_values(expr: &SetExpr) -> Vec<String> {
        let mut cumulator: Vec<String> = Vec::new();
        match expr {
            SetExpr::Values(v) => {
                for value in &v.0 {
                    for ex in value {
                        match ex {
                            Expr::Value(v) => {
                                cumulator.push(CassandraCodec2::expr_to_string(v).clone());
                            }
                            _ => {}
                        }
                    }
                }
            }
            _ => {}
        }
        return cumulator;
    }

    pub fn rebuild_query_string_from_ast(message: &mut QueryMessage) {
        if let QueryMessage {
            original: _,
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
            original: _,
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
                                        0: namespace.deref().clone(),
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

    fn parse_query_string(
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
                    Statement::Query(q) => match q.body.borrow() {
                        SetExpr::Select(s) => {
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
                        _ => {}
                    },
                    Insert {
                        table_name,
                        columns,
                        source,
                    } => {
                        namespace = table_name.0.clone();
                        let values = CassandraCodec2::get_column_values(&source.body);
                        for (i, c) in columns.iter().enumerate() {
                            projection.push(c.clone());
                            match values.get(i) {
                                Some(v) => {
                                    colmap.insert(c.to_string(), Value::Strings(v.clone()));
                                }
                                None => {} //TODO some error
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

        return ParsedCassandraQueryString {
            namespace: Some(namespace),
            colmap: Some(colmap),
            projection: Some(projection),
            primary_key,
            ast: ast,
        };
    }

    fn build_response_message(frame: Frame, matching_query: Option<QueryMessage>) -> Message {
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

        return Message::Response(QueryResponse {
            matching_query,
            original: RawFrame::CASSANDRA(frame),
            result,
            error,
            response_meta: None,
        });
    }

    pub fn process_cassandra_frame(&self, frame: Frame) -> Message {
        if self.bypass {
            return Message::Bypass(RawMessage {
                original: RawFrame::CASSANDRA(frame),
            });
        }

        return match frame.opcode {
            Opcode::Query => {
                if let Ok(body) = frame.get_body() {
                    if let ResponseBody::Query(brq) = body {
                        let parsed_string = CassandraCodec2::parse_query_string(
                            brq.query.clone().into_plain(),
                            &self.pk_col_map,
                        );
                        if parsed_string.ast.is_none() {
                            // TODO: Currently this will probably catch schema changes that don't match
                            // what the SQL parser expects
                            return Message::Bypass(RawMessage {
                                original: RawFrame::CASSANDRA(frame),
                            });
                        }
                        return Message::Query(QueryMessage {
                            original: RawFrame::CASSANDRA(frame),
                            query_string: brq.query.into_plain(),
                            namespace: parsed_string.namespace.unwrap(),
                            primary_key: parsed_string.primary_key,
                            query_values: parsed_string.colmap,
                            projection: parsed_string.projection,
                            query_type: QueryType::Read,
                            ast: parsed_string.ast.map(|s| ASTHolder::SQL(s)),
                        });
                    }
                }
                return Message::Bypass(RawMessage {
                    original: RawFrame::CASSANDRA(frame),
                });
            }
            Opcode::Result => CassandraCodec2::build_response_message(frame, None),
            Opcode::Error => {
                if let Ok(body) = frame.get_body() {
                    if let ResponseBody::Error(e) = body {
                        return Message::Response(QueryResponse {
                            matching_query: None,
                            original: RawFrame::CASSANDRA(frame),
                            result: None,
                            error: Some(Value::Strings(e.message.as_plain())),
                            response_meta: None,
                        });
                    }
                }
                return Message::Bypass(RawMessage {
                    original: RawFrame::CASSANDRA(frame),
                });
            }
            _ => {
                return Message::Bypass(RawMessage {
                    original: RawFrame::CASSANDRA(frame),
                });
            }
        };
    }
}

impl Decoder for CassandraCodec2 {
    type Item = Message;
    type Error = anyhow::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        return Ok(self
            .decode_raw(src)?
            .map(|f| self.process_cassandra_frame(f)));
    }
}

impl Encoder<Message> for CassandraCodec2 {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: Message,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        match item {
            Message::Modified(modified_message) => {
                match *modified_message {
                    Message::Bypass(_) => {
                        //TODO: throw error -> we should not be modifing a bypass message
                        unimplemented!()
                    }
                    Message::Query(q) => {
                        return self.encode_raw(
                            CassandraCodec2::build_cassandra_query_frame(
                                q,
                                Consistency::LocalQuorum,
                            ),
                            dst,
                        );
                    }
                    Message::Response(r) => {
                        return self
                            .encode_raw(CassandraCodec2::build_cassandra_response_frame(r), dst);
                    }
                    Message::Modified(_) => {
                        //TODO: throw error -> we should not have a nested modified message
                        unimplemented!()
                    }
                    Message::Bulk(messages) => {
                        for message in messages {
                            self.encode(message, dst)?
                        }
                        return Ok(());
                    }
                }
            }

            Message::Query(qm) => {
                if let RawFrame::CASSANDRA(frame) = qm.original {
                    return self.encode_raw(frame, dst);
                } else {
                    //TODO throw error
                    unimplemented!()
                }
            }
            Message::Response(resp) => {
                if let RawFrame::CASSANDRA(frame) = resp.original {
                    return self.encode_raw(frame, dst);
                } else {
                    //TODO throw error
                    unimplemented!()
                }
            }
            Message::Bypass(resp) => {
                if let RawFrame::CASSANDRA(frame) = resp.original {
                    return self.encode_raw(frame, dst);
                } else {
                    //TODO throw error
                    unimplemented!()
                }
            }
            Message::Bulk(messages) => {
                for message in messages {
                    self.encode(message, dst)?
                }
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod cassandra_protocol_tests {
    use crate::message::{ASTHolder, Message, QueryMessage};
    use crate::protocols::cassandra_protocol2::CassandraCodec2;
    use bytes::BytesMut;
    use hex_literal::hex;
    use rdkafka::message::ToBytes;
    use std::collections::HashMap;
    use tokio_util::codec::{Decoder, Encoder};

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

    fn build_bytesmut(slice: &[u8]) -> BytesMut {
        let mut v: Vec<u8> = Vec::new();
        v.extend_from_slice(slice);
        return BytesMut::from(v.to_bytes());
    }

    fn test_frame(codec: &mut CassandraCodec2, raw_frame: &[u8]) {
        let mut bytes: BytesMut = build_bytesmut(raw_frame);
        if let Ok(Some(message)) = codec.decode(&mut bytes) {
            let mut dest: BytesMut = BytesMut::new();
            if let Ok(()) = codec.encode(message, &mut dest) {
                assert_eq!(build_bytesmut(raw_frame), dest)
            }
        } else {
            panic!("Could not decode frame");
        }
    }

    #[test]
    fn test_startup_codec() {
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
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
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
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
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
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
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
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
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );
        let mut codec = CassandraCodec2::new(pk_map, false);
        test_frame(&mut codec, &QUERY_BYTES);
    }

    fn remove_whitespace(s: &mut String) {
        s.retain(|c| !c.is_whitespace());
    }

    #[test]
    fn test_query_codec_ast_builder() {
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );

        let mut codec = CassandraCodec2::new(pk_map, false);
        let mut bytes: BytesMut = build_bytesmut(&QUERY_BYTES);
        if let Ok(Some(message)) = codec.decode(&mut bytes) {
            if let Message::Query(QueryMessage {
                original: _,
                query_string,
                namespace: _,
                primary_key: _,
                query_values: _,
                projection: _,
                query_type: _,
                ast: Some(ASTHolder::SQL(ast)),
            }) = message
            {
                println!("{}", query_string);
                println!("{}", ast);
                assert_eq!(
                    remove_whitespace(&mut format!("{}", query_string)),
                    remove_whitespace(&mut format!("{}", ast))
                );
            }
        } else {
            panic!("Could not decode frame");
        }
    }
}
