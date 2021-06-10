use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;

use anyhow::{anyhow, Result};
use bytes::{BufMut, BytesMut};
use cassandra_proto::frame::frame_response::ResponseBody;
use cassandra_proto::frame::frame_result::{
    BodyResResultRows, ColSpec, ColType, ColTypeOption, ResResultBody, RowsMetadata,
};
use cassandra_proto::frame::{parser, Flag, Frame, IntoBytes, Opcode, Version};
use cassandra_proto::types::value::Value as CValue;
use cassandra_proto::types::{CBytes, CInt, CString};

use crate::ast::ASTHolder;
use crate::RawFrame;
use crate::{Message, MessageDetails, Messages, QueryMessage, QueryResponse, QueryType, Value};
use byteorder::{BigEndian, WriteBytesExt};
use cassandra_proto::consistency::Consistency;
use cassandra_proto::query::QueryValues;
use chrono::DateTime;
use sqlparser::ast::Expr::{BinaryOp, Identifier};
use sqlparser::ast::Statement::{Delete, Insert, Update};
use sqlparser::ast::{
    BinaryOperator, Expr, ObjectName, Select, SelectItem, SetExpr, Statement, TableFactor,
    Value as SQLValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct ParsedCassandraQueryString {
    pub namespace: Option<Vec<String>>,
    pub colmap: Option<HashMap<String, Value>>,
    pub projection: Option<Vec<String>>,
    pub primary_key: HashMap<String, Value>,
    pub ast: Option<Statement>,
}

pub fn value_to_expr(v: &Value) -> SQLValue {
    match v {
        Value::NULL => SQLValue::Null,
        Value::Bytes(b) => SQLValue::SingleQuotedString(String::from_utf8(b.to_vec()).unwrap()), // todo: this is definitely wrong
        Value::Strings(s) => SQLValue::SingleQuotedString(s.clone()),
        Value::Integer(i) => SQLValue::Number(i.to_string()),
        Value::Float(f) => SQLValue::Number(f.to_string()),
        Value::Boolean(b) => SQLValue::Boolean(*b),
        Value::Timestamp(t) => SQLValue::Timestamp(t.to_rfc2822()),
        _ => SQLValue::Null,
    }
}

fn value_to_bind(_v: &Value) -> SQLValue {
    //TODO fix bind handling
    SQLValue::SingleQuotedString("XYz-1-zYX".to_string())
}

pub fn build_cassandra_query_frame(
    mut query: QueryMessage,
    default_consistency: Consistency,
) -> Frame {
    rebuild_ast_in_message(&mut query);
    rebuild_query_string_from_ast(&mut query);
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
                            tablename: Some(CString::new(query.namespace.get(1).unwrap().clone())),
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
                                        let _ = temp.write_i32::<BigEndian>(*x as i32).unwrap();
                                        temp
                                        // (x.clone() as CInt).into_cbytes()
                                    }
                                    Value::Timestamp(x) => Vec::from(x.to_rfc2822().as_bytes()),
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
    unreachable!()
}

fn expr_to_value(v: &SQLValue) -> Value {
    match v {
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
    }
}

fn expr_to_string(v: &SQLValue) -> String {
    match v {
        SQLValue::Number(v)
        | SQLValue::SingleQuotedString(v)
        | SQLValue::NationalStringLiteral(v)
        | SQLValue::HexStringLiteral(v)
        | SQLValue::Date(v)
        | SQLValue::Time(v)
        | SQLValue::Timestamp(v) => format!("{:?}", v),
        SQLValue::Boolean(v) => format!("{:?}", v),
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
                rebuild_binops_tree(left, map, use_bind);
                rebuild_binops_tree(right, map, use_bind);
            }
            BinaryOperator::Eq => {
                if let Identifier(i) = left.borrow_mut() {
                    if let Expr::Value(v) = right.borrow_mut() {
                        if let Some((_, new_v)) = map.get_key_value(&i.to_string()) {
                            if use_bind {
                                let _ = std::mem::replace(v, value_to_bind(new_v));
                            } else {
                                let _ = std::mem::replace(v, value_to_expr(new_v));
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
                binary_ops_to_hashmap(left, map);
                binary_ops_to_hashmap(right, map);
            }
            BinaryOperator::Eq => {
                if let Identifier(i) = left.borrow() {
                    if let Expr::Value(v) = right.borrow() {
                        map.insert(i.to_string(), expr_to_value(v));
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
                    cumulator.push(expr_to_string(v).clone());
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
                                    0: namespace.deref().clone(),
                                },
                            );
                        }
                    }

                    //Rebuild selection
                    // TODO allow user control of bind
                    if let Some(selection) = selection {
                        rebuild_binops_tree(selection, query_values, true);
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

pub fn parse_query_string(
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
                            binary_ops_to_hashmap(sel, colmap.borrow_mut());
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
                    let values = get_column_values(&source.body);
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
                            let converted_value = expr_to_value(v.borrow());
                            colmap.insert(assignment.id.clone(), converted_value);
                        }
                    }
                    if let Some(s) = selection {
                        binary_ops_to_hashmap(s, &mut primary_key);
                    }
                    // projection = ;
                }
                Delete {
                    table_name,
                    selection,
                } => {
                    namespace = table_name.0.clone();
                    if let Some(s) = selection {
                        binary_ops_to_hashmap(s, &mut primary_key);
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

pub fn build_response_message(frame: Frame, matching_query: Option<QueryMessage>) -> Messages {
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

    Messages::new_single_response(
        QueryResponse {
            matching_query,
            result,
            error,
            response_meta: None,
        },
        false,
        RawFrame::Cassandra(frame),
    )
}
