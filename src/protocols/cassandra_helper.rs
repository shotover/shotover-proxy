#![feature(box_patterns)]

use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::str::FromStr;

use crate::message::{Message, QueryMessage, QueryResponse, QueryType, RawMessage, Value};
use crate::protocols::cassandra_protocol2::RawFrame;
use cassandra_proto::frame::frame_response::ResponseBody;
use cassandra_proto::frame::{Frame, Opcode};
use chrono::DateTime;
use sqlparser::ast::Expr::{BinaryOp, Identifier};
use sqlparser::ast::Statement::{Delete, Insert, Query, Update};
use sqlparser::ast::{
    BinaryOperator, Expr, ObjectName, Select, SelectItem, SetExpr, Statement, TableFactor,
    Value as SQLValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::ops::{Deref, DerefMut};

fn value_to_expr(v: &Value) -> SQLValue {
    return match v {
        Value::NULL => SQLValue::Null,
        Value::Bytes(b) => SQLValue::SingleQuotedString(String::from_utf8(b.to_vec()).unwrap()), // todo: this is definitely wrong
        Value::Strings(s) => SQLValue::SingleQuotedString(s.clone()),
        Value::Integer(i) => SQLValue::Number(i.to_string()),
        Value::Float(f) => SQLValue::Number(f.to_string()),
        Value::Boolean(b) => SQLValue::Boolean(b.clone()),
        Value::Timestamp(t) => SQLValue::Timestamp(t.to_rfc2822()),
        _ => SQLValue::Null,
    };
}

fn value_to_bind(v: &Value) -> SQLValue {
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

fn build_key(namespace: String, pks: &Vec<String>, col_map: &HashMap<String, &String>) -> String {
    let mut s: String = String::new();
    s.push_str(namespace.as_str());
    for pk in pks {
        if let Some(v) = col_map.get(pk) {
            s.push_str(v);
        }
    }
    return s;
}

fn rebuild_binops_tree<'a>(
    node: &'a mut Expr,
    map: &'a mut HashMap<String, Value>,
    use_bind: bool,
) {
    match node {
        BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                rebuild_binops_tree(left, map, use_bind);
                rebuild_binops_tree(right, map, use_bind);
            }
            BinaryOperator::Eq => {
                if let Identifier(i) = left.borrow_mut() {
                    if let Expr::Value(v) = right.borrow_mut() {
                        if let Some((new_i, new_v)) = map.get_key_value(&i.to_string()) {
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
        },
        _ => {}
    }
}

fn binary_ops_to_hashmap<'a>(node: &'a Expr, map: &'a mut HashMap<String, Value>) {
    match node {
        BinaryOp { left, op, right } => match op {
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
        },
        _ => {}
    }
}

fn expr_value_to_string(node: &Expr) -> String {
    if let Expr::Value(v) = node {
        return expr_to_string(v).clone();
    }
    "".to_string()
}

struct ParsedCassandraQueryString {
    namespace: Option<Vec<String>>,
    colmap: Option<HashMap<String, Value>>,
    projection: Option<Vec<String>>,
    primary_key: HashMap<String, Value>,
    ast: Option<Statement>,
}

fn get_column_values(expr: &SetExpr) -> Vec<String> {
    let mut cumulator: Vec<String> = Vec::new();
    match expr {
        SetExpr::Values(v) => {
            for value in &v.0 {
                for ex in value {
                    match ex {
                        Expr::Value(v) => {
                            cumulator.push(expr_to_string(v).clone());
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
        original,
        query_string,
        namespace,
        primary_key,
        query_values: Some(query_values),
        projection: Some(qm_projection),
        query_type,
        ast: Some(ast),
    } = message
    {
        let new_query_string = format!("{}", ast);
        let _ = std::mem::replace(query_string, new_query_string);
    }
}

pub fn rebuild_ast_in_message(message: &mut QueryMessage) {
    if let QueryMessage {
        original,
        query_string,
        namespace,
        primary_key,
        query_values: Some(query_values),
        projection: Some(qm_projection),
        query_type,
        ast: Some(ast),
    } = message
    {
        match ast {
            Statement::Query(query) => {
                if let SetExpr::Select(select) = &mut query.body {
                    let Select {
                        distinct,
                        projection,
                        from,
                        selection,
                        group_by,
                        having,
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
                            alias,
                            args,
                            with_hints,
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

fn parse_query_string<'a>(
    query_string: String,
    pk_col_map: &HashMap<String, Vec<String>>,
) -> ParsedCassandraQueryString {
    let dialect = GenericDialect {}; //TODO write CQL dialect
    let mut namespace: Vec<String> = Vec::new();
    let mut colmap: HashMap<String, Value> = HashMap::new();
    let mut projection: Vec<String> = Vec::new();
    let mut primary_key: HashMap<String, Value> = HashMap::new();
    let mut ast: Option<Statement> = None;
    let foo = Parser::parse_sql(&dialect, query_string.clone());
    //TODO handle pks
    // println!("{:#?}", foo);

    //TODO: We absolutely don't handle multiple statements despite this loop indicating otherwise
    // for statement in ast_list.iter() {
    if let Ok(ast_list) = foo {
        if let Some(statement) = ast_list.get(0) {
            ast = Some(statement.clone());
            match statement {
                Statement::Query(q) => match q.body.borrow() {
                    SetExpr::Select(s) => {
                        projection = s.projection.iter().map(|s| s.to_string()).collect();
                        if let TableFactor::Table {
                            name,
                            alias,
                            args,
                            with_hints,
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
                    _ => {}
                },
                Insert {
                    table_name,
                    columns,
                    source,
                } => {
                    namespace = table_name.0.clone();
                    let values = get_column_values(&source.body);
                    for (i, c) in columns.iter().enumerate() {
                        projection.push(c.clone());
                        match values.get(i) {
                            Some(v) => {
                                let key = c.to_string();
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

    return ParsedCassandraQueryString {
        namespace: Some(namespace),
        colmap: Some(colmap),
        projection: Some(projection),
        primary_key,
        ast: ast,
    };
}

pub fn process_cassandra_frame(
    mut frame: Frame,
    pk_col_map: &HashMap<String, Vec<String>>,
) -> Message {
    return match frame.opcode {
        Opcode::Query => {
            if let Ok(body) = frame.get_body() {
                if let ResponseBody::Query(brq) = body {
                    let parsed_string =
                        parse_query_string(brq.query.clone().into_plain(), pk_col_map);
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
                        ast: parsed_string.ast,
                    });
                }
            }
            return Message::Bypass(RawMessage {
                original: RawFrame::CASSANDRA(frame),
            });
        }
        Opcode::Result => Message::Response(QueryResponse {
            matching_query: None,
            original: RawFrame::CASSANDRA(frame.clone()),
            result: None,
            error: None,
        }),
        _ => {
            return Message::Bypass(RawMessage {
                original: RawFrame::CASSANDRA(frame),
            });
        }
    };
}
