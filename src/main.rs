#![warn(rust_2018_idioms)]
#![recursion_limit="256"]

use tokio::prelude::*;
use tokio::stream::{ StreamExt};
use tokio_util::codec::{Framed};

use futures::FutureExt;
use futures::SinkExt;
use futures::{pin_mut, select};

use std::env;
use std::error::Error;

use rust_practice::cassandra_protocol::{CassandraCodec, CassandraFrame, MessageType, Direction, RawFrame};
use rust_practice::transforms::chain::{Transform, TransformChain, Wrapper, ChainResponse};
use rust_practice::transforms::{NoOp, Printer, QueryTypeFilter, Forward, SimpleRedisCache};
use rust_practice::message::{QueryType, Message, QueryMessage, QueryResponse, Value};
use rust_practice::message::Message::{Query, Response};
use rust_practice::cassandra_protocol::RawFrame::CASSANDRA;

use tokio::net::{TcpListener, TcpStream};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use sqlparser::ast::{SetExpr, TableFactor, Value as SQLValue, Expr, Statement, BinaryOperator};
use sqlparser::ast::Statement::{Insert, Update, Delete};
use sqlparser::ast::Expr::{Identifier, BinaryOp};
use std::borrow::{Borrow, BorrowMut};
use chrono::DateTime;
use std::str::FromStr;
use bytes::Buf;
use futures::executor::block_on;
use redis::aio::MultiplexedConnection;
use tokio::runtime::Runtime;


struct Config {

}

#[tokio::main(core_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    let listen_addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9043".to_string());
    let server_addr = env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:9042".to_string());

    println!("Listening on: {}", listen_addr);
    println!("Proxying to: {}", server_addr);

    let mut listener = TcpListener::bind(listen_addr).await?;

    while let Ok((inbound, _)) = listener.accept().await {
        println!("Connection received from {:?}", inbound.peer_addr());

        let messages = Framed::new(inbound, CassandraCodec::new());
        let outbound_stream = TcpStream::connect(server_addr.clone()).await?;
        let outbound_framed_codec = Framed::new(outbound_stream, CassandraCodec::new());

        let transfer = transfer(messages, outbound_framed_codec).map(|r| {
            if let Err(e) = r {
                println!("Failed to transfer; error={}", e);
            }
        });

        tokio::spawn(transfer);
    }

    Ok(())
}

fn expr_to_value(v: &SQLValue) -> Value {
    return match v {
        SQLValue::Number(v) |
        SQLValue::SingleQuotedString(v) |
        SQLValue::NationalStringLiteral(v) |
        SQLValue::HexStringLiteral(v) |
        SQLValue::Date(v) |
        SQLValue::Time(v) => {
            Value::Strings(format!("{:?}", v))
        },
        SQLValue::Timestamp(v) => {
            if let Ok(r) = DateTime::from_str(v.as_str()) {
                return Value::Timestamp(r);
            }
            Value::Strings(format!("{:?}", v))
        }
        SQLValue::Boolean(v) => {
            Value::Boolean(*v)
        }
        _ => {
            Value::Strings("NULL".to_string())
        }
    }
}

fn expr_to_string<'a>(v: &'a SQLValue) -> String {
    return match v {
        SQLValue::Number(v) |
        SQLValue::SingleQuotedString(v) |
        SQLValue::NationalStringLiteral(v) |
        SQLValue::HexStringLiteral(v) |
        SQLValue::Date(v) |
        SQLValue::Time(v) |
        SQLValue::Timestamp(v) => {
            format!("{:?}", v)
        },
        SQLValue::Boolean(v) => {
            format!("{:?}", v)
        }
        _ => {
            "NULL".to_string()
        }
    }
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

fn binary_ops_to_hashmap<'a>(node: &'a Expr, map: &'a mut HashMap<String, Value>)  {
    match node {
        BinaryOp{left, op, right} => {
            match op {
                BinaryOperator::And => {
                    binary_ops_to_hashmap(left, map);
                    binary_ops_to_hashmap(right, map);
                }
                BinaryOperator::Eq=> {
                    if let Identifier(i) = left.borrow() {
                        if let Expr::Value(v) = right.borrow() {
                            map.insert(i.to_string(), expr_to_value(v));
                        }

                    }
                }
                _ => {}
            }
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
    ast: Option<Statement>
}

fn getColumnValues(expr: &SetExpr) -> Vec<String> {
        let mut cumulator: Vec<String> = Vec::new();
         match expr {
             SetExpr::Values(v) => {
                 for value in &v.0 {
                     for ex in value {
                         match ex {
                             Expr::Value(v) => {
                                 cumulator.push(expr_to_string(v).clone());
                             },
                             _ => {}
                         }
                     }
                 }
             },
             _ => {}
         }
        return cumulator;
}


fn parse_query_string<'a>(query_string: String, pk_col_map: &HashMap<String, Vec<String>>) -> ParsedCassandraQueryString {

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
                Statement::Query(q) => {
                    match q.body.borrow() {
                        SetExpr::Select(s) => {
                            projection = s.projection.iter().map(|s| {s.to_string()}).collect();
                            if let TableFactor::Table{name, alias, args, with_hints }  = &s.from.get(0).unwrap().relation {
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
                        },
                        _ => {}
                    }
                },
                Insert {table_name, columns, source} => {
                    namespace = table_name.0.clone();
                    let values = getColumnValues(&source.body);
                    for (i, c) in columns.iter().enumerate() {
                        projection.push(c.clone());
                        match values.get(i) {
                            Some(v) => {
                                let key = c.to_string();
                                colmap.insert(c.to_string(), Value::Strings(v.clone()));
                            },
                            None => {}, //TODO some error
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

                },
                Update {table_name, assignments, selection} => {
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

                },
                Delete {table_name, selection} => {
                    namespace = table_name.0.clone();
                    if let Some(s) = selection {
                        binary_ops_to_hashmap(s, &mut primary_key);
                    }
                    // projection = None;
                },
                _ => {},
            }
        }

    }

    return ParsedCassandraQueryString{
        namespace: Some(namespace),
        colmap: Some(colmap),
        projection: Some(projection),
        primary_key,
        ast: ast
    }

}

fn process_cassandra_frame(mut frame: CassandraFrame, pk_col_map: &HashMap<String, Vec<String>>) -> Message {
    if frame.header.direction == Direction::Request {
        match frame.get_query() {
            Some(q) => {
                let query_string = String::from(String::from_utf8_lossy(q.query_string.bytes()));
                let other = query_string.clone();
                let parsed_string = parse_query_string(other, pk_col_map);

                Message::Query(QueryMessage{
                    original: RawFrame::CASSANDRA(frame),
                    query_string,
                    namespace: parsed_string.namespace.unwrap(),
                    primary_key: parsed_string.primary_key,
                    query_values: parsed_string.colmap,
                    projection: parsed_string.projection,
                    query_type: QueryType::Read,
                    ast: parsed_string.ast
                })
            },
            None => {
                Message::Query(QueryMessage{
                    original: RawFrame::CASSANDRA(frame),
                    query_string: "".to_string(),
                    namespace: Vec::new(),
                    primary_key: HashMap::new(),
                    query_values: None,
                    projection: None,
                    query_type: QueryType::Read,
                    ast: None
                })
            }
        }
    } else {
        Message::Response(QueryResponse{
            matching_query: None,
            original: RawFrame::CASSANDRA(frame),
            result: None,
            error: None
        })
    }
}


// TODO we should allow users to build and define their own topology of transforms/tasks to perform on a single request
// however for the poc we'll just decode the C* frame to a common format, run through a lua script then write to C* and REDIS
async fn process_message<'a, 'c>(mut frame: Wrapper, transforms: &'c TransformChain<'a, 'c>) -> ChainResponse<'c> {
    return transforms.process_request(frame).await
}

async fn transfer<'a>(
    mut inbound: Framed<TcpStream, CassandraCodec>,
    mut outbound: Framed<TcpStream, CassandraCodec>,
) -> Result<(), Box<dyn Error>> {
    let noop_transformer = NoOp::new();
    let printer_transform = Printer::new();
    let query_transform = QueryTypeFilter::new(vec![QueryType::Write]);
    let forward = Forward::new();
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = block_on(client.get_multiplexed_tokio_connection())?;

    let response = redis::cmd("PING").query_async(&mut con).await?;
    println!("REDIS: {:?}", response);

    let redis_cache = SimpleRedisCache::new(con);
    let mut cassandra_ks: HashMap<String, Vec<String>> = HashMap::new();
    // cassandra_ks.insert("system.local".to_string(), vec!["key".to_string()]);
    cassandra_ks.insert("test.simple".to_string(), vec!["pk".to_string()]);
    cassandra_ks.insert("test.clustering".to_string(), vec!["pk".to_string(), "clustering".to_string()]);


    let chain = TransformChain::new(vec![&noop_transformer, &printer_transform, &query_transform, &redis_cache, &forward], "test");
    // Holy snappers this is terrible - seperate out inbound and outbound loops
    // We should probably have a seperate thread for inbound and outbound, but this will probably do. Also not sure on select behavior.
    loop {
        select! {
            i = inbound.next().fuse() => {
                if let Some(result) = i {
                    match result {
                        Ok(message) => {
                            // Current logic assumes that if a message is to be forward upstream, expect something
                            // If the message is going somewhere else, and the original (modified or otherwise) shouldn't be
                            // fowarded, then we should not do anything
                            // If we don't want to forward upstream, then process message should return a response and we'll just
                            // return it to the client instead of forwarding.
                            // This could be something like a spoofed success.
                            let mut frame = Wrapper::new(process_cassandra_frame(message, &cassandra_ks));
                            let pm = process_message(frame, &chain).await;
                            println!("{:?}", pm);
                            if let Ok(modified_message) = pm {
                                match modified_message {
                                    Query(query)    =>  {
                                        //Unwrap c* frame
                                        if let CASSANDRA(f) = query.original {
                                            outbound.send(f).await?
                                        }
                                    },
                                    Response(resp)  =>  {
                                        if let CASSANDRA(f) = resp.original {
                                            inbound.send(f).await?
                                        }
                                    }
                                }
                            }
                            // Process message decided to drop the message so do nothing (warning this may cause client timeouts)
                        }
                        Err(e) => {
                            println!("uh oh! {}", e)
                        }
                    }
                }
            },

            // TODO - allow us to filter responses from the server.
            o = outbound.next().fuse() => {
                if let Some(result) = o {
                    match result {
                        Ok(message) => {
                            // let modified_message = process_message(message, &topology);
                            inbound.send(message).await?;
                        }
                        Err(e) => {
                            println!("uh oh! {}", e)
                        }
                    }
                }
            },
        };
    }
}
