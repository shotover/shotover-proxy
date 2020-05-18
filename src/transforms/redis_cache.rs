use std::collections::HashMap;
use crate::message::Message::{Query as MessageQuery};
use crate::message::{Message, QueryResponse};
use redis::{AsyncCommands, RedisFuture, pipe, RedisResult, Commands};
use sqlparser::ast::Statement::*;
use sqlparser::ast::{SetExpr::Values, Expr, SetExpr, Expr::Value as EValue};
use sqlparser::ast::Value;
use std::iter::{Iterator};

use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};
use std::borrow::{Borrow};
use crate::message::{Value as MValue};

use redis::aio::{MultiplexedConnection};
use sqlparser::ast::Expr::{BinaryOp, Identifier};
use std::cell::{RefCell, RefMut};
use std::rc::Rc;
use crate::cassandra_protocol::RawFrame;
use serde::{Serialize, Deserialize};

use async_trait::async_trait;
use tokio::runtime::{Handle};
use crate::transforms::{TransformsFromConfig, Transforms};


#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RedisConfig {
    #[serde(rename = "config_values")]
    pub uri: String
}


#[async_trait]
impl TransformsFromConfig for RedisConfig {
    async fn get_source(&self) -> Transforms {
        unimplemented!()
    }
}

impl From<RedisConfig> for SimpleRedisCache {
    fn from(k: RedisConfig) -> Self {
        SimpleRedisCache::new_from_config(&k.uri)
    }
}


#[derive(Clone, Deserialize)]
#[serde(from = "RedisConfig")]
pub struct SimpleRedisCache {
    name: &'static str,
    con: MultiplexedConnection,
    tables_to_pks: HashMap<String, Vec<String>>,
}

impl SimpleRedisCache {
    //"redis://127.0.0.1/"
    pub fn new(connection: MultiplexedConnection) -> SimpleRedisCache {
        return SimpleRedisCache {
            name: "SimpleRedisCache",
            con: connection,
            tables_to_pks: HashMap::new(),
        };
    }

    pub fn new_from_config(params: &String) -> SimpleRedisCache {
        let client = redis::Client::open(params.clone()).unwrap();
        let con = Handle::current().block_on(client.get_multiplexed_tokio_connection()).unwrap();
        return SimpleRedisCache {
            name: "SimpleRedisCache",
            con,
            tables_to_pks: HashMap::new(),
        }
    }


    // TODO: learn rust macros as this will probably make parsing ASTs a million times easier
    fn getColumnValues(&self, expr: &SetExpr) -> Vec<String> {
        let mut cumulator: Vec<String> = Vec::new();
        match expr {
            Values(v) => {
                for value in &v.0 {
                    for ex in value {
                        match ex {
                            EValue(v) => {
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

}

fn expr_to_string<'a>(v: &'a Value) -> String {
    return match v {
        Value::Number(v) |
        Value::SingleQuotedString(v) |
        Value::NationalStringLiteral(v) |
        Value::HexStringLiteral(v) |
        Value::Date(v) |
        Value::Time(v) |
        Value::Timestamp(v) => {
            format!("{:?}", v)
        },
        Value::Boolean(v) => {
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


fn binary_ops_to_hashmap<'a>(node: &'a Expr, map: &'a Rc<RefCell<HashMap<String, String>>>)  {
    if let BinaryOp{left, op, right} = node {
        match op {
            AND => {
                binary_ops_to_hashmap(left, map);
                binary_ops_to_hashmap(right, map);
            }
            EQUAL=> {
                if let Identifier(i) = left.borrow() {
                    if let EValue(v) = right.borrow() {
                        let mut mut_map: RefMut<_> = (**map).borrow_mut();
                        mut_map.insert(i.to_string(), expr_to_string(v));
                    }

                }
            }
        }
    }
}


#[async_trait]
impl Transform for SimpleRedisCache {
    async fn transform(&self, mut qd: Wrapper, t: & TransformChain) -> ChainResponse {
        let message  = qd.message.borrow();

        // Only handle client requests
        if let MessageQuery(qm) = message {
            if qm.primary_key.is_empty() {
                return self.call_next_transform(qd, t).await;
            }

            for statement in qm.ast.iter() {
                match statement {
                    /*
                     Query String: SELECT id, lastname, teams FROM cycling.cyclist_career_teams WHERE id='5b6962dd-3f90-4c93-8f61-eabfa4a803e2'
                     AST: [Query(Query {
                        ctes: [],
                        body: Select(Select {
                            distinct: false,
                            projection: [UnnamedExpr(Identifier("id")), UnnamedExpr(Identifier("lastname")), UnnamedExpr(Identifier("teams"))],
                            from: [TableWithJoins {
                                relation: Table {
                                    name: ObjectName(["cycling", "cyclist_career_teams"]),
                                    alias: None, args: [],
                                    with_hints: [] },
                                joins: [] }],
                            selection:
                                    Some(BinaryOp {
                                        left: Identifier("id"),
                                        op: Eq,
                                        right: Value(SingleQuotedString("5b6962dd-3f90-4c93-8f61-eabfa4a803e2")) }),
                                group_by: [],
                                having: None }),
                        order_by: [],
                        limit: None,
                        offset: None,
                        fetch: None
                    })]

                    */
                    Query(q) => {
                        let mut client_copy = self.con.clone();

                        //TODO: something something what happens if hset fails.
                        // let f: RedisFuture<HashMap<String, String>> = client_copy.hgetall(&qm.get_primary_key());
                        let mut p = &mut pipe();
                        if let Some(pk) = qm.get_namespaced_primary_key() {
                            if let Some(values) = &qm.projection {
                                for v in values {
                                    p.hget(&pk, v);
                                }
                            }
                        }

                        // println!("{:?}", p);
                        let result: RedisResult<(Vec<String>)> = p.query_async(&mut client_copy).await;
                        println!("{:?}", result);

                        if let Ok(ok_result) = result {
                            if !ok_result.is_empty() {

                                //TODO a type translation function should be generalised here
                                let some = ok_result.into_iter().map(|x| serde_json::from_str(x.as_str()).unwrap()).collect::<Vec<MValue>>();
                                return ChainResponse::Ok(Message::Response(QueryResponse{
                                    matching_query: Some(qm.clone()),
                                    original: RawFrame::NONE,
                                    result: Some(MValue::Rows(vec![some])), //todo: Translate function
                                    error: None,
                                }))
                            }

                        }

                        return self.call_next_transform(qd, t).await

                    },

                    /*
                    Query String: INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES ('6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47', 'KRUIKSWIJK', 'Steven')
                    AST: [Insert {
                            table_name: ObjectName(["cycling", "cyclist_name"]),
                            columns: ["id", "lastname", "firstname"],
                            source: Query {
                                ctes: [],
                                body: Values(
                                    Values(
                                        [[Value(SingleQuotedString("6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47")), Value(SingleQuotedString("KRUIKSWIJK")), Value(SingleQuotedString("Steven"))]]
                                        )
                                      ),
                                      order_by: [],
                                      limit: None,
                                      offset: None,
                                      fetch: None }
                            }]

                    */
                    Insert {table_name, columns, source} => {
                        let mut insert_values: Vec<(String, String)> = Vec::new();

                        if let Some(pk) = qm.get_namespaced_primary_key() {
                            if let Some(value_map) = qm.query_values.borrow()  {
                                for (k, v) in value_map {
                                    insert_values.push((k.clone(), serde_json::to_string(&v).unwrap()));
                                }

                                let mut client_copy = self.con.clone();

                                //TODO: something something what happens if hset fails.

                                let f: RedisFuture<()>  = client_copy.hset_multiple(pk, insert_values.as_slice());

                                // TODO: We update the cache asynchronously - currently errors on cache update are ignored
                                let res = self.call_next_transform(qd, t).await;

                                if let Err(e) = f.await {
                                    println!("Cache update failed {:?} !", e);
                                } else {
                                    println!("Cache update success !");

                                }

                                return res;
                            }
                        }

                        return self.call_next_transform(qd, t).await;

                    },
                    Update {table_name, assignments, selection} => {},
                    Delete {table_name, selection} => {},
                    _ => {},


                }
            }


        }
//        match message.
        self.call_next_transform(qd, t).await
    }


    fn get_name(&self) -> &'static str {
        self.name
    }
}

