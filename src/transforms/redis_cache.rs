use crate::message::Message::Query as MessageQuery;
use crate::message::{Message, QueryResponse};
use redis::{pipe, AsyncCommands, RedisFuture, RedisResult};
use sqlparser::ast::Statement::*;
use std::collections::HashMap;
use std::iter::Iterator;

use crate::message::Value as MValue;
use crate::transforms::chain::{ Transform, TransformChain, Wrapper};
use std::borrow::Borrow;

use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::transforms::{Transforms, TransformsFromConfig};
use async_trait::async_trait;

use tracing::trace;
use crate::protocols::RawFrame;
use crate::error::{ChainResponse, RequestError};
use anyhow::{anyhow, Result};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisConfig {
    #[serde(rename = "config_values")]
    pub uri: String,
}

#[async_trait]
impl TransformsFromConfig for RedisConfig {
    async fn get_source(
        &self,
        _topics: &TopicHolder,
    ) -> Result<Transforms> {
        Ok(Transforms::RedisCache(SimpleRedisCache::new_from_config(
            &self.uri,
        ).await))
    }
}

#[derive(Clone)]
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

    pub async fn new_from_config(params: &String) -> SimpleRedisCache {
        let client = redis::Client::open(params.clone()).unwrap();
        let con = client.get_multiplexed_tokio_connection().await.unwrap();
        return SimpleRedisCache {
            name: "SimpleRedisCache",
            con,
            tables_to_pks: HashMap::new(),
        };
    }
}

#[async_trait]
impl Transform for SimpleRedisCache {
    async fn transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let message = qd.message.borrow();

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
                    Query(_) => {
                        let mut client_copy = self.con.clone();

                        //TODO: something something what happens if hset fails.
                        // let f: RedisFuture<HashMap<String, String>> = client_copy.hgetall(&qm.get_primary_key());
                        let p = &mut pipe();
                        if let Some(pk) = qm.get_namespaced_primary_key() {
                            if let Some(values) = &qm.projection {
                                for v in values {
                                    p.hget(&pk, v);
                                }
                            }
                        }

                        let result: RedisResult<Vec<String>> =
                            p.query_async(&mut client_copy).await;
                        println!("{:?}", result);

                        if let Ok(ok_result) = result {
                            if !ok_result.is_empty() {
                                //TODO a type translation function should be generalised here
                                let some = ok_result
                                    .into_iter()
                                    .map(|x| serde_json::from_str(x.as_str()).unwrap())
                                    .collect::<Vec<MValue>>();
                                return ChainResponse::Ok(Message::Response(QueryResponse {
                                    matching_query: Some(qm.clone()),
                                    original: RawFrame::NONE,
                                    result: Some(MValue::Rows(vec![some])), //todo: Translate function
                                    error: None,
                                }));
                            }
                        }

                        return self.call_next_transform(qd, t).await;
                    }

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
                    Insert {
                        table_name: _,
                        columns: _,
                        source: _,
                    } => {
                        let mut insert_values: Vec<(String, String)> = Vec::new();

                        if let Some(pk) = qm.get_namespaced_primary_key() {
                            if let Some(value_map) = qm.query_values.borrow() {
                                for (k, v) in value_map {
                                    insert_values
                                        .push((k.clone(), serde_json::to_string(&v).unwrap()));
                                }

                                let mut client_copy = self.con.clone();

                                //TODO: something something what happens if hset fails.

                                let f: RedisFuture<()> =
                                    client_copy.hset_multiple(pk, insert_values.as_slice());

                                // TODO: We update the cache asynchronously - currently errors on cache update are ignored
                                let res = self.call_next_transform(qd, t).await;

                                if let Err(e) = f.await {
                                    trace!( "Cache update failed {:?} !", e);
                                } else {
                                    trace!( "Cache update success !");
                                }

                                return res;
                            }
                        }

                        return self.call_next_transform(qd, t).await;
                    }
                    Update {
                        table_name: _,
                        assignments: _,
                        selection: _,
                    } => {
                        let mut insert_values: Vec<(String, String)> = Vec::new();

                        if let Some(pk) = qm.get_namespaced_primary_key() {
                            if let Some(value_map) = qm.query_values.borrow() {
                                for (k, v) in value_map {
                                    insert_values
                                        .push((k.clone(), serde_json::to_string(&v).unwrap()));
                                }

                                let mut client_copy = self.con.clone();

                                //TODO: something something what happens if hset fails.

                                let f: RedisFuture<()> =
                                    client_copy.hset_multiple(pk, insert_values.as_slice());

                                // TODO: We update the cache asynchronously - currently errors on cache update are ignored
                                let res = self.call_next_transform(qd, t).await;

                                if let Err(e) = f.await {
                                    trace!("Cache update failed {:?} !", e);
                                } else {
                                    trace!("Cache update success !");
                                }

                                return res;
                            }
                        }
                        return self.call_next_transform(qd, t).await;
                    }
                    Delete{ table_name: _, selection: _ } => {
                        let p = &mut pipe();
                        if let Some(pk) = qm.get_namespaced_primary_key() {
                            if let Some(value_map) = qm.query_values.borrow() {
                                for (k, _) in value_map {
                                    p.hdel(pk.clone(), k.clone());
                                }

                                let mut client_copy = self.con.clone();

                                //TODO: await these using a join.
                                let f: RedisResult<Vec<i32>> = p.query_async(&mut client_copy).await;
                                let res = self.call_next_transform(qd, t).await;

                                if let Err(e) = f {
                                    trace!("Cache update failed {:?} !", e);
                                } else {
                                    trace!("Cache update success !");
                                }
                                return res;
                            }
                        }
                        return self.call_next_transform(qd, t).await;
                    }
                    _ => {}
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
