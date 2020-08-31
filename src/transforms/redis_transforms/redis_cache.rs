use crate::message::{ASTHolder, Message, MessageDetails, Messages, QueryResponse};
use redis::{pipe, AsyncCommands, RedisResult};
use sqlparser::ast::Statement::*;
use std::collections::HashMap;
use std::iter::Iterator;

use crate::message::Value as MValue;
use crate::transforms::chain::TransformChain;
use std::borrow::Borrow;

use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};
use async_trait::async_trait;

use crate::error::ChainResponse;
use anyhow::Result;
use core::fmt;
use serde::export::Formatter;
use std::fmt::Debug;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisConfig {
    #[serde(rename = "config_values")]
    pub uri: String,
}

#[async_trait]
impl TransformsFromConfig for RedisConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::RedisCache(
            SimpleRedisCache::new_from_config(&self.uri).await,
        ))
    }
}

#[derive(Clone)]
pub struct SimpleRedisCache {
    name: &'static str,
    con: MultiplexedConnection,
    tables_to_pks: HashMap<String, Vec<String>>,
}

impl Debug for SimpleRedisCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Name: {}, conversions: {:?}",
            self.name, self.tables_to_pks
        )
    }
}

impl SimpleRedisCache {
    //"redis://127.0.0.1/"
    pub fn new(connection: MultiplexedConnection) -> SimpleRedisCache {
        SimpleRedisCache {
            name: "SimpleRedisCache",
            con: connection,
            tables_to_pks: HashMap::new(),
        }
    }

    pub async fn new_from_config(params: &str) -> SimpleRedisCache {
        let client = redis::Client::open(params).unwrap();
        let con = client.get_multiplexed_tokio_connection().await.unwrap();
        SimpleRedisCache {
            name: "SimpleRedisCache",
            con,
            tables_to_pks: HashMap::new(),
        }
    }
}

#[async_trait]
impl Transform for SimpleRedisCache {
    // #[instrument]
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        let mut responses = Messages::new();
        // for message in &qd.message.messages {
        //     let wrapped_message = Wrapper::new_with_next_transform(
        //         Messages::new_from_message(message.clone()),
        //         0,
        //     );
        //     if let MessageDetails::Query(qm) = &message.details {
        //         if qm.primary_key.is_empty() {
        //             responses
        //                 .messages
        //                 .append(&mut t.call_next_transform(wrapped_message).await?.messages);
        //         } else {
        //             if let Some(ASTHolder::SQL(ast)) = &qm.ast {
        //                 match ast {
        //                     Query(_) => {
        //                         let mut client_copy = self.con.clone();
        //
        //                         //TODO: something something what happens if hset fails.
        //                         // let f: RedisFuture<HashMap<String, String>> = client_copy.hgetall(&qm.get_primary_key());
        //                         let p = &mut pipe();
        //                         if let Some(pk) = qm.get_namespaced_primary_key() {
        //                             if let Some(values) = &qm.projection {
        //                                 for v in values {
        //                                     p.hget(&pk, v);
        //                                 }
        //                             }
        //                         }
        //
        //                         let result: RedisResult<Vec<String>> =
        //                             p.query_async(&mut client_copy).await;
        //                         println!("{:?}", result);
        //
        //                         if let Ok(ok_result) = result {
        //                             if !ok_result.is_empty() {
        //                                 //TODO a type translation function should be generalised here
        //                                 let some = ok_result
        //                                     .into_iter()
        //                                     .map(|x| serde_json::from_str(x.as_str()).unwrap())
        //                                     .collect::<Vec<MValue>>();
        //
        //                                 responses.messages.push(Message::new_response(
        //                                     QueryResponse {
        //                                         matching_query: Some(qm.clone()),
        //                                         result: Some(MValue::Rows(vec![some])), //todo: Translate function
        //                                         error: None,
        //                                         response_meta: None,
        //                                     },
        //                                     true,
        //                                     RawFrame::NONE,
        //                                 ));
        //                             }
        //                         } else {
        //                             responses.messages.append(
        //                                 &mut t.call_next_transform(wrapped_message).await?.messages,
        //                             );
        //                         }
        //                     }
        //
        //                     /*
        //                     Query String: INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES ('6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47', 'KRUIKSWIJK', 'Steven')
        //                     AST: [Insert {
        //                             table_name: ObjectName(["cycling", "cyclist_name"]),
        //                             columns: ["id", "lastname", "firstname"],
        //                             source: Query {
        //                                 ctes: [],
        //                                 body: Values(
        //                                     Values(
        //                                         [[Value(SingleQuotedString("6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47")), Value(SingleQuotedString("KRUIKSWIJK")), Value(SingleQuotedString("Steven"))]]
        //                                         )
        //                                       ),
        //                                       order_by: [],
        //                                       limit: None,
        //                                       offset: None,
        //                                       fetch: None }
        //                             }]
        //                     */
        //                     Insert {
        //                         table_name: _,
        //                         columns: _,
        //                         source: _,
        //                     } => {
        //                         let mut insert_values: Vec<(String, String)> = Vec::new();
        //
        //                         if let Some(pk) = qm.get_namespaced_primary_key() {
        //                             if let Some(value_map) = qm.query_values.borrow() {
        //                                 for (k, v) in value_map {
        //                                     insert_values.push((
        //                                         k.clone(),
        //                                         serde_json::to_string(&v).unwrap(),
        //                                     ));
        //                                 }
        //
        //                                 let mut client_copy = self.con.clone();
        //
        //                                 //TODO: something something what happens if hset fails.
        //                                 let (cache_update, chain_r): (
        //                                     RedisResult<()>,
        //                                     ChainResponse,
        //                                 ) = tokio::join!(
        //                                     client_copy.hset_multiple(pk, insert_values.as_slice()),
        //                                     t.call_next_transform(wrapped_message)
        //                                 );
        //
        //                                 responses.messages.append(&mut chain_r?.messages);
        //
        //                                 // TODO: We update the cache asynchronously - currently errors on cache update are ignored
        //
        //                                 if let Err(e) = cache_update {
        //                                     trace!("Cache update failed {:?} !", e);
        //                                 } else {
        //                                     trace!("Cache update success !");
        //                                 }
        //                             }
        //                         } else {
        //                             responses.messages.append(
        //                                 &mut t.call_next_transform(wrapped_message).await?.messages,
        //                             );
        //                         }
        //                     }
        //                     Update {
        //                         table_name: _,
        //                         assignments: _,
        //                         selection: _,
        //                     } => {
        //                         let mut insert_values: Vec<(String, String)> = Vec::new();
        //
        //                         if let Some(pk) = qm.get_namespaced_primary_key() {
        //                             if let Some(value_map) = qm.query_values.borrow() {
        //                                 for (k, v) in value_map {
        //                                     insert_values.push((
        //                                         k.clone(),
        //                                         serde_json::to_string(&v).unwrap(),
        //                                     ));
        //                                 }
        //
        //                                 let mut client_copy = self.con.clone();
        //
        //                                 //TODO: something something what happens if hset fails.
        //
        //                                 let (cache_update, chain_r): (
        //                                     RedisResult<()>,
        //                                     ChainResponse,
        //                                 ) = tokio::join!(
        //                                     client_copy.hset_multiple(pk, insert_values.as_slice()),
        //                                     t.call_next_transform(wrapped_message)
        //                                 );
        //
        //                                 // TODO: We update the cache asynchronously - currently errors on cache update are ignored
        //                                 responses.messages.append(&mut chain_r?.messages);
        //
        //                                 if let Err(e) = cache_update {
        //                                     trace!("Cache update failed {:?} !", e);
        //                                 } else {
        //                                     trace!("Cache update success !");
        //                                 }
        //                             }
        //                         } else {
        //                             responses.messages.append(
        //                                 &mut t.call_next_transform(wrapped_message).await?.messages,
        //                             );
        //                         }
        //                     }
        //                     Delete {
        //                         table_name: _,
        //                         selection: _,
        //                     } => {
        //                         let p = &mut pipe();
        //                         if let Some(pk) = qm.get_namespaced_primary_key() {
        //                             if let Some(value_map) = qm.query_values.borrow() {
        //                                 for k in value_map.keys() {
        //                                     p.hdel(pk.clone(), k.clone());
        //                                 }
        //
        //                                 let mut client_copy = self.con.clone();
        //
        //                                 let (cache_update, chain_r): (
        //                                     RedisResult<Vec<i32>>,
        //                                     ChainResponse,
        //                                 ) = tokio::join!(
        //                                     p.query_async(&mut client_copy),
        //                                     t.call_next_transform(wrapped_message)
        //                                 );
        //
        //                                 // TODO: We update the cache asynchronously - currently errors on cache update are ignored
        //                                 responses.messages.append(&mut chain_r?.messages);
        //
        //                                 if let Err(e) = cache_update {
        //                                     trace!("Cache update failed {:?} !", e);
        //                                 } else {
        //                                     trace!("Cache update success !");
        //                                 }
        //                             }
        //                         } else {
        //                             responses.messages.append(
        //                                 &mut t.call_next_transform(wrapped_message).await?.messages,
        //                             );
        //                         }
        //                     }
        //                     _ => {}
        //                 }
        //             } else {
        //                 responses
        //                     .messages
        //                     .append(&mut t.call_next_transform(wrapped_message).await?.messages);
        //             }
        //         }
        //     }
        // }
        Ok(responses)
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
