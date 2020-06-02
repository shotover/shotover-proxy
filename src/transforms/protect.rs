use crate::config::topology::TopicHolder;
use crate::config::ConfigError;
use crate::message::Value;
use crate::message::Value::Rows;
use crate::message::{Message, Protected, QueryMessage, QueryResponse, QueryType};
use crate::runtimes::lua::LuaRuntime;
use crate::transforms::chain::{ChainResponse, RequestError, Transform, TransformChain, Wrapper};
use crate::transforms::{Transforms, TransformsFromConfig};
use async_trait::async_trait;
use core::mem;
use rlua::{Lua, ToLua, UserData, UserDataMethods};
use rlua_serde;
use serde::{Deserialize, Serialize};
use slog::Logger;
use slog::{info, warn};
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::Key;
use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::ops::Deref;

#[derive(Clone)]
pub struct Protect {
    name: &'static str,
    logger: Logger,
    key: Key,
    keyspace_table_columns: HashMap<String, HashMap<String, Vec<String>>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ProtectConfig {
    pub key: Option<Key>,
    pub keyspace_table_columns: HashMap<String, HashMap<String, Vec<String>>>,
}

#[async_trait]
impl TransformsFromConfig for ProtectConfig {
    async fn get_source(
        &self,
        _: &TopicHolder,
        logger: &Logger,
    ) -> Result<Transforms, ConfigError> {
        let key = match &self.key {
            None => panic!("No encryption key provided"),
            Some(k) => k.clone(),
        };

        Ok(Transforms::Protect(Protect {
            name: "protect",
            logger: logger.clone(),
            key,
            keyspace_table_columns: self.keyspace_table_columns.clone(),
        }))
    }
}

#[async_trait]
impl Transform for Protect {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        if let Message::Query(qm) = &mut qd.message {
            // Encrypt the writes
            if QueryType::Write == qm.query_type {
                if qm.namespace.len() == 2 {
                    if let Some((keyspace, tables)) = self
                        .keyspace_table_columns
                        .get_key_value(qm.namespace.get(0).unwrap())
                    {
                        if let Some((table, columns)) =
                            tables.get_key_value(qm.namespace.get(1).unwrap())
                        {
                            if let Some(query_values) = &mut qm.query_values {
                                for col in columns {
                                    if let Some(value) = query_values.get_mut(col) {
                                        let mut protected = Protected::Plaintext(value.clone());
                                        protected = protected.protect(&self.key);
                                        let _ = mem::replace(value, protected.into());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        let mut result = self.call_next_transform(qd, t).await?;
        // this used to be worse https://doc.rust-lang.org/book/ch18-03-pattern-syntax.html#destructuring-structs-and-tuples
        // todo: destructure the above bracket mountain as below
        if let Message::Response(QueryResponse {
            matching_query:
                Some(QueryMessage {
                    original: _,
                    query_string,
                    namespace,
                    primary_key,
                    query_values,
                    projection: Some(projection),
                    query_type,
                    ast,
                }),
            original: _,
            result: Some(Rows(rows)),
            error: None,
        }) = &mut result
        {
            if namespace.len() == 2 {
                if let Some((keyspace, tables)) = self
                    .keyspace_table_columns
                    .get_key_value(namespace.get(0).unwrap())
                {
                    if let Some((table, protect_columns)) =
                        tables.get_key_value(namespace.get(1).unwrap())
                    {
                        let mut positions: Vec<usize> = Vec::new();
                        for (i, p) in projection.iter().enumerate() {
                            if protect_columns.contains(p) {
                                positions.push(i);
                            }
                        }
                        for row in rows {
                            for index in &positions {
                                if let Some(v) = row.get_mut(*index) {
                                    if let Value::Bytes(_) = v {
                                        let mut protected =
                                            Protected::from_encrypted_bytes_value(v.borrow())
                                                .unwrap();
                                        let new_value: Value = protected.unprotect(&self.key);
                                        let _ = mem::replace(v, new_value);
                                    } else {
                                        warn!(self.logger, "Tried decrypting non-blob column")
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return Ok(result);
    }

    fn get_name(&self) -> &'static str {
        "protect"
    }
}

#[cfg(test)]
mod protect_transform_tests {
    use crate::config::topology::TopicHolder;
    use crate::message::{Message, QueryMessage, QueryResponse, QueryType, Value};
    use crate::protocols::cassandra_protocol2::RawFrame;
    use crate::transforms::chain::{ChainResponse, Transform, TransformChain, Wrapper};
    use crate::transforms::null::Null;
    use crate::transforms::printer::Printer;
    use crate::transforms::protect::ProtectConfig;
    use crate::transforms::{Transforms, TransformsFromConfig};
    use async_trait::async_trait;
    use slog::info;
    use sloggers::terminal::{Destination, TerminalLoggerBuilder};
    use sloggers::types::Severity;
    use sloggers::Build;
    use sodiumoxide::crypto::secretbox;
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;
    use crate::transforms::test_transforms::ReturnerTransform;
    use cassandra_proto::frame::{Frame, Flag};
    use cassandra_proto::consistency::Consistency;
    use crate::protocols::cassandra_helper::process_cassandra_frame;

    #[tokio::test(threaded_scheduler)]
    async fn test_protect_transform() -> Result<(), Box<dyn Error>> {
        let t_holder = TopicHolder {
            topics_rx: Default::default(),
            topics_tx: Default::default(),
        };
        let projection: Vec<String> = vec!["pk", "cluster", "col1", "col2", "col3"]
            .iter()
            .map(|&x| String::from(x))
            .collect();

        let mut protection_map: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        let mut protection_table_map: HashMap<String, Vec<String>> = HashMap::new();
        protection_table_map.insert("old".to_string(), vec!["col1".to_string()]);
        protection_map.insert("keyspace".to_string(), protection_table_map);

        let protect_t = ProtectConfig {
            key: Some(secretbox::gen_key()),
            keyspace_table_columns: protection_map,
        };

        let secret_data: String = String::from("I am gonna get encrypted!!");

        let mut query_values: HashMap<String, Value> = HashMap::new();
        let mut primary_key: HashMap<String, Value> = HashMap::new();

        query_values.insert(String::from("pk"), Value::Strings(String::from("pk1")));
        primary_key.insert(String::from("pk"), Value::Strings(String::from("pk1")));
        query_values.insert(
            String::from("cluster"),
            Value::Strings(String::from("cluster")),
        );
        primary_key.insert(
            String::from("cluster"),
            Value::Strings(String::from("cluster")),
        );
        query_values.insert(String::from("col1"), Value::Strings(secret_data.clone()));
        query_values.insert(String::from("col2"), Value::Integer(42));
        query_values.insert(String::from("col3"), Value::Boolean(true));

        let wrapper = Wrapper::new(Message::Query(QueryMessage {
            original: RawFrame::NONE,
            query_string: "INSERT INTO keyspace.old (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 42, true);".to_string(),
            namespace: vec![String::from("keyspace"), String::from("old")],
            primary_key,
            query_values: Some(query_values),
            projection: Some(projection),
            query_type: QueryType::Write,
            ast: None
        }));

        let mut builder = TerminalLoggerBuilder::new();
        builder.level(Severity::Debug);
        builder.destination(Destination::Stderr);

        let logger = builder.build().unwrap();

        let transforms: Vec<Transforms> = vec![
            Transforms::Null(Null::new()),
        ];

        let chain = TransformChain::new(transforms, String::from("test_chain"));

        if let Transforms::Protect(mut protect) = protect_t.get_source(&t_holder, &logger).await? {
            let mut result = protect.transform(wrapper, &chain).await;
            if let Ok(mut m) = result {
                if let Message::Response(QueryResponse {
                    matching_query:
                        Some(QueryMessage {
                            original,
                            query_string,
                            namespace,
                            primary_key,
                            query_values: Some(query_values),
                            projection,
                            query_type,
                            ast,
                        }),
                    original: _,
                    result: _,
                    error: _,
                }) = &mut m
                {
                    let encrypted_val = query_values.remove("col1").unwrap();
                    assert_ne!(
                        encrypted_val.clone(),
                        Value::Strings(secret_data.clone())
                    );

                    let cframe = Frame::new_req_query(
                        "SELECT col1 FROM keyspace.old WHERE pk = 'pk1' AND cluster = 'cluster';".to_string(),
                        Consistency::LocalQuorum,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        vec![]
                    );

                    let mut colk_map: HashMap<String, Vec<String>> = HashMap::new();
                    colk_map.insert("keyspace.old".to_string(), vec!["pk".to_string(), "cluster".to_string()]);


                    if let Message::Query(qm) = process_cassandra_frame(cframe.clone(), &colk_map) {
                        let returner_message = QueryResponse {
                            matching_query: Some(qm.clone()),
                            original: RawFrame::NONE,
                            result: Some(Value::Rows(vec![vec![encrypted_val]])),
                            error: None
                        };

                        let ret_transforms: Vec<Transforms> = vec![
                            Transforms::RepeatMessage(ReturnerTransform{
                                message: Message::Response(returner_message.clone())
                            }),
                        ];

                        let ret_chain = TransformChain::new(ret_transforms, String::from("test_chain2"));

                        let mut resultr = protect.transform(Wrapper::new(Message::Query(qm.clone())), &ret_chain).await;
                        if let Ok(Message::Response(QueryResponse{ matching_query, original, result:Some(Value::Rows(r)), error })) = resultr {
                            if let Value::Strings(s) = r.get(0).unwrap().get(0).unwrap() {
                                assert_eq!(s.clone(), secret_data);
                                return Ok(())
                            }
                        }
                    }
                }
            }
        }
    panic!()
    }
}
