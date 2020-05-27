use crate::transforms::chain::{TransformChain, Wrapper, Transform, ChainResponse, RequestError};
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use crate::transforms::{Transforms, TransformsFromConfig};
use crate::config::ConfigError;
use crate::config::topology::TopicHolder;
use slog::Logger;
use crate::message::{Message, QueryMessage, QueryResponse, Protected, QueryType};
use core::mem;
use rlua::{Lua, UserData, UserDataMethods, ToLua};
use rlua_serde;
use crate::runtimes::lua::LuaRuntime;
use sodiumoxide::crypto::secretbox::Key;
use sodiumoxide::crypto::secretbox;
use std::collections::HashMap;
use crate::message::Value;
use slog::{info, warn};
use crate::message::Value::Rows;
use std::borrow::{BorrowMut, Borrow};
use std::ops::Deref;


#[derive(Clone)]
pub struct Protect {
    name: &'static str,
    logger: Logger,
    key: Key,
    keyspace_table_columns: HashMap<String, HashMap<String, Vec<String>>>
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ProtectConfig {
    pub key: Option<Key>,
    pub keyspace_table_columns: HashMap<String, HashMap<String, Vec<String>>>
}

#[async_trait]
impl TransformsFromConfig for ProtectConfig {
    async fn get_source(&self, _: &TopicHolder, logger: &Logger) -> Result<Transforms, ConfigError> {
        let key = match &self.key {
            None => {panic!("No encryption key provided")},
            Some(k) => {k.clone()},
        };

        Ok(Transforms::Protect(Protect {
            name: "protect",
            logger: logger.clone(),
            key,
            keyspace_table_columns: self.keyspace_table_columns.clone()
        }))
    }
}

#[async_trait]
impl Transform for Protect {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        if let Message::Query(qm) = & mut qd.message {
            // Encrypt the writes
            if QueryType::Write == qm.query_type {
                if qm.namespace.len() == 2 {
                    if let Some((keyspace, tables)) = self.keyspace_table_columns.get_key_value(qm.namespace.get(0).unwrap()) {
                        if let Some((table, columns)) = tables.get_key_value(qm.namespace.get(1).unwrap()) {
                            if let Some(query_values) = & mut qm.query_values {
                                for col in columns {
                                    if let Some(value) = query_values.get_mut(col) {
                                        let mut protected = Protected::Plaintext(value.clone());
                                        protected = protected.protect(&self.key);
                                        let _ = mem::replace( value, protected.into());

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
                                     matching_query: Some( QueryMessage{
                                                              original: _,
                                                              query_string,
                                                              namespace,
                                                              primary_key,
                                                              query_values,
                                                              projection: Some(projection),
                                                              query_type,
                                                              ast }),
                                     original: _,
                                     result: Some(Rows(rows)),
                                     error: None }) = & mut result
        {
            if namespace.len() == 2 {
                if let Some((keyspace, tables)) = self.keyspace_table_columns.get_key_value(namespace.get(0).unwrap()) {
                    if let Some((table, protect_columns)) = tables.get_key_value(namespace.get(1).unwrap()) {
                        let mut positions:Vec<usize> = Vec::new();
                        for (i, p) in projection.iter().enumerate() {
                            if protect_columns.contains(p) {
                                positions.push(i);
                            }
                        }
                        for row in rows {
                            for index in &positions {
                                if let Some(v) = row.get_mut(*index) {
                                    if let Value::Bytes(_)  = v {
                                        let mut  protected = Protected::from_encrypted_bytes_value(v.borrow()).unwrap();
                                        let new_value: Value = protected.unprotect(&self.key);
                                        let _ = mem::replace( v, new_value);
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
    use crate::transforms::{TransformsFromConfig, Transforms};
    use crate::config::topology::TopicHolder;
    use std::error::Error;
    use crate::transforms::chain::{Wrapper, Transform, TransformChain, ChainResponse};
    use crate::message::{Message, QueryMessage, QueryType, QueryResponse, Value};
    use crate::protocols::cassandra_protocol2::RawFrame;
    use slog::info;
    use sloggers::Build;
    use sloggers::terminal::{TerminalLoggerBuilder, Destination};
    use sloggers::types::Severity;
    use std::sync::Arc;
    use crate::transforms::null::Null;
    use async_trait::async_trait;
    use crate::transforms::printer::Printer;
    use crate::transforms::protect::ProtectConfig;
    use sodiumoxide::crypto::secretbox;



    #[tokio::test(threaded_scheduler)]
    async fn test_protect_transform() -> Result<(), Box<dyn Error>> {
        let t_holder = TopicHolder {
            topics_rx: Default::default(),
            topics_tx: Default::default()
        };
        let protect_t = ProtectConfig {
            key: Some(secretbox::gen_key()),
            keyspace_table_columns: Default::default()
            //todo build map for keyspace column etc
        };

        let wrapper = Wrapper::new(Message::Query(QueryMessage {
            original: RawFrame::NONE,
            query_string: "".to_string(),
            namespace: vec![String::from("keyspace"), String::from("old")],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::Read,
            ast: None
        }));

        let mut builder = TerminalLoggerBuilder::new();
        builder.level(Severity::Debug);
        builder.destination(Destination::Stderr);

        let logger = builder.build().unwrap();

        let transforms: Vec<Transforms> = vec![Transforms::Printer(Printer::new()), Transforms::Null(Null::new())];

        let chain = TransformChain::new(transforms, String::from("test_chain"));

        if let Transforms::Protect(mut protect) = protect_t.get_source(&t_holder, &logger).await? {
            let result = protect.transform(wrapper, &chain).await;


        } else {
            panic!()
        }
        Ok(())
    }
}