mod pkcs_11;
mod local_kek;
mod aws_kms;
mod key_management;

use crate::config::topology::TopicHolder;
use crate::message::Value;
use crate::message::Value::Rows;
use crate::message::{Message, QueryMessage, QueryResponse, QueryType};
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{Transforms, TransformsFromConfig};
use async_trait::async_trait;
use core::mem;
use serde::{Deserialize, Serialize};
use tracing::{ warn};
use std::borrow::{Borrow};
use std::collections::HashMap;
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::{Key, Nonce};
use anyhow::anyhow;

use crate::error::{ChainResponse};
use anyhow::{ Result};
use bytes::{Bytes, Buf};
use crate::transforms::protect::key_management::{KeyManager, KeyManagerConfig};

#[derive(Clone)]
pub struct Protect {
    name: &'static str,
    keyspace_table_columns: HashMap<String, HashMap<String, Vec<String>>>,
    key_source: KeyManager,
    // TODO this should be a function to create key_ids based on "seomthing", e.g. primary key
    // for the moment this is just a string
    key_id: String
}

#[derive(Clone)]
pub struct KeyMaterial {
    pub ciphertext_blob: Bytes,
    pub key_id: String,
    pub plaintext: Key
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ProtectConfig {
    pub keyspace_table_columns: HashMap<String, HashMap<String, Vec<String>>>,
    pub key_manager: KeyManagerConfig
}


// A protected value meets the following properties:
// https://doc.libsodium.org/secret-key_cryptography/secretbox
// This all relies on crypto_secretbox_easy which takes care of
// all padding, copying and timing issues associated with crypto
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum Protected {
    Plaintext(Value),
    Ciphertext { cipher: Vec<u8>, nonce: Nonce, enc_dek: Vec<u8>, kek_id: String },
}

fn encrypt(plaintext: String, sym_key: &Key) -> (Vec<u8>, Nonce) {
    let nonce = secretbox::gen_nonce();
    let ciphertext = secretbox::seal(plaintext.as_bytes(), &nonce, sym_key);
    return (ciphertext, nonce);
}

fn decrypt(ciphertext: Vec<u8>, nonce: Nonce, sym_key: &Key) -> Result<Value> {
    let decrypted_bytes = secretbox::open(&ciphertext, &nonce, sym_key).map_err(|_| anyhow!("couldn't open box"))?;
    //todo make error handing better here - failure here indicates a authenticity failure
    let decrypted_value: Value =
        serde_json::from_slice(decrypted_bytes.as_slice()).map_err(|_| anyhow!("couldn't open box"))?;
    return Ok(decrypted_value);
}

impl From<Protected> for Value {
    fn from(p: Protected) -> Self {
        match p {
            Protected::Plaintext(_) => panic!(
                "tried to move unencrypted value to plaintext without explicitly calling decrypt"
            ),
            Protected::Ciphertext { .. } => {
                Value::Bytes(Bytes::from(serde_json::to_vec(&p).unwrap()))
            }
        }
    }
}

impl Protected {
    pub async fn from_encrypted_bytes_value(value: &Value) -> Result<Protected> {
        match value {
            Value::Bytes(b) => {
                let protected_something: Protected = serde_json::from_slice(b.bytes())?;
                return Ok(protected_something);
            }
            _ => {
                return Err(anyhow!("Could not get bytes to decrypt - wrong value type {:?}", value));
            }
        }
    }
// TODO should this actually return self (we are sealing the plaintext value, but we don't swap out the plaintext??
    pub async fn protect(self, key_management: &KeyManager, key_id: &String) -> Result<Protected> {
        let sym_key = key_management.cached_get_key(key_id.clone(), None, None).await?;
        match &self {
            Protected::Plaintext(p) => {
                let (cipher, nonce) = encrypt(serde_json::to_string(p).unwrap(), &sym_key.plaintext);
                Ok(Protected::Ciphertext {
                    cipher,
                    nonce,
                    enc_dek: sym_key.ciphertext_blob.to_vec(),
                    kek_id: sym_key.key_id.clone()})
            }
            Protected::Ciphertext{ cipher, nonce, enc_dek, kek_id } => Ok(self),
        }
    }

    pub async fn unprotect(self, key_management: &KeyManager, key_id: &String) -> Result<Value> {
        return match self {
            Protected::Plaintext(p) => Ok(p),
            Protected::Ciphertext { cipher, nonce, enc_dek, kek_id } => {
                let sym_key = key_management.cached_get_key(key_id.clone(), Some(enc_dek), Some(kek_id)).await?;
                decrypt(cipher, nonce, &sym_key.plaintext)
            },
        };
    }
}

#[async_trait]
impl TransformsFromConfig for ProtectConfig {
    async fn get_source(
        &self,
        _: &TopicHolder,
    ) -> Result<Transforms> {
        Ok(Transforms::Protect(Protect {
            name: "protect",
            keyspace_table_columns: self.keyspace_table_columns.clone(),
            key_source: self.key_manager.build()?,
            key_id: "XXXXXXX".to_string()
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
                    if let Some((_, tables)) = self
                        .keyspace_table_columns
                        .get_key_value(qm.namespace.get(0).unwrap())
                    {
                        if let Some((_, columns)) =
                            tables.get_key_value(qm.namespace.get(1).unwrap())
                        {
                            if let Some(query_values) = &mut qm.query_values {
                                for col in columns {
                                    if let Some(value) = query_values.get_mut(col) {
                                        let mut protected = Protected::Plaintext(value.clone());
                                        protected = protected.protect(&self.key_source, &self.key_id).await?;
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
                    query_string: _,
                    namespace,
                    primary_key: _,
                    query_values: _,
                    projection: Some(projection),
                    query_type: _,
                    ast: _,
                }),
            original: _,
            result: Some(Rows(rows)),
            error: None,
        }) = &mut result
        {
            if namespace.len() == 2 {
                if let Some((_keyspace, tables)) = self
                    .keyspace_table_columns
                    .get_key_value(namespace.get(0).unwrap())
                {
                    if let Some((_table, protect_columns)) =
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
                                        let protected =
                                            Protected::from_encrypted_bytes_value(v.borrow()).await?;
                                        let new_value: Value = protected.unprotect(&self.key_source, &self.key_id).await?;
                                        let _ = mem::replace(v, new_value);
                                    } else {
                                        warn!("Tried decrypting non-blob column")
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
    use crate::transforms::chain::{Transform, TransformChain, Wrapper};
    use crate::transforms::null::Null;
    use crate::transforms::protect::ProtectConfig;
    use crate::transforms::{Transforms, TransformsFromConfig};
    use sodiumoxide::crypto::secretbox;
    use std::collections::HashMap;
    use std::error::Error;
    use crate::transforms::test_transforms::ReturnerTransform;
    use cassandra_proto::frame::{Frame};
    use cassandra_proto::consistency::Consistency;
    use crate::protocols::RawFrame;
    use crate::protocols::cassandra_protocol2::CassandraCodec2;
    use tokio::sync::mpsc::channel;
    use crate::transforms::protect::key_management::KeyManagerConfig;
    use std::env;

    #[tokio::test(threaded_scheduler)]
    async fn test_protect_transform() -> Result<(), Box<dyn Error>> {
        let (mut global_map_r, mut global_map_w) = evmap::new();
        let (global_tx, mut global_rx) = channel(1);

        let t_holder = TopicHolder {
            topics_rx: Default::default(),
            topics_tx: Default::default(),
            global_tx: global_tx,
            global_map_handle: global_map_r.factory()
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
            key_manager: KeyManagerConfig::LOCAL { kek: secretbox::gen_key(), kek_id: "".to_string() },
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

        let transforms: Vec<Transforms> = vec![
            Transforms::Null(Null::new()),
        ];

        let chain = TransformChain::new(transforms, String::from("test_chain"), t_holder.get_global_map_handle(), t_holder.get_global_tx());

        if let Transforms::Protect(protect) = protect_t.get_source(&t_holder).await? {
            let result = protect.transform(wrapper, &chain).await;
            if let Ok(mut m) = result {
                if let Message::Response(QueryResponse {
                    matching_query:
                        Some(QueryMessage {
                            original: _,
                            query_string: _,
                            namespace: _,
                            primary_key: _,
                            query_values: Some(query_values),
                            projection: _,
                            query_type: _,
                            ast: _,
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

                    // Let's make sure the plain text is not in the encrypted value when actually formated the same way!!!!!!!
                    let encrypted_payload = format!("encrypted: {:?}", encrypted_val.clone());
                    assert!(!encrypted_payload.contains(format!("plaintext {:?}", serde_json::to_string(&secret_data.clone().into_bytes())?).as_str()));

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

                    let codec = CassandraCodec2::new(colk_map, false);

                    if let Message::Query(qm) = codec.process_cassandra_frame(cframe.clone()) {
                        let returner_message = QueryResponse {
                            matching_query: Some(qm.clone()),
                            original: RawFrame::NONE,
                            result: Some(Value::Rows(vec![vec![encrypted_val]])),
                            error: None
                        };

                        let ret_transforms: Vec<Transforms> = vec![
                            Transforms::RepeatMessage(Box::new(ReturnerTransform{
                                message: Message::Response(returner_message.clone()),
                                ok: true
                            })),
                        ];

                        let ret_chain = TransformChain::new(ret_transforms, String::from("test_chain"), t_holder.get_global_map_handle(), t_holder.get_global_tx());

                        let resultr = protect.transform(Wrapper::new(Message::Query(qm.clone())), &ret_chain).await;
                        if let Ok(Message::Response(QueryResponse{ matching_query: _, original: _, result:Some(Value::Rows(r)), error: _ })) = resultr {
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

    #[tokio::test(threaded_scheduler)]
    async fn test_protect_kms_transform() -> Result<(), Box<dyn Error>> {
        let (mut global_map_r, mut global_map_w) = evmap::new();
        let (global_tx, mut global_rx) = channel(1);

        let t_holder = TopicHolder {
            topics_rx: Default::default(),
            topics_tx: Default::default(),
            global_tx: global_tx,
            global_map_handle: global_map_r.factory()
        };

        let projection: Vec<String> = vec!["pk", "cluster", "col1", "col2", "col3"]
            .iter()
            .map(|&x| String::from(x))
            .collect();

        let mut protection_map: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        let mut protection_table_map: HashMap<String, Vec<String>> = HashMap::new();
        protection_table_map.insert("old".to_string(), vec!["col1".to_string()]);
        protection_map.insert("keyspace".to_string(), protection_table_map);

        let aws_config = KeyManagerConfig::AWS_KMS {
            region: env::var("CMK_REGION").or::<String>(Ok("US-EAST-1".to_string()))?,
            cmk_id: env::var("CMK_ID").or::<String>(Ok("alias/InstaProxyDev".to_string()))?,
            encryption_context: None,
            key_spec: None,
            number_of_bytes: Some(32), // 256-bit (it's specified in bytes)
            grant_tokens: None
        };

        let protect_t = ProtectConfig {
            key_manager: aws_config,
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

        let transforms: Vec<Transforms> = vec![
            Transforms::Null(Null::new()),
        ];

        let chain = TransformChain::new(transforms, String::from("test_chain"), t_holder.get_global_map_handle(), t_holder.get_global_tx());

        let t = protect_t.get_source(&t_holder).await?;
        if let Transforms::Protect(protect) = t {
            let mut m = protect.transform(wrapper, &chain).await?;
                if let Message::Response(QueryResponse {
                                             matching_query:
                                             Some(QueryMessage {
                                                      original: _,
                                                      query_string: _,
                                                      namespace: _,
                                                      primary_key: _,
                                                      query_values: Some(query_values),
                                                      projection: _,
                                                      query_type: _,
                                                      ast: _,
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

                    // Let's make sure the plain text is not in the encrypted value when actually formated the same way!!!!!!!
                    let encrypted_payload = format!("encrypted: {:?}", encrypted_val.clone());
                    assert!(!encrypted_payload.contains(format!("plaintext {:?}", serde_json::to_string(&secret_data.clone().into_bytes())?).as_str()));

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

                    let codec = CassandraCodec2::new(colk_map, false);

                    if let Message::Query(qm) = codec.process_cassandra_frame(cframe.clone()) {
                        let returner_message = QueryResponse {
                            matching_query: Some(qm.clone()),
                            original: RawFrame::NONE,
                            result: Some(Value::Rows(vec![vec![encrypted_val]])),
                            error: None
                        };

                        let ret_transforms: Vec<Transforms> = vec![
                            Transforms::RepeatMessage(Box::new(ReturnerTransform{
                                message: Message::Response(returner_message.clone()),
                                ok: true
                            })),
                        ];

                        let ret_chain = TransformChain::new(ret_transforms, String::from("test_chain"), t_holder.get_global_map_handle(), t_holder.get_global_tx());

                        let resultr = protect.transform(Wrapper::new(Message::Query(qm.clone())), &ret_chain).await;
                        if let Ok(Message::Response(QueryResponse{ matching_query: _, original: _, result:Some(Value::Rows(r)), error: _ })) = resultr {
                            if let Value::Strings(s) = r.get(0).unwrap().get(0).unwrap() {
                                assert_eq!(s.clone(), secret_data);
                                return Ok(())
                            }
                        }
                    }
                }

        }
        panic!()
    }



}
