use core::mem;
use std::collections::HashMap;

use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::{Key, Nonce};
use tracing::warn;

use crate::error::ChainResponse;
use crate::message::Value;
use crate::message::Value::Rows;
use crate::message::{MessageDetails, QueryMessage, QueryResponse, QueryType};
use crate::transforms::protect::key_management::{KeyManager, KeyManagerConfig};
use crate::transforms::{Transform, Transforms, Wrapper};

mod aws_kms;
mod key_management;
mod local_kek;
mod pkcs_11;

#[derive(Clone)]
pub struct Protect {
    keyspace_table_columns: HashMap<String, HashMap<String, Vec<String>>>,
    key_source: KeyManager,
    // TODO this should be a function to create key_ids based on "something", e.g. primary key
    // for the moment this is just a string
    key_id: String,
}

#[derive(Clone)]
pub struct KeyMaterial {
    pub ciphertext_blob: Bytes,
    pub key_id: String,
    pub plaintext: Key,
}

#[derive(Deserialize)]
pub struct ProtectConfig {
    pub keyspace_table_columns: HashMap<String, HashMap<String, Vec<String>>>,
    pub key_manager: KeyManagerConfig,
}

// A protected value meets the following properties:
// https://doc.libsodium.org/secret-key_cryptography/secretbox
// This all relies on crypto_secretbox_easy which takes care of
// all padding, copying and timing issues associated with crypto
#[derive(Serialize, Deserialize)]
pub enum Protected {
    Plaintext(Value),
    Ciphertext {
        cipher: Vec<u8>,
        nonce: Nonce,
        enc_dek: Vec<u8>,
        kek_id: String,
    },
}

fn encrypt(plaintext: Vec<u8>, sym_key: &Key) -> (Vec<u8>, Nonce) {
    let nonce = secretbox::gen_nonce();
    let ciphertext = secretbox::seal(&plaintext, &nonce, sym_key);
    (ciphertext, nonce)
}

fn decrypt(ciphertext: Vec<u8>, nonce: Nonce, sym_key: &Key) -> Result<Value> {
    let decrypted_bytes =
        secretbox::open(&ciphertext, &nonce, sym_key).map_err(|_| anyhow!("couldn't open box"))?;
    //TODO make error handing better here - failure here indicates a authenticity failure
    // let decrypted_value: Value =
    //     serde_json::from_slice(decrypted_bytes.as_slice()).map_err(|_| anyhow!("couldn't open box"))?;
    let decrypted_value: Value =
        bincode::deserialize(&decrypted_bytes).map_err(|_| anyhow!("couldn't open box"))?;
    Ok(decrypted_value)
}

// TODO: Switch to something smaller/more efficient like bincode
impl From<Protected> for Value {
    fn from(p: Protected) -> Self {
        match p {
            Protected::Plaintext(_) => panic!(
                "tried to move unencrypted value to plaintext without explicitly calling decrypt"
            ),
            Protected::Ciphertext { .. } => {
                // Value::Bytes(Bytes::from(serde_json::to_vec(&p).unwrap()))
                Value::Bytes(Bytes::from(bincode::serialize(&p).unwrap()))
            }
        }
    }
}

impl Protected {
    pub async fn from_encrypted_bytes_value(value: &Value) -> Result<Protected> {
        match value {
            Value::Bytes(b) => {
                // let protected_something: Protected = serde_json::from_slice(b.bytes())?;
                let protected_something: Protected = bincode::deserialize(b)?;
                Ok(protected_something)
            }
            _ => Err(anyhow!(
                "Could not get bytes to decrypt - wrong value type {:?}",
                value
            )),
        }
    }
    // TODO should this actually return self (we are sealing the plaintext value, but we don't swap out the plaintext??
    pub async fn protect(self, key_management: &KeyManager, key_id: &str) -> Result<Protected> {
        let sym_key = key_management
            .cached_get_key(key_id.to_string(), None, None)
            .await?;
        match &self {
            Protected::Plaintext(p) => {
                // let (cipher, nonce) = encrypt(serde_json::to_string(p).unwrap(), &sym_key.plaintext);
                let (cipher, nonce) = encrypt(bincode::serialize(&p).unwrap(), &sym_key.plaintext);
                Ok(Protected::Ciphertext {
                    cipher,
                    nonce,
                    enc_dek: sym_key.ciphertext_blob.to_vec(),
                    kek_id: sym_key.key_id,
                })
            }
            Protected::Ciphertext { .. } => Ok(self),
        }
    }

    pub async fn unprotect(self, key_management: &KeyManager, key_id: &str) -> Result<Value> {
        match self {
            Protected::Plaintext(p) => Ok(p),
            Protected::Ciphertext {
                cipher,
                nonce,
                enc_dek,
                kek_id,
            } => {
                let sym_key = key_management
                    .cached_get_key(key_id.to_string(), Some(enc_dek), Some(kek_id))
                    .await?;
                decrypt(cipher, nonce, &sym_key.plaintext)
            }
        }
    }
}

impl ProtectConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        Ok(Transforms::Protect(Protect {
            keyspace_table_columns: self.keyspace_table_columns.clone(),
            key_source: self.key_manager.build()?,
            key_id: "XXXXXXX".to_string(),
        }))
    }
}

#[async_trait]
impl Transform for Protect {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        for message in message_wrapper.messages.iter_mut() {
            if let MessageDetails::Query(qm) = &mut message.details {
                // Encrypt the writes
                if QueryType::Write == qm.query_type && qm.namespace.len() == 2 {
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
                                        protected = protected
                                            .protect(&self.key_source, &self.key_id)
                                            .await?;
                                        let _ = mem::replace(value, protected.into());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut original_messages = message_wrapper.messages.clone();
        let mut result = message_wrapper.call_next_transform().await?;

        for (response, request) in result.iter_mut().zip(original_messages.iter_mut()) {
            if let MessageDetails::Response(QueryResponse {
                result: Some(Rows(rows)),
                error: None,
                ..
            }) = &mut response.details
            {
                if let MessageDetails::Query(QueryMessage {
                    namespace,
                    projection: Some(projection),
                    ..
                }) = &request.details
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
                                                    Protected::from_encrypted_bytes_value(v)
                                                        .await?;
                                                let new_value: Value = protected
                                                    .unprotect(&self.key_source, &self.key_id)
                                                    .await?;
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
            }
        }

        // this used to be worse https://doc.rust-lang.org/book/ch18-03-pattern-syntax.html#destructuring-structs-and-tuples
        // TODO: destructure the above bracket mountain as below
        Ok(result)
    }
}

#[cfg(test)]
mod protect_transform_tests {
    use std::collections::HashMap;
    use std::env;
    use std::error::Error;

    use anyhow::{anyhow, Result};

    use cassandra_protocol::consistency::Consistency;
    use cassandra_protocol::frame::{Flags, Frame, Version};
    use sodiumoxide::crypto::secretbox;

    use crate::message::{
        IntSize, Message, MessageDetails, QueryMessage, QueryResponse, QueryType, Value,
    };
    use crate::protocols::cassandra_codec::CassandraCodec;
    use crate::protocols::RawFrame;
    use crate::transforms::chain::TransformChain;
    use crate::transforms::debug::returner::{DebugReturner, Response};
    use crate::transforms::loopback::Loopback;
    use crate::transforms::protect::key_management::KeyManagerConfig;
    use crate::transforms::protect::ProtectConfig;
    use crate::transforms::{Transform, Transforms, Wrapper};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_protect_transform() -> Result<(), Box<dyn Error>> {
        let projection: Vec<String> = vec!["pk", "cluster", "col1", "col2", "col3"]
            .iter()
            .map(|&x| String::from(x))
            .collect();

        let mut protection_map: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        let mut protection_table_map: HashMap<String, Vec<String>> = HashMap::new();
        protection_table_map.insert("old".to_string(), vec!["col1".to_string()]);
        protection_map.insert("keyspace".to_string(), protection_table_map);

        let protect_t = ProtectConfig {
            key_manager: KeyManagerConfig::Local {
                kek: secretbox::gen_key(),
                kek_id: "".to_string(),
            },
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
        query_values.insert(String::from("col2"), Value::Integer(42, IntSize::I32));
        query_values.insert(String::from("col3"), Value::Boolean(true));

        let mut wrapper = Wrapper::new(vec!(Message::new(MessageDetails::Query(QueryMessage {
            query_string: "INSERT INTO keyspace.old (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 42, true);".to_string(),
            namespace: vec![String::from("keyspace"), String::from("old")],
            primary_key,
            query_values: Some(query_values),
            projection: Some(projection),
            query_type: QueryType::Write,
            ast: None,
        }), true, RawFrame::None)));

        let transforms: Vec<Transforms> = vec![Transforms::Loopback(Loopback::default())];

        let mut chain = TransformChain::new(transforms, String::from("test_chain"));

        wrapper.reset(chain.get_inner_chain_refs());

        if let Transforms::Protect(mut protect) = protect_t.get_source().await? {
            let result = protect.transform(wrapper).await;
            if let Ok(mut m) = result {
                if let MessageDetails::Response(QueryResponse {
                    matching_query:
                        Some(QueryMessage {
                            query_values: Some(query_values),
                            ..
                        }),
                    ..
                }) = &mut m.pop().unwrap().details
                {
                    let encrypted_val = query_values.remove("col1").unwrap();
                    assert_ne!(encrypted_val, Value::Strings(secret_data.clone()));

                    // Let's make sure the plain text is not in the encrypted value when actually formated the same way!!!!!!!
                    let encrypted_payload = format!("encrypted: {:?}", encrypted_val.clone());
                    assert!(!encrypted_payload.contains(
                        format!(
                            "plaintext {:?}",
                            bincode::serialize(&secret_data.clone().into_bytes())?
                        )
                        .as_str()
                    ));

                    let cframe = Frame::new_req_query(
                        "SELECT col1 FROM keyspace.old WHERE pk = 'pk1' AND cluster = 'cluster';"
                            .to_string(),
                        Consistency::LocalQuorum,
                        None,
                        false,
                        None,
                        None,
                        None,
                        None,
                        Flags::empty(),
                        Version::V4,
                    );

                    let mut colk_map: HashMap<String, Vec<String>> = HashMap::new();
                    colk_map.insert(
                        "keyspace.old".to_string(),
                        vec!["pk".to_string(), "cluster".to_string()],
                    );

                    let codec = CassandraCodec::new(colk_map, false);

                    if let MessageDetails::Query(qm) = codec
                        .process_cassandra_frame(cframe.clone())
                        .pop()
                        .unwrap()
                        .details
                    {
                        let returner_message = QueryResponse {
                            matching_query: Some(qm.clone()),
                            result: Some(Value::Rows(vec![vec![encrypted_val]])),
                            error: None,
                            response_meta: None,
                        };

                        let ret_transforms: Vec<Transforms> = vec![Transforms::DebugReturner(
                            DebugReturner::new(Response::Message(vec![Message::new(
                                MessageDetails::Response(returner_message.clone()),
                                true,
                                RawFrame::None,
                            )])),
                        )];

                        let mut ret_chain =
                            TransformChain::new(ret_transforms, String::from("test_chain"));

                        let mut new_wrapper = Wrapper::new(vec![Message::new(
                            MessageDetails::Query(qm),
                            true,
                            RawFrame::None,
                        )]);

                        new_wrapper.reset(ret_chain.get_inner_chain_refs());

                        let result = protect.transform(new_wrapper).await;
                        if let MessageDetails::Response(QueryResponse {
                            result: Some(Value::Rows(r)),
                            ..
                        }) = result.unwrap().pop().unwrap().details
                        {
                            if let Value::Strings(s) = r.get(0).unwrap().get(0).unwrap() {
                                assert_eq!(s, &secret_data);
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
        panic!()
    }

    //#[tokio::test(flavor = "multi_thread")]
    //#[ignore] // reason: requires AWS credentials
    #[allow(unused)]
    // Had to disable this when the repo went public as we dont have access to github secrets anymore
    async fn test_protect_kms_transform() -> Result<()> {
        let projection: Vec<String> = vec!["pk", "cluster", "col1", "col2", "col3"]
            .iter()
            .map(|&x| String::from(x))
            .collect();

        let mut protection_map: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        let mut protection_table_map: HashMap<String, Vec<String>> = HashMap::new();
        protection_table_map.insert("old".to_string(), vec!["col1".to_string()]);
        protection_map.insert("keyspace".to_string(), protection_table_map);

        let aws_config = KeyManagerConfig::AWSKms {
            region: env::var("CMK_REGION")
                .or_else::<String, _>(|_| Ok("US-EAST-1".to_string()))
                .map_err(|e| anyhow!(e))?,
            cmk_id: env::var("CMK_ID")
                .or_else::<String, _>(|_| Ok("alias/InstaProxyDev".to_string()))
                .map_err(|e| anyhow!(e))?,
            encryption_context: None,
            key_spec: None,
            number_of_bytes: Some(32), // 256-bit (it's specified in bytes)
            grant_tokens: None,
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
        query_values.insert(String::from("col2"), Value::Integer(42, IntSize::I32));
        query_values.insert(String::from("col3"), Value::Boolean(true));

        let mut wrapper = Wrapper::new(vec!(Message::new(MessageDetails::Query(QueryMessage {
            query_string: "INSERT INTO keyspace.old (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 42, true);".to_string(),
            namespace: vec![String::from("keyspace"), String::from("old")],
            primary_key,
            query_values: Some(query_values),
            projection: Some(projection),
            query_type: QueryType::Write,
            ast: None,
        }), true, RawFrame::None)));

        let transforms: Vec<Transforms> = vec![Transforms::Loopback(Loopback::default())];

        let mut chain = TransformChain::new(transforms, String::from("test_chain"));

        wrapper.reset(chain.get_inner_chain_refs());

        let t = protect_t.get_source().await?;
        if let Transforms::Protect(mut protect) = t {
            let mut m = protect.transform(wrapper).await?;
            let mut details = m.pop().unwrap().details;
            if let MessageDetails::Response(QueryResponse {
                matching_query:
                    Some(QueryMessage {
                        query_values: Some(query_values),
                        ..
                    }),
                ..
            }) = &mut details
            {
                let encrypted_val = query_values.remove("col1").unwrap();
                assert_ne!(encrypted_val.clone(), Value::Strings(secret_data.clone()));

                // Let's make sure the plain text is not in the encrypted value when actually formated the same way!!!!!!!
                let encrypted_payload = format!("encrypted: {:?}", encrypted_val.clone());
                assert!(!encrypted_payload.contains(
                    format!(
                        "plaintext {:?}",
                        bincode::serialize(&secret_data.clone().into_bytes())?
                    )
                    .as_str()
                ));

                let cframe = Frame::new_req_query(
                    "SELECT col1 FROM keyspace.old WHERE pk = 'pk1' AND cluster = 'cluster';"
                        .to_string(),
                    Consistency::LocalQuorum,
                    None,
                    false,
                    None,
                    None,
                    None,
                    None,
                    Flags::empty(),
                    Version::V4,
                );

                let mut colk_map: HashMap<String, Vec<String>> = HashMap::new();
                colk_map.insert(
                    "keyspace.old".to_string(),
                    vec!["pk".to_string(), "cluster".to_string()],
                );

                let codec = CassandraCodec::new(colk_map, false);

                if let MessageDetails::Query(qm) = codec
                    .process_cassandra_frame(cframe.clone())
                    .pop()
                    .unwrap()
                    .details
                {
                    let returner_message = QueryResponse {
                        matching_query: Some(qm.clone()),
                        result: Some(Value::Rows(vec![vec![encrypted_val]])),
                        error: None,
                        response_meta: None,
                    };

                    let ret_transforms: Vec<Transforms> = vec![Transforms::DebugReturner(
                        DebugReturner::new(Response::Message(vec![Message::new(
                            MessageDetails::Response(returner_message.clone()),
                            true,
                            RawFrame::None,
                        )])),
                    )];

                    let mut ret_chain =
                        TransformChain::new(ret_transforms, String::from("test_chain"));

                    let mut new_wrapper = Wrapper::new(vec![Message::new(
                        MessageDetails::Query(qm.clone()),
                        true,
                        RawFrame::None,
                    )]);

                    new_wrapper.reset(ret_chain.get_inner_chain_refs());

                    let result = protect.transform(new_wrapper).await;
                    if let MessageDetails::Response(QueryResponse {
                        result: Some(Value::Rows(r)),
                        ..
                    }) = result.unwrap().pop().unwrap().details
                    {
                        return if let Value::Strings(s) = r.get(0).unwrap().get(0).unwrap() {
                            assert_eq!(s.clone(), secret_data);
                            Ok(())
                        } else {
                            Err(anyhow!("Couldn't get string"))
                        };
                    }
                }
            }
        }
        panic!()
    }
}
