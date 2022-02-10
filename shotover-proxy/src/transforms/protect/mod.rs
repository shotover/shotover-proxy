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
use crate::message::MessageValue;
use crate::message::MessageValue::Rows;
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

#[derive(Deserialize, Debug, Clone)]
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
    Plaintext(MessageValue),
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

fn decrypt(ciphertext: Vec<u8>, nonce: Nonce, sym_key: &Key) -> Result<MessageValue> {
    let decrypted_bytes =
        secretbox::open(&ciphertext, &nonce, sym_key).map_err(|_| anyhow!("couldn't open box"))?;
    //TODO make error handing better here - failure here indicates a authenticity failure
    // let decrypted_value: Value =
    //     serde_json::from_slice(decrypted_bytes.as_slice()).map_err(|_| anyhow!("couldn't open box"))?;
    let decrypted_value: MessageValue =
        bincode::deserialize(&decrypted_bytes).map_err(|_| anyhow!("couldn't open box"))?;
    Ok(decrypted_value)
}

// TODO: Switch to something smaller/more efficient like bincode
impl From<Protected> for MessageValue {
    fn from(p: Protected) -> Self {
        match p {
            Protected::Plaintext(_) => panic!(
                "tried to move unencrypted value to plaintext without explicitly calling decrypt"
            ),
            Protected::Ciphertext { .. } => {
                // Value::Bytes(Bytes::from(serde_json::to_vec(&p).unwrap()))
                MessageValue::Bytes(Bytes::from(bincode::serialize(&p).unwrap()))
            }
        }
    }
}

impl Protected {
    pub async fn from_encrypted_bytes_value(value: &MessageValue) -> Result<Protected> {
        match value {
            MessageValue::Bytes(b) => {
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

    pub async fn unprotect(
        self,
        key_management: &KeyManager,
        key_id: &str,
    ) -> Result<MessageValue> {
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
                    if let Some((_, tables)) =
                        self.keyspace_table_columns.get_key_value(&qm.namespace[0])
                    {
                        if let Some((_, columns)) = tables.get_key_value(&qm.namespace[1]) {
                            if let Some(query_values) = &mut qm.query_values {
                                for col in columns {
                                    if let Some(value) = query_values.get_mut(col) {
                                        let mut protected = Protected::Plaintext(value.clone());
                                        protected = protected
                                            .protect(&self.key_source, &self.key_id)
                                            .await?;
                                        *value = protected.into();
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
                        if let Some((_keyspace, tables)) =
                            self.keyspace_table_columns.get_key_value(&namespace[0])
                        {
                            if let Some((_table, protect_columns)) =
                                tables.get_key_value(&namespace[1])
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
                                            if let MessageValue::Bytes(_) = v {
                                                let protected =
                                                    Protected::from_encrypted_bytes_value(v)
                                                        .await?;
                                                let new_value: MessageValue = protected
                                                    .unprotect(&self.key_source, &self.key_id)
                                                    .await?;
                                                *v = new_value;
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
