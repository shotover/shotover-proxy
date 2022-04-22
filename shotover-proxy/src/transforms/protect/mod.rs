use crate::error::ChainResponse;
use crate::frame::cassandra::CQLStatement;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::MessageValue;
use crate::transforms::protect::key_management::{KeyManager, KeyManagerConfig};
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{FQName, Operand};
use cql3_parser::insert::InsertValues;
use cql3_parser::select::{Select, SelectElement};
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::{Key, Nonce};
use sodiumoxide::hex;
use std::collections::HashMap;
use tracing::warn;

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

impl Protect {
    fn get_protected_columns(&self, table_name : &FQName ) -> Option<&Vec<String>> {
        // TODO replace "" with cached keyspace name
        if let Some(tables) = self.keyspace_table_columns.get(table_name.extract_keyspace( ""))
        {
            tables.get(&table_name.name)
        } else {
            None
        }
    }

    /// processes the select statement to modify the rows.  returns true if the rows were modified
    async fn process_select(&self, select : &Select, columns : &Vec<String>, rows : &mut Vec<Vec<MessageValue>>) -> Result<bool> {
        let mut modified = false;

        let positions: Vec<usize> = select
            .columns
            .iter()
            .enumerate()
            .filter_map(|(i, col)| {
                if let SelectElement::Column(named) = col {
                    if columns.contains(&named.name) {
                        Some(i)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        for row in &mut *rows {
            for index in &positions {
                if let Some(message_value) = row.get_mut(*index) {
                    let protected = Protected::extract_result(message_value);

                    let new_value: MessageValue = protected.unwrap()
                        .unprotect(
                            &self.key_source,
                            &self.key_id,
                        )
                        .await?;
                    *message_value = new_value;
                    modified = true;
                }
            }
        }

        Ok(modified)
    }

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
    let decrypted_value: MessageValue = serde_json::from_slice(decrypted_bytes.as_slice())
        .map_err(|_| anyhow!("couldn't open box"))?;
    // let decrypted_value: MessageValue =
    //     bincode::deserialize(&decrypted_bytes).map_err(|_| anyhow!("couldn't open box"))?;
    Ok(decrypted_value)
}

// TODO: Switch to something smaller/more efficient like bincode - Need the new cassandra AST first so we can create Blob's in the ast
impl From<Protected> for MessageValue {
    fn from(p: Protected) -> Self {
        match p {
            Protected::Plaintext(_) => panic!(
                "tried to move unencrypted value to plaintext without explicitly calling decrypt"
            ),
            Protected::Ciphertext { .. } => {
                //MessageValue::Bytes(Bytes::from(serde_json::to_vec(&p).unwrap()))
                MessageValue::Bytes(Bytes::from(bincode::serialize(&p).unwrap()))
            }
        }
    }
}

impl From<&Protected> for Operand {
    fn from(p: &Protected) -> Self {
        match p {
            Protected::Plaintext(_) => panic!(
                "tried to move unencrypted value to plaintext without explicitly calling decrypt"
            ),
            Protected::Ciphertext { .. } => Operand::Const(format!("'{}'", hex::encode(serde_json::to_vec(&p).unwrap()))),
        }
    }
}

impl Protected {
    fn extract_result(value : &MessageValue ) -> Result<Protected> {
            match value {
                MessageValue::Bytes(b) => {
                    // let protected_something: Protected = serde_json::from_slice(b.bytes())?;
                    let protected_something = bincode::deserialize(b);
                    if protected_something.is_err() {
                        Err(anyhow!( "{:?}", protected_something.err()))
                    } else {
                        Ok(protected_something.unwrap())
                    }
                }
                MessageValue::Varchar(s) => {
                    warn!("varchar {}", s );
                    let mut hex_value = s.chars();
                    hex_value.next();
                    hex_value.next_back();


                    let byte_value = hex::decode(hex_value.as_str());
                    //let plain = decrypt( byte_value);
                    //let x = bincode::deserialize(&byte_value.unwrap());

                    let protected_something = bincode::deserialize(&byte_value.unwrap());
                    if protected_something.is_err() {
                        Err(anyhow!( "{:?}", protected_something.err()))
                    } else {
                        Ok(protected_something.unwrap())
                    }
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
                let result = decrypt(cipher, nonce, &sym_key.plaintext);
                if result.is_err() {
                    Err( anyhow!( "{}", result.err().unwrap() ))
                } else {
                    Ok( result.unwrap())
                }

            }
        }
    }
}

impl ProtectConfig {
    pub async fn get_transform(&self) -> Result<Transforms> {
        Ok(Transforms::Protect(Protect {
            keyspace_table_columns: self.keyspace_table_columns.clone(),
            key_source: self.key_manager.build()?,
            key_id: "XXXXXXX".to_string(),
        }))
    }
}

/// determines if columns in the CassandraStatement need to be encrypted and encrypts them.  Returns `true` if any columns were changed.
async fn encrypt_columns(
    statement: &mut CassandraStatement,
    columns: &[String],
    key_source: &KeyManager,
    key_id: &str,
) -> Result<bool> {
    let mut data_changed = false;
    match statement {
        CassandraStatement::Insert(insert) => {
            let indices: Vec<usize> = insert
                .columns
                .iter()
                .enumerate()
                .filter_map(|(i, col_name)| {
                    if columns.contains(col_name) {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect();
            match &mut insert.values {
                InsertValues::Values(value_operands) => {
                    for idx in indices {
                        let mut protected =
                            Protected::Plaintext(MessageValue::from(&value_operands[idx]));
                        protected = protected.protect(key_source, key_id).await?;
                        value_operands[idx] = Operand::from(&protected);
                        data_changed = true
                    }
                }
                InsertValues::Json(_) => {
                    // TODO parse json and encrypt.
                }
            }
        }
        CassandraStatement::Update(update) => {
            for assignment in &mut update.assignments {
                if columns.contains(&assignment.name.column) {
                    let mut protected = Protected::Plaintext(MessageValue::from(&assignment.value));
                    protected = protected.protect(key_source, key_id).await?;
                    assignment.value = Operand::from(&protected);
                    data_changed = true;
                }
            }
        }
        _ => {
            // no other statement are modified
        }
    }
    Ok(data_changed)
}


#[async_trait]
impl Transform for Protect {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        // encrypt the values included in any INSERT or UPDATE queries
        for message in message_wrapper.messages.iter_mut() {
            let mut data_changed = false;

            if let Some(Frame::Cassandra(CassandraFrame {
                operation: CassandraOperation::Query { query, .. },
                ..
            })) = message.frame()
            {
                for cql_statement in &mut query.statements {
                    let statement = &mut cql_statement.statement;
                    if let Some(table_name) = CQLStatement::get_table_name(statement) {
                        if let Some(columns) = self.get_protected_columns( table_name ) {
                            data_changed = encrypt_columns(
                                statement,
                                &columns,
                                &self.key_source,
                                &self.key_id,
                            ).await?;
                            if data_changed {
                                warn!( "statement changed to {}", statement );
                            }
                        }
                    }
                }
            }

            if data_changed {
                message.invalidate_cache();
            }
        }

        let mut original_messages = message_wrapper.messages.clone();
        let mut result = message_wrapper.call_next_transform().await?;

        for (response, request) in result.iter_mut().zip(original_messages.iter_mut()) {
            let mut invalidate_cache = false;
            if let Some(Frame::Cassandra(CassandraFrame {
                                             operation: CassandraOperation::Query { query, .. },
                                             ..
                                         })) = request.frame() {
                if let Some(Frame::Cassandra(CassandraFrame {
                                                 operation:
                                                 CassandraOperation::Result(CassandraResult::Rows {
                                                                                value: MessageValue::Rows(rows),
                                                                                ..
                                                                            }),
                                                 ..
                                             })) = response.frame() {
                    for cql_statement in &mut query.statements {
                        let statement = &mut cql_statement.statement;
                        if let Some(table_name) = CQLStatement::get_table_name(statement) {
                            if let Some(columns) = self.get_protected_columns( table_name ) {
                                if let CassandraStatement::Select(select) = &statement {
                                    invalidate_cache |=  self.process_select( select, columns, rows ).await?
                                }
                            }
                        }
                    }
                }
            }
            if invalidate_cache {
                response.invalidate_cache();
            }
        }

        // this used to be worse https://doc.rust-lang.org/book/ch18-03-pattern-syntax.html#destructuring-structs-and-tuples
        // TODO: destructure the above bracket mountain as below
        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use serde::Serialize;
    use crate::message::MessageValue;
    use crate::transforms::protect::key_management::{KeyManagement, KeyManager};
    use crate::transforms::protect::local_kek::LocalKeyManagement;
    use crate::transforms::protect::Protected;
    use sodiumoxide::crypto::secretbox::{Key, Nonce};

    #[tokio::test(flavor = "multi_thread")]
    //#[test]
    async fn round_trip_test() {
        if sodiumoxide::init().is_err() {
            panic!( "could not init sodiumoxide");
        }
        let kek = sodiumoxide::crypto::secretbox::xsalsa20poly1305::gen_key();
        let local_key_mgr = LocalKeyManagement{ kek, kek_id: "".to_string() };
        let key_mgr = KeyManager::Local(local_key_mgr);
        let msg_value =  MessageValue::Varchar("Hello World".to_string());
        let plain = Protected::Plaintext( msg_value.clone() );
        let encr = plain.protect( &key_mgr, "" ).await.unwrap();
        let new_msg = encr.unprotect( &key_mgr, "" ).await;

        assert_eq!( &msg_value, &new_msg.unwrap() );

    }
}
