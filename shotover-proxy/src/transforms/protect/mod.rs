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
use tracing_log::log::debug;

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
    /// encodes a Protected object into a byte array.  This is here to centeralize the serde for
    /// the Protected object.
    /// Returns an error if a Plaintext Protected object is passed
    fn encode(protected: &Protected) -> Result<Vec<u8>> {
        match protected {
            Protected::Plaintext(_) => Err(anyhow!("can not encode plain text")),
            Protected::Ciphertext { .. } => Ok(serde_json::to_vec(protected)?),
        }
    }

    /// decodes a byte array into the Protected object.  This is here to centeralize the serde for
    /// the Protected object.
    fn decode(data: &[u8]) -> Result<Protected> {
        Ok(serde_json::from_slice(data)?)
    }

    /// get the list of protected columns for the specified table name.  Will return `None` if no columns
    /// are defined for the table.
    fn get_protected_columns(&self, table_name: &FQName) -> Option<&Vec<String>> {
        // TODO replace "" with cached keyspace name
        if let Some(tables) = self
            .keyspace_table_columns
            .get(table_name.extract_keyspace(""))
        {
            tables.get(&table_name.name)
        } else {
            None
        }
    }

    /// extractes the protected object from the message value.  Resulting object is a Protected::Ciphertext
    fn extract_protected(&self, value: &MessageValue) -> Result<Protected> {
        match value {
            MessageValue::Bytes(b) => Protect::decode(&b[..]),
            MessageValue::Varchar(hex_value) => {
                let byte_value = hex::decode(hex_value);
                Protect::decode(&byte_value.unwrap())
            }
            _ => Err(anyhow!(
                "Could not get bytes to decrypt - wrong value type {:?}",
                value
            )),
        }
    }

    /// determines if columns in the CassandraStatement need to be encrypted and encrypts them.  Returns `true` if any columns were changed.
    ///  * `statement` the statement to encrypt.
    ///  * `columns` the column names to encrypt.
    ///  * `key_source` the key manager with encryption keys.
    ///  * `key_id` the key within the manager to use.
    async fn encrypt_columns(&self, statement: &mut CassandraStatement) -> Result<bool> {
        let mut data_changed = false;
        if let Some(table_name) = CQLStatement::get_table_name(statement) {
            if let Some(columns) = self.get_protected_columns(table_name) {
                match statement {
                    CassandraStatement::Insert(insert) => {
                        // get the indices of the inserted protected columns
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
                        // if there are columns process them
                        if !indices.is_empty() {
                            match &mut insert.values {
                                InsertValues::Values(value_operands) => {
                                    for idx in indices {
                                        let mut protected = Protected::Plaintext(
                                            MessageValue::from(&value_operands[idx]),
                                        );
                                        protected = protected
                                            .protect(&self.key_source, &self.key_id)
                                            .await?;
                                        value_operands[idx] = Operand::from(&protected);
                                        data_changed = true
                                    }
                                }
                                InsertValues::Json(_) => {
                                    // TODO parse json and encrypt.
                                }
                            }
                        }
                    }
                    CassandraStatement::Update(update) => {
                        for assignment in &mut update.assignments {
                            if columns.contains(&assignment.name.column) {
                                let mut protected =
                                    Protected::Plaintext(MessageValue::from(&assignment.value));
                                protected =
                                    protected.protect(&self.key_source, &self.key_id).await?;
                                assignment.value = Operand::from(&protected);
                                data_changed = true;
                            }
                        }
                    }
                    _ => {
                        // no other statement are modified
                    }
                }
            }
        }
        Ok(data_changed)
    }

    /// processes the select statement to modify the rows.  returns `true` if the rows were modified
    async fn process_select(
        &self,
        select: &Select,
        columns: &[String],
        rows: &mut Vec<Vec<MessageValue>>,
    ) -> Result<bool> {
        let mut modified = false;

        // get the positions of the protected columns in the result
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
        // only do the work if there are columns we are interested in
        if !positions.is_empty() {
            for row in &mut *rows {
                for index in &positions {
                    if let Some(message_value) = row.get_mut(*index) {
                        let protected = self.extract_protected(message_value).unwrap();
                        let new_value = protected.unprotect(&self.key_source, &self.key_id).await?;
                        *message_value = new_value;
                        modified = true;
                    }
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

/// encrypts the message value
fn encrypt(message_value: &MessageValue, sym_key: &Key) -> (Vec<u8>, Nonce) {
    let ser = bincode::serialize(message_value);
    let nonce = secretbox::gen_nonce();
    let ciphertext = secretbox::seal(&ser.unwrap(), &nonce, sym_key);
    (ciphertext, nonce)
}

/// decrypts a message value
fn decrypt(ciphertext: Vec<u8>, nonce: Nonce, sym_key: &Key) -> Result<MessageValue> {
    let decrypted_bytes =
        secretbox::open(&ciphertext, &nonce, sym_key).map_err(|_| anyhow!("couldn't open box"))?;
    //TODO make error handing better here - failure here indicates a authenticity failure
    let decrypted_value: MessageValue =
        bincode::deserialize(&decrypted_bytes).map_err(|_| anyhow!("couldn't open box"))?;
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
    fn from(protected: &Protected) -> Self {
        match protected {
            Protected::Plaintext(_) => panic!(
                "tried to move unencrypted value to plaintext without explicitly calling decrypt"
            ),
            Protected::Ciphertext { .. } => Operand::Const(format!(
                "'{}'",
                hex::encode(Protect::encode(protected).unwrap())
            )),
        }
    }
}

impl Protected {
    // TODO should this actually return self (we are sealing the plaintext value, but we don't swap out the plaintext??
    pub async fn protect(self, key_management: &KeyManager, key_id: &str) -> Result<Protected> {
        let sym_key = key_management
            .cached_get_key(key_id.to_string(), None, None)
            .await?;
        match &self {
            Protected::Plaintext(message_value) => {
                let (cipher, nonce) = encrypt(message_value, &sym_key.plaintext);
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
                    Err(anyhow!("{}", result.err().unwrap()))
                } else {
                    Ok(result.unwrap())
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
                    data_changed |= self.encrypt_columns(statement).await.unwrap();
                    if data_changed {
                        debug!("statement changed to {}", statement);
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
            })) = request.frame()
            {
                if let Some(Frame::Cassandra(CassandraFrame {
                    operation:
                        CassandraOperation::Result(CassandraResult::Rows {
                            value: MessageValue::Rows(rows),
                            ..
                        }),
                    ..
                })) = response.frame()
                {
                    for cql_statement in &mut query.statements {
                        let statement = &mut cql_statement.statement;
                        if let Some(table_name) = CQLStatement::get_table_name(statement) {
                            if let Some(columns) = self.get_protected_columns(table_name) {
                                if let CassandraStatement::Select(select) = &statement {
                                    invalidate_cache |=
                                        self.process_select(select, columns, rows).await?
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
    use crate::frame::CQL;
    use crate::message::MessageValue;
    use crate::transforms::protect::key_management::KeyManager;
    use crate::transforms::protect::local_kek::LocalKeyManagement;
    use crate::transforms::protect::{Protect, Protected};
    use bytes::Bytes;
    use cql3_parser::cassandra_statement::CassandraStatement;
    use cql3_parser::common::Operand;
    use cql3_parser::insert::InsertValues;
    use sodiumoxide::crypto::secretbox::Nonce;
    use std::collections::HashMap;

    #[test]
    fn test_serde() {
        let n: [u8; 24] = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        ];
        let ocipher = Bytes::from("this would be encrypted data").to_vec();
        let ononce = Nonce::from_slice(&n).unwrap();
        let oenc_dek = Bytes::from("this would be enc_dek").to_vec();
        let okek_id = "The KEK id".to_string();

        let protected = Protected::Ciphertext {
            cipher: ocipher.clone(),
            nonce: ononce,
            enc_dek: oenc_dek.clone(),
            kek_id: okek_id.clone(),
        };
        let encoded = Protect::encode(&protected).unwrap();
        let decoded = Protect::decode(&encoded).unwrap();
        if let Protected::Ciphertext {
            cipher,
            nonce,
            enc_dek,
            kek_id,
        } = decoded
        {
            assert_eq!(&ocipher, &cipher);
            assert_eq!(&ononce, &nonce);
            assert_eq!(&oenc_dek, &enc_dek);
            assert_eq!(&okek_id, &kek_id);
        } else {
            panic!("not a Ciphertext")
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    //#[test]
    async fn round_trip_test() {
        if sodiumoxide::init().is_err() {
            panic!("could not init sodiumoxide");
        }

        // verify low level round trip works.
        let kek = sodiumoxide::crypto::secretbox::xsalsa20poly1305::gen_key();
        let local_key_mgr = LocalKeyManagement {
            kek,
            kek_id: "".to_string(),
        };

        let cols = vec!["col1".to_string()];
        let mut tables = HashMap::new();
        tables.insert("test_table".to_string(), cols.clone());
        let mut keyspace_table_columns = HashMap::new();
        keyspace_table_columns.insert("".to_string(), tables);
        let protect = Protect {
            keyspace_table_columns,
            key_source: KeyManager::Local(local_key_mgr),
            key_id: "".to_string(),
        };

        // test protect/unprotect works
        let msg_value = MessageValue::Varchar("Hello World".to_string());
        let plain = Protected::Plaintext(msg_value.clone());
        let encr = plain
            .protect(&protect.key_source, &protect.key_id)
            .await
            .unwrap();
        let new_msg = encr
            .unprotect(&protect.key_source, &protect.key_id)
            .await
            .unwrap();
        assert_eq!(&msg_value, &new_msg);

        // test insert change is reversed on select
        let stmt_txt = "insert into test_table (col1, col2) VALUES ('Hello World', 'i am clean')";
        let mut cql = CQL::parse_from_string(stmt_txt);
        let statement = &mut cql.statements[0].statement;
        let data_changed = protect.encrypt_columns(statement).await.unwrap();
        assert!(data_changed);

        if let CassandraStatement::Insert(insert) = statement {
            if let InsertValues::Values(operands) = &insert.values {
                if let Operand::Const(encr_value) = &operands[0] {
                    assert!(!encr_value.eq("Hello World'"));
                    // remove the quotes
                    let mut hex_value = encr_value.chars();
                    hex_value.next();
                    hex_value.next_back();
                    let s = hex_value.as_str().to_string();
                    let mv = MessageValue::Varchar(s);
                    // build the row
                    let row = vec![mv];
                    let mut rows = vec![row];

                    let stmt_txt = "select col1 from test_table where col2='i am clean'";
                    let cql = CQL::parse_from_string(stmt_txt);
                    let statement = &cql.statements[0].statement;

                    if let CassandraStatement::Select(select) = statement {
                        let result = protect.process_select(select, &cols, &mut rows).await;
                        assert!(result.unwrap());
                        assert_eq!(&msg_value, &rows[0][0]);
                    }
                } else {
                    panic!("Not a const value");
                }
            } else {
                panic!("Not a InsertValues::Values object");
            }
        } else {
            panic!("not an INSERT");
        }
    }
}
