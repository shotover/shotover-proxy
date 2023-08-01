use crate::frame::{
    value::GenericValue, CassandraFrame, CassandraOperation, CassandraResult, Frame,
};
use crate::message::Messages;
use crate::transforms::protect::key_management::KeyManager;
pub use crate::transforms::protect::key_management::KeyManagerConfig;
use crate::transforms::{Transform, TransformBuilder, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::Identifier;
use cql3_parser::insert::InsertValues;
use cql3_parser::select::SelectElement;
use serde::Deserialize;
use std::collections::HashMap;

mod aws_kms;
mod crypto;
mod key_management;
mod local_kek;
mod pkcs_11;

#[derive(Deserialize, Debug)]
pub struct ProtectConfig {
    pub keyspace_table_columns: HashMap<String, HashMap<String, Vec<String>>>,
    pub key_manager: KeyManagerConfig,
}

#[cfg(feature = "alpha-transforms")]
use crate::transforms::TransformConfig;

#[cfg(feature = "alpha-transforms")]
#[typetag::deserialize(name = "Protect")]
#[async_trait(?Send)]
impl TransformConfig for ProtectConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(Protect {
            keyspace_table_columns: self
                .keyspace_table_columns
                .iter()
                .map(|(k, v)| {
                    (
                        Identifier::Quoted(k.clone()),
                        v.iter()
                            .map(|(k, v)| {
                                (
                                    Identifier::Quoted(k.clone()),
                                    v.iter().map(|x| Identifier::Quoted(x.clone())).collect(),
                                )
                            })
                            .collect(),
                    )
                })
                .collect(),
            key_source: self.key_manager.build()?,
            key_id: "XXXXXXX".to_string(),
        }))
    }
}

#[derive(Clone)]
pub struct Protect {
    /// map of keyspace Identifiers to map of table Identifiers to column Identifiers
    keyspace_table_columns: HashMap<Identifier, HashMap<Identifier, Vec<Identifier>>>,
    key_source: KeyManager,
    // TODO this should be a function to create key_ids based on "something", e.g. primary key
    // for the moment this is just a string
    key_id: String,
}

impl TransformBuilder for Protect {
    fn build(&self) -> Transforms {
        Transforms::Protect(Box::new(self.clone()))
    }

    fn get_name(&self) -> &'static str {
        "Protect"
    }
}

impl Protect {
    fn get_protected_columns(&self, statement: &CassandraStatement) -> &[Identifier] {
        // TODO replace `Identifier::default()` with cached keyspace name
        if let Some(table_name) = statement.get_table_name() {
            if let Some(tables) = self.keyspace_table_columns.get(
                table_name
                    .keyspace
                    .as_ref()
                    .unwrap_or(&Identifier::default()),
            ) {
                if let Some(columns) = tables.get(&table_name.name) {
                    return columns;
                }
            }
        }
        &[]
    }

    /// Encrypts any values in the insert/update statements that are configured to be encrypted.
    /// Returns `true` if any columns were changed.
    async fn encrypt_columns(&self, statement: &mut CassandraStatement) -> Result<bool> {
        let mut invalidate_cache = false;
        let columns_to_encrypt = self.get_protected_columns(statement);
        match statement {
            CassandraStatement::Insert(insert) => {
                for (i, col_name) in insert.columns.iter().enumerate() {
                    if columns_to_encrypt.contains(col_name) {
                        match &mut insert.values {
                            InsertValues::Values(value_operands) => {
                                if let Some(value) = value_operands.get_mut(i) {
                                    *value = crypto::encrypt(value, &self.key_source, &self.key_id)
                                        .await?;
                                    invalidate_cache = true
                                }
                            }
                            InsertValues::Json(_) => todo!("parse json and encrypt."),
                        }
                    }
                }
            }
            CassandraStatement::Update(update) => {
                for assignment in &mut update.assignments {
                    if columns_to_encrypt.contains(&assignment.name.column) {
                        assignment.value =
                            crypto::encrypt(&assignment.value, &self.key_source, &self.key_id)
                                .await?;
                        invalidate_cache = true;
                    }
                }
            }
            _ => {
                // no other statements are modified
            }
        }
        Ok(invalidate_cache)
    }

    /// Decrypts any values in the rows that are configured to be encrypted.
    /// Returns `true` if any columns were changed.
    async fn decrypt_results(
        &self,
        statement: &CassandraStatement,
        rows: &mut Vec<Vec<GenericValue>>,
    ) -> Result<bool> {
        let mut invalidate_cache = false;
        if let CassandraStatement::Select(select) = &statement {
            let columns_to_decrypt = self.get_protected_columns(statement);
            for (i, col) in select.columns.iter().enumerate() {
                if let SelectElement::Column(col) = col {
                    if columns_to_decrypt.contains(&col.name) {
                        for row in &mut *rows {
                            if let Some(message_value) = row.get_mut(i) {
                                *message_value =
                                    crypto::decrypt(message_value, &self.key_source, &self.key_id)
                                        .await?;
                                invalidate_cache = true;
                            }
                        }
                    }
                }
            }
        }
        Ok(invalidate_cache)
    }
}

#[async_trait]
impl Transform for Protect {
    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        // encrypt the values included in any INSERT or UPDATE queries
        for message in requests_wrapper.requests.iter_mut() {
            let mut invalidate_cache = false;

            if let Some(Frame::Cassandra(CassandraFrame { operation, .. })) = message.frame() {
                for statement in operation.queries() {
                    invalidate_cache |= self.encrypt_columns(statement).await.unwrap();
                }
            }
            if invalidate_cache {
                message.invalidate_cache();
            }
        }

        let mut original_messages = requests_wrapper.requests.clone();
        let mut result = requests_wrapper.call_next_transform().await?;

        for (response, request) in result.iter_mut().zip(original_messages.iter_mut()) {
            let mut invalidate_cache = false;
            if let Some(Frame::Cassandra(CassandraFrame { operation, .. })) = request.frame() {
                if let Some(Frame::Cassandra(CassandraFrame {
                    operation: CassandraOperation::Result(CassandraResult::Rows { rows, .. }),
                    ..
                })) = response.frame()
                {
                    for statement in operation.queries() {
                        invalidate_cache |= self.decrypt_results(statement, rows).await?
                    }
                }
            }
            if invalidate_cache {
                response.invalidate_cache();
            }
        }

        Ok(result)
    }
}
