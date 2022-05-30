use crate::error::ChainResponse;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::MessageValue;
use crate::transforms::protect::key_management::{KeyManager, KeyManagerConfig};
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use itertools::Itertools;
use serde::Deserialize;
use sqlparser::ast::{Assignment, Expr, Ident, Query, SetExpr, Statement, Value as SQLValue};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use tracing::warn;

mod aws_kms;
mod crypto;
mod key_management;
mod local_kek;
mod pkcs_11;

#[derive(Deserialize, Debug, Clone)]
pub struct ProtectConfig {
    pub keyspace_table_columns: HashMap<String, HashMap<String, Vec<String>>>,
    pub key_manager: KeyManagerConfig,
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

#[derive(Clone)]
pub struct Protect {
    keyspace_table_columns: HashMap<String, HashMap<String, Vec<String>>>,
    key_source: KeyManager,
    // TODO this should be a function to create key_ids based on "something", e.g. primary key
    // for the moment this is just a string
    key_id: String,
}

pub fn get_values_from_insert_or_update_mut(ast: &mut Statement) -> HashMap<String, &mut SQLValue> {
    match ast {
        Statement::Insert {
            source, columns, ..
        } => get_values_from_insert_mut(columns.as_mut(), source.borrow_mut()),
        Statement::Update { assignments, .. } => get_values_from_update_mut(assignments.as_mut()),
        _ => HashMap::new(),
    }
}

fn get_values_from_insert_mut<'a>(
    columns: &'a mut [Ident],
    source: &'a mut Query,
) -> HashMap<String, &'a mut SQLValue> {
    let mut map = HashMap::new();
    let mut columns_iter = columns.iter();
    if let SetExpr::Values(v) = &mut source.body {
        for value in &mut v.0 {
            for ex in value {
                if let Expr::Value(v) = ex {
                    if let Some(c) = columns_iter.next() {
                        map.insert(c.value.to_string(), v);
                    }
                }
            }
        }
    }
    map
}

fn get_values_from_update_mut(assignments: &mut [Assignment]) -> HashMap<String, &mut SQLValue> {
    let mut map = HashMap::new();
    for assignment in assignments {
        if let Expr::Value(v) = &mut assignment.value {
            map.insert(assignment.id.iter().map(|x| &x.value).join("."), v);
        }
    }
    map
}

#[async_trait]
impl Transform for Protect {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        // encrypt the values included in any INSERT or UPDATE queries
        for message in message_wrapper.messages.iter_mut() {
            let mut invalidate_cache = false;
            if let Some(namespace) = message.namespace() {
                if namespace.len() == 2 {
                    if let Some(Frame::Cassandra(frame)) = message.frame() {
                        if let Ok(queries) = frame.operation.queries() {
                            for query in queries {
                                if let Some((_, tables)) =
                                    self.keyspace_table_columns.get_key_value(&namespace[0])
                                {
                                    if let Some((_, columns)) = tables.get_key_value(&namespace[1])
                                    {
                                        let mut values =
                                            get_values_from_insert_or_update_mut(query);
                                        for col in columns {
                                            if let Some(value) = values.get_mut(col) {
                                                **value = crypto::encrypt(
                                                    value,
                                                    &self.key_source,
                                                    &self.key_id,
                                                )
                                                .await?;
                                                invalidate_cache = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if invalidate_cache {
                message.invalidate_cache();
            }
        }

        let mut original_messages = message_wrapper.messages.clone();
        let mut result = message_wrapper.call_next_transform().await?;

        for (response, request) in result.iter_mut().zip(original_messages.iter_mut()) {
            let mut invalidate_cache = false;
            if let Some(Frame::Cassandra(CassandraFrame {
                operation:
                    CassandraOperation::Result(CassandraResult::Rows {
                        value: MessageValue::Rows(rows),
                        ..
                    }),
                ..
            })) = response.frame()
            {
                if let Some(namespace) = request.namespace() {
                    if let Some(Frame::Cassandra(frame)) = request.frame() {
                        if let Ok(queries) = frame.operation.queries() {
                            for query in queries {
                                let projection: Vec<String> =
                                    get_values_from_insert_or_update_mut(query)
                                        .into_keys()
                                        .collect();
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
                                            for row in rows.iter_mut() {
                                                for index in &mut positions {
                                                    if let Some(v) = row.get_mut(*index) {
                                                        if let MessageValue::Bytes(_) = v {
                                                            *v = crypto::decrypt(
                                                                v,
                                                                &self.key_source,
                                                                &self.key_id,
                                                            )
                                                            .await?;
                                                            invalidate_cache = true;
                                                        } else {
                                                            warn!(
                                                                "Tried decrypting non-blob column"
                                                            )
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
