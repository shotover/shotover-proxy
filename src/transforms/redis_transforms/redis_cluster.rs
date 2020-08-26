use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{ASTHolder, Message, QueryMessage, QueryResponse, Value};
use crate::protocols::RawFrame;
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use futures::stream::{self, StreamExt};

use redis::cluster_async::{ClusterClientBuilder, ClusterConnection};
use redis::ErrorKind;
use redis::RedisResult;

use tracing::{debug, trace};

use crate::transforms::{Transforms, TransformsFromConfig};
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::Mutex;

// TODO this may be worth implementing with a redis codec destination
// buttt... the driver already supported a ton of stuff that didn't make sense
// to reimplement. It may be worth reworking the redis driver and redis protocol
// to use the same types. j

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionDetails {
    pub first_contact_points: Vec<String>,
    pub password: Option<String>,
}

pub struct RedisCluster {
    pub name: &'static str,
    pub client: Arc<Mutex<ConnectionDetails>>,
    pub connection: Arc<Mutex<Option<ClusterConnection>>>,
}

impl Clone for RedisCluster {
    fn clone(&self) -> Self {
        RedisCluster {
            name: self.name,
            client: self.client.clone(),
            connection: Arc::new(Mutex::new(None)),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisClusterConfig {
    pub first_contact_points: Vec<String>,
}

#[async_trait]
impl TransformsFromConfig for RedisClusterConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::RedisCluster(RedisCluster {
            name: "RedisCluster",
            client: Arc::new(Mutex::new(ConnectionDetails {
                first_contact_points: self.first_contact_points.clone(),
                password: None,
            })),
            connection: Arc::new(Mutex::new(None)),
        }))
    }
}

fn build_error(code: String, description: String, original: Option<QueryMessage>) -> ChainResponse {
    Ok(Message::Modified(Box::new(Message::Response(
        QueryResponse {
            matching_query: original,
            original: RawFrame::NONE,
            result: None,
            error: Some(Value::Strings(format!("{} {}", code, description))),
            response_meta: None,
        },
    ))))
}

#[async_trait]
impl Transform for RedisCluster {
    async fn transform(&self, qd: Wrapper, _: &TransformChain) -> ChainResponse {
        let mut lock = self.connection.lock().await;

        if lock.is_none() {
            let mut builder_lock = self.client.lock().await;
            let mut eat_message = false;

            let mut client = ClusterClientBuilder::new(builder_lock.first_contact_points.clone());

            if let Message::Query(qm) = &qd.message {
                if let Some(ASTHolder::Commands(Value::List(mut commands))) = qm.ast.clone() {
                    if !commands.is_empty() {
                        let command = commands.remove(0);
                        if let Value::Bytes(b) = &command {
                            let command_string = String::from_utf8(b.to_vec())
                                .unwrap_or_else(|_| "couldn't decode".to_string());

                            trace!(command = %command_string, connection = ?lock);

                            if command_string == "AUTH" {
                                eat_message = true;
                            }

                            if builder_lock.password.is_none() && command_string == "AUTH" {
                                if let Value::Bytes(password) = commands.remove(0) {
                                    builder_lock.deref_mut().password.replace(
                                        String::from_utf8(password.to_vec())
                                            .unwrap_or_else(|_| "couldn't decode".to_string()),
                                    );
                                }
                            }
                        }
                    }
                }
            }

            if let Some(password) = builder_lock.password.as_deref() {
                client = client.password(password.to_string());
            }

            let cli_res = client.readonly(false).open().await;

            let connection_res = cli_res?.get_connection().await;

            match connection_res {
                Ok(conn) => {
                    debug!(connection = ?conn);
                    *lock = Some(conn)
                }
                Err(error) => {
                    debug!(error = ?error);
                    let my_err = build_error(
                        error.code().unwrap_or("ERR").to_string(),
                        error
                            .detail()
                            .unwrap_or("something went wrong?")
                            .to_string(),
                        None,
                    );
                    return my_err;
                }
            }

            if eat_message {
                //We need to eat the auth message and return ok before processing it again
                return Ok(Message::new_mod(Message::Response(
                    QueryResponse::result_with_matching(None, Value::Strings("OK".to_string())),
                )));
            }
        }

        match qd.message {
            Message::Bulk(messages) => {
                debug!("Building pipelined query {:?}", messages);
                let mut pipe = redis::pipe();

                for message in messages {
                    if let Message::Query(qm) = message {
                        if let Some(ASTHolder::Commands(Value::List(mut commands))) = qm.ast {
                            if !commands.is_empty() {
                                let command = commands.remove(0);
                                if let Value::Bytes(b) = &command {
                                    let command_string = String::from_utf8(b.to_vec())
                                        .unwrap_or_else(|_| "couldn't decode".to_string());

                                    pipe.cmd(command_string.as_str());
                                    for args in commands {
                                        pipe.arg(args);
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some(connection) = lock.deref_mut() {
                    let result: RedisResult<Vec<Value>> = pipe.query_async(connection).await;

                    return match result {
                        Ok(result) => {
                            trace!(result = ?result);
                            Ok(Message::Bulk(
                                stream::iter(result)
                                    .then(|v| async move {
                                        Message::new_mod(Message::Response(
                                            QueryResponse::just_result(v),
                                        ))
                                    })
                                    .collect()
                                    .await,
                            ))
                        }
                        Err(error) => {
                            trace!(error = ?error);
                            match error.kind() {
                                ErrorKind::MasterDown
                                | ErrorKind::IoError
                                | ErrorKind::ClientError
                                | ErrorKind::ExtensionError => {
                                    Err(anyhow!("Got connection error with cluster {}", error))
                                }
                                _ => build_error(
                                    error.code().unwrap_or("ERR").to_string(),
                                    error
                                        .detail()
                                        .unwrap_or("something went wrong?")
                                        .to_string(),
                                    None,
                                ),
                            }
                        }
                    };
                }
            }
            Message::Query(qm) => {
                debug!("Building regular query {:?}", qm);
                let original = qm.clone();
                if let Some(ASTHolder::Commands(Value::List(mut commands))) = qm.ast {
                    if !commands.is_empty() {
                        let command = commands.remove(0);
                        if let Value::Bytes(b) = &command {
                            let command_string = String::from_utf8(b.to_vec())
                                .unwrap_or_else(|_| "couldn't decode".to_string());

                            trace!(command = %command_string, connection = ?lock);

                            // Here we create the connection on the first request. This does have a higher startup cost
                            // and impact latency, but its makes the proxy transparent to the client from an authentication
                            // perspective.

                            if let Some(conn) = lock.deref_mut() {
                                let mut cmd = redis::cmd(command_string.as_str());
                                for args in commands {
                                    cmd.arg(args);
                                }
                                let response_res: RedisResult<Value> = cmd.query_async(conn).await;
                                trace!("{:#?}", response_res);

                                return match response_res {
                                    Ok(result) => {
                                        trace!(result = ?result);
                                        Ok(Message::new_mod(Message::Response(
                                            QueryResponse::result_with_matching(
                                                Some(original),
                                                result,
                                            ),
                                        )))
                                    }
                                    Err(error) => {
                                        trace!(error = ?error);
                                        match error.kind() {
                                            ErrorKind::MasterDown
                                            | ErrorKind::IoError
                                            | ErrorKind::ClientError
                                            | ErrorKind::ExtensionError => Err(anyhow!(
                                                "Got connection error with cluster {}",
                                                error
                                            )),
                                            _ => build_error(
                                                error.code().unwrap_or("ERR").to_string(),
                                                error
                                                    .detail()
                                                    .unwrap_or("something went wrong?")
                                                    .to_string(),
                                                Some(original),
                                            ),
                                        }
                                    }
                                };
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Err(anyhow!(
            "Redis Cluster transform did not have enough information to build a request"
        ))
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
