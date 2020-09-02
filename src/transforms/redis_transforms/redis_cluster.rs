use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{
    ASTHolder, Message, MessageDetails, Messages, QueryMessage, QueryResponse, Value,
};
use crate::protocols::RawFrame;
use futures::stream::{self, StreamExt};

use redis::cluster_async::{ClusterClientBuilder, ClusterConnection};
use redis::ErrorKind;
use redis::RedisResult;

use tracing::{debug, trace};

use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};

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
    pub client: ConnectionDetails,
    pub connection: Option<ClusterConnection>,
}

impl Clone for RedisCluster {
    fn clone(&self) -> Self {
        RedisCluster {
            name: self.name,
            client: self.client.clone(),
            connection: None,
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
            client: ConnectionDetails {
                first_contact_points: self.first_contact_points.clone(),
                password: None,
            },
            connection: None,
        }))
    }
}

fn build_error(code: String, description: String, original: Option<QueryMessage>) -> ChainResponse {
    Ok(Messages::new_single_response(
        QueryResponse {
            matching_query: original,
            result: None,
            error: Some(Value::Strings(format!("{} {}", code, description))),
            response_meta: None,
        },
        true,
        RawFrame::NONE,
    ))
}

#[async_trait]
impl Transform for RedisCluster {
    async fn transform<'a>(&'a mut self, mut qd: Wrapper<'a>) -> ChainResponse {
        if self.connection.is_none() {
            let mut eat_message = false;

            let mut client = ClusterClientBuilder::new(self.client.first_contact_points.clone());

            if let Some(Message {
                details: MessageDetails::Query(qm),
                modified: _,
                original: _,
            }) = &qd.message.messages.get(0)
            {
                if let Some(ASTHolder::Commands(Value::List(mut commands))) = qm.ast.clone() {
                    if !commands.is_empty() {
                        let command = commands.remove(0);
                        if let Value::Bytes(b) = &command {
                            let command_string = String::from_utf8(b.to_vec())
                                .unwrap_or_else(|_| "couldn't decode".to_string());

                            trace!(command = %command_string, connection = ?self.client);

                            if command_string == "AUTH" {
                                eat_message = true;
                            }

                            if self.client.password.is_none() && command_string == "AUTH" {
                                if let Value::Bytes(password) = commands.remove(0) {
                                    self.client.password.replace(
                                        String::from_utf8(password.to_vec())
                                            .unwrap_or_else(|_| "couldn't decode".to_string()),
                                    );
                                }
                            }
                        }
                    }
                }
            }

            if let Some(password) = self.client.password.as_deref() {
                client = client.password(password.to_string());
            }

            let cli_res = client.readonly(false).open().await;

            let connection_res = cli_res?.get_connection().await;

            match connection_res {
                Ok(conn) => {
                    debug!(connection = ?conn);
                    self.connection = Some(conn)
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
                if qd.message.messages.len() <= 1 {
                    return Ok(Messages::new_single_response(
                        QueryResponse::result_with_matching(None, Value::Strings("OK".to_string())),
                        true,
                        RawFrame::NONE,
                    ));
                } else {
                    // looks like there was an AUTH message at the top of the pipeline, so just eat the AUTH message and continue to processing
                    qd.message.messages.remove(0);
                }
            }
        }

        debug!("Building pipelined query {:?}", qd.message.messages);
        let mut pipe = redis::pipe();

        for message in qd.message.messages {
            if let MessageDetails::Query(qm) = message.details {
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

        // Why do we handle these differently? Well the driver unpacks single cmds in a pipeline differently and we don't want to have to handle it.
        // But we still need to fake it being a Vec of results
        if let Some(connection) = &mut self.connection {
            let result: RedisResult<Vec<Value>> = if pipe.len() == 1 {
                let cmd = pipe.pop().unwrap();
                cmd.query_async(connection).await.map(|r| vec![r])
            } else {
                pipe.query_async(connection).await
            };

            return match result {
                Ok(result) => {
                    trace!(result = ?result);
                    Ok(Messages {
                        messages: stream::iter(result)
                            .then(|v| async move {
                                Message::new_response(
                                    QueryResponse::just_result(v),
                                    true,
                                    RawFrame::NONE,
                                )
                            })
                            .collect()
                            .await,
                    })
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

        Err(anyhow!(
            "Redis Cluster transform did not have enough information to build a request"
        ))
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
