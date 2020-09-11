use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{
    ASTHolder, Message, MessageDetails, Messages, QueryMessage, QueryResponse, Value,
};
use crate::protocols::RawFrame;
use futures::stream::{self, StreamExt};

use redis::cluster_async::{ClusterClientBuilder, ClusterConnection, RoutingInfo};
use redis::{cmd as redis_cmd, Cmd};
use redis::{ErrorKind, ToRedisArgs};
use redis::{Pipeline, RedisError, RedisResult};

use tracing::{debug, trace};

use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};
use itertools::Itertools;
use std::collections::HashMap;

// TODO this may be worth implementing with a redis codec destination
// buttt... the driver already supported a ton of stuff that didn't make sense
// to reimplement. It may be worth reworking the redis driver and redis protocol
// to use the same types. j

const RANDOM_STRING: &str = "<<RANDOM";

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

async fn remap_cluster_commands<'a>(
    connection: &'a mut ClusterConnection,
    mut qd: Wrapper<'a>,
    use_slots: bool,
) -> Result<HashMap<String, Vec<(usize, Cmd)>>, ChainResponse> {
    let mut cmd_map: HashMap<String, Vec<(usize, Cmd)>> = HashMap::new();
    cmd_map.insert(RANDOM_STRING.to_string(), vec![]);
    for (i, message) in qd.message.messages.into_iter().enumerate() {
        if let MessageDetails::Query(qm) = message.details {
            if let Some(ASTHolder::Commands(Value::List(mut commands))) = qm.ast {
                if !commands.is_empty() {
                    let command = commands.remove(0);
                    if let Value::Bytes(b) = &command {
                        let command_string = String::from_utf8(b.to_vec())
                            .unwrap_or_else(|_| "couldn't decode".to_string());

                        let mut redis_command = redis_cmd(command_string.as_str());

                        for args in commands {
                            redis_command.arg(args);
                        }

                        match RoutingInfo::for_packed_command(&redis_command) {
                            Some(RoutingInfo::Random) => {
                                if let Some(cmds) = cmd_map.get_mut(RANDOM_STRING) {
                                    cmds.push((i, redis_command));
                                } else {
                                    cmd_map.insert(
                                        RANDOM_STRING.to_string(),
                                        vec![(i, redis_command)],
                                    );
                                }
                            }
                            Some(RoutingInfo::Slot(slot)) => {
                                let bucket = if use_slots {
                                    format!("{}", slot)
                                } else {
                                    if let Some(server) =
                                        connection.get_connection_string(slot).await
                                    {
                                        server
                                    } else {
                                        return Err(build_error(
                                            "ERR".to_string(),
                                            format!(
                                                "Could not route request: {}",
                                                String::from_utf8_lossy(
                                                    &*redis_command.get_packed_command()
                                                )
                                            ),
                                            None,
                                        ));
                                    }
                                };
                                if let Some(cmds) = cmd_map.get_mut(&bucket) {
                                    cmds.push((i, redis_command));
                                } else {
                                    cmd_map.insert(bucket.to_string(), vec![(i, redis_command)]);
                                }
                            }
                            Some(RoutingInfo::AllNodes) | Some(RoutingInfo::AllMasters) => {
                                for cmds in cmd_map.values_mut() {
                                    cmds.push((i, redis_command.clone()));
                                }
                            }
                            None => {
                                return Err(build_error(
                                    "ERR".to_string(),
                                    "Could not route request".to_string(),
                                    None,
                                ))
                            }
                        }
                    }
                }
            }
        }
    }

    return Ok(cmd_map);
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
                    trace!(connection = ?conn);
                    self.connection = Some(conn)
                }
                Err(error) => {
                    trace!(error = ?error);
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

        trace!("Building pipelined query {:?}", qd.message.messages);

        // Why do we handle these differently? Well the driver unpacks single cmds in a pipeline differently and we don't want to have to handle it.
        // But we still need to fake it being a Vec of results
        if let Some(connection) = &mut self.connection {
            let mut result: RedisResult<Vec<Value>> = Err(RedisError::from((
                ErrorKind::ClientError,
                "couldn't extract single command",
            )));

            if qd.message.messages.len() == 1 {
                let message = qd.message.messages.pop().unwrap();
                if let MessageDetails::Query(qm) = message.details {
                    if let Some(ASTHolder::Commands(Value::List(mut commands))) = qm.ast {
                        if !commands.is_empty() {
                            let command = commands.remove(0);
                            if let Value::Bytes(b) = &command {
                                let command_string = String::from_utf8(b.to_vec())
                                    .unwrap_or_else(|_| "couldn't decode".to_string());

                                let mut redis_command = redis_cmd(command_string.as_str());

                                for args in commands {
                                    redis_command.arg(args);
                                }
                                result =
                                    redis_command.query_async(connection).await.map(|r| vec![r]);
                            }
                        }
                    }
                }
            } else {
                let mut remapped_pipe = match remap_cluster_commands(connection, qd, false).await {
                    Ok(v) => v,
                    Err(e) => {
                        return e;
                    }
                };
                let mut redis_results: Vec<Vec<(usize, Value)>> = vec![];
                for (key, ordered_pipe) in remapped_pipe {
                    let (order, pipe): (Vec<usize>, Vec<Cmd>) = ordered_pipe.into_iter().unzip();
                    let redis_pipe = Pipeline {
                        commands: pipe,
                        transaction_mode: false,
                    };

                    let result: RedisResult<Vec<Value>> = redis_pipe.query_async(connection).await;
                    match result {
                        Ok(rv) => {
                            redis_results.push(order.into_iter().zip(rv.into_iter()).collect_vec());
                        }
                        Err(error) => {
                            trace!(error = ?error);
                            return match error.kind() {
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
                            };
                        }
                    }
                }
                debug!("Got results {:?}", redis_results);
                let ordered_results = redis_results
                    .into_iter()
                    .kmerge_by(|(a_order, _), (b_order, _)| a_order < b_order)
                    .map(|(order, value)| value)
                    .collect_vec();
                debug!("Reordered {:?}", ordered_results);
                result = Ok(ordered_results)
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
