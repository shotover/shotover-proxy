use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{
    parse_redis, ASTHolder, Message, MessageDetails, Messages, QueryMessage, QueryResponse, Value,
};
use crate::protocols::RawFrame;
use futures::stream::{self, StreamExt};

use redis::cluster_async::{ClusterClientBuilder, ClusterConnection, RoutingInfo};
use redis::{cmd as redis_cmd, Cmd, ErrorKind};
use redis::{Pipeline, RedisError, RedisResult};

use tracing::{debug, info, instrument, trace, warn};

use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};
use cached::Cached;
use itertools::Itertools;
use rand::RngCore;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::error::Error;
use std::iter::FromIterator;

// TODO this may be worth implementing with a redis codec destination
// but... the driver already supported a ton of stuff that didn't make sense
// to reimplement. It may be worth reworking the redis driver and redis protocol
// to use the same types. j

const RANDOM_STRING: &str = "<<RANDOM";

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionDetails {
    pub first_contact_points: Vec<String>,
    pub password: Option<String>,
    pub username: Option<String>,
}

#[derive(Debug)]
pub struct RedisCluster {
    pub name: &'static str,
    pub client: ConnectionDetails,
    pub connection: Option<ClusterConnection>,
}

impl RedisCluster {
    pub(crate) async fn try_connect(&mut self) -> RedisResult<()> {
        let mut client = ClusterClientBuilder::new(self.client.first_contact_points.clone());

        if let Some(username) = self.client.username.as_deref() {
            client = client.username(username.to_string());
        }

        if let Some(password) = self.client.password.as_deref() {
            client = client.password(password.to_string());
        }

        let cli_res = client.readonly(false).open().await;

        let conn = cli_res?.get_connection().await?;

        trace!(connection = ?conn);
        self.connection.replace(conn);
        // self.connection = Some(conn);

        Ok(())
    }
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
                username: None,
            },
            connection: None,
        }))
    }
}

fn build_error_from_redis(error: RedisError, count: Option<usize>) -> ChainResponse {
    Ok(build_error_response(
        error.code(),
        Some(&*error.to_string()),
        error.detail(),
        None,
        count,
    ))
}

fn format_errors(code: Option<&str>, description: Option<&str>, detail: Option<&str>) -> Value {
    Value::Strings(format!(
        "{} {}",
        code.unwrap_or("ERR").to_string(),
        detail
            .or(description)
            .unwrap_or("Redis error from driver, couldn't determine cause")
            .to_string()
    ))
}

fn build_error_response(
    code: Option<&str>,
    description: Option<&str>,
    detail: Option<&str>,
    original: Option<QueryMessage>,
    count: Option<usize>,
) -> Messages {
    return match count {
        None => Messages::new_single_response(
            QueryResponse::empty_with_error(Some(format_errors(code, description, detail))),
            true,
            RawFrame::NONE,
        ),
        Some(i) => Messages::from_iter((0..i).into_iter().map(|_| {
            Message::new(
                MessageDetails::Response(QueryResponse {
                    matching_query: original.clone(),
                    result: None,
                    error: Some(format_errors(code, description, detail)),
                    response_meta: None,
                }),
                true,
                RawFrame::NONE,
            )
        })),
    };
}

async fn remap_cluster_commands<'a>(
    connection: &'a mut ClusterConnection,
    qd: Wrapper<'a>,
    use_slots: bool,
) -> Result<HashMap<String, Vec<(usize, Cmd)>>, ChainResponse> {
    let mut cmd_map: HashMap<String, Vec<(usize, Cmd)>> = HashMap::new();
    let error_len = qd.message.messages.len();
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
                            Some(RoutingInfo::Slot(slot)) => {
                                let bucket = if use_slots {
                                    format!("{}", slot)
                                } else if let Some(server) =
                                    connection.get_connection_string(slot).await
                                {
                                    server
                                } else {
                                    return Err(Ok(build_error_response(
                                        Some("ERR"),
                                        Some(&*format!(
                                            "Could not route request to redis node (couldn't calculate slot): {}",
                                            String::from_utf8_lossy(
                                                &*redis_command.get_packed_command()
                                            )
                                        )),
                                        None,
                                        None,
                                        Some(error_len)
                                    )));
                                };

                                if let Some(cmds) = cmd_map.get_mut(&bucket) {
                                    cmds.push((i, redis_command));
                                } else {
                                    cmd_map.insert(bucket.to_string(), vec![(i, redis_command)]);
                                }
                            }
                            Some(RoutingInfo::AllNodes)
                            | Some(RoutingInfo::AllMasters)
                            | Some(RoutingInfo::Random) => {
                                // these commands are special and need to go in their own bucket so the driver can work it out
                                let mut insert_value = vec![(i, redis_command.clone())];
                                while let Some(collision) = cmd_map
                                    .insert(format!("{}", connection.rng.next_u64()), insert_value)
                                {
                                    insert_value = collision;
                                    trace!("collision in command remapping hash")
                                }
                            }
                            None => {
                                return Err(Ok(build_error_response(
                                    Some("ERR"),
                                    Some("Could not route request"),
                                    None,
                                    None,
                                    Some(error_len),
                                )))
                            }
                        }
                    }
                }
            }
        }
    }

    return Ok(cmd_map);
}

fn unwrap_borrow_mut_option<T>(option: &mut Option<T>) -> &mut T {
    match option {
        None => panic!("Could not borrow reference to None"),
        Some(a) => a.borrow_mut(),
    }
}

#[async_trait]
impl Transform for RedisCluster {
    async fn transform<'a>(&'a mut self, mut qd: Wrapper<'a>) -> ChainResponse {
        let origin_client = qd.from_client.clone();

        if self.connection.is_none() {
            let mut eat_message = false;

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

                            trace!(command = %command_string);

                            if command_string == "AUTH" {
                                eat_message = true;
                            }

                            if self.client.password.is_none() && command_string == "AUTH" {
                                if commands.len() == 2 {
                                    if let Value::Bytes(username) = commands.remove(0) {
                                        self.client.username.replace(
                                            String::from_utf8(username.to_vec())
                                                .unwrap_or_else(|_| "couldn't decode".to_string()),
                                        );
                                    }
                                }
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

            if let Err(error) = self.try_connect().await {
                trace!(error = ?error);
                return build_error_from_redis(error, None);
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

        // if let Some(connection) = &mut self.connection {
        if self.connection.is_some() {
            let remapped_pipe = match remap_cluster_commands(
                unwrap_borrow_mut_option(&mut self.connection),
                qd,
                false,
            )
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    return e;
                }
            };

            let mut redis_results: Vec<Vec<(usize, QueryResponse)>> = vec![];
            trace!(?remapped_pipe);
            for (key, ordered_pipe) in remapped_pipe {
                let (order, mut pipe): (Vec<usize>, Vec<Cmd>) = ordered_pipe.into_iter().unzip();
                trace!(?order);
                trace!(?pipe);

                let result: RedisResult<Vec<RedisResult<_>>>;

                // Why do we handle these differently? Well the driver unpacks single cmds in a pipeline differently and we don't want to have to handle it.
                // But we still need to fake it being a Vec of results
                if pipe.len() == 1 {
                    let cmd = pipe.pop().unwrap();
                    let q_result: RedisResult<_> = cmd
                        .query_async(unwrap_borrow_mut_option(&mut self.connection))
                        .await;
                    result = Ok(vec![q_result]);
                // result = q_result.map(|r| vec![r]);
                } else {
                    let redis_pipe = Pipeline {
                        commands: pipe,
                        transaction_mode: false,
                    };

                    result = redis_pipe
                        .execute_pipelined_async_raw(unwrap_borrow_mut_option(&mut self.connection))
                        .await;
                    trace!(?key, ?result);
                }

                match result {
                    Ok(rv) => {
                        redis_results.push(
                            order
                                .into_iter()
                                .zip(rv.into_iter().map(|v| {
                                    return match v {
                                        Ok(v) => QueryResponse::just_result(parse_redis(&v)),
                                        Err(e) => {
                                            QueryResponse::empty_with_error(Some(format_errors(
                                                e.code(),
                                                Some(e.description()),
                                                e.detail(),
                                            )))
                                        }
                                    };
                                }))
                                .collect_vec(),
                        );
                    }
                    Err(error) => {
                        if ErrorKind::IoError == error.kind()
                            || ErrorKind::ResponseError == error.kind()
                        {
                            self.connection.take(); // Turns this to a None
                            warn!(?error)
                        }
                        trace!(error = ?error);
                        redis_results.push(
                            order
                                .into_iter()
                                .map(|u| {
                                    (
                                        u,
                                        QueryResponse::empty_with_error(Some(format_errors(
                                            error.code(),
                                            Some(&*error.to_string()),
                                            error.detail(),
                                        ))),
                                    )
                                })
                                .collect_vec(),
                        );
                    }
                }
            }
            trace!(?redis_results);
            let ordered_results = redis_results
                .into_iter()
                .kmerge_by(|(a_order, _), (b_order, _)| a_order < b_order)
                .map(|(_order, value)| value)
                .collect_vec();
            trace!(?ordered_results);

            return Ok(Messages {
                messages: stream::iter(ordered_results)
                    .then(|v| async move { Message::new_response(v, true, RawFrame::NONE) })
                    .collect()
                    .await,
            });
        }

        Ok(build_error_response(
            Some("ERR"),
            Some("Shotover couldn't connect to upstream redis cluster"),
            None,
            None,
            None,
        ))
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
