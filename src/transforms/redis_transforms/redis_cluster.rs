use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{
    parse_redis, ASTHolder, Message, MessageDetails, Messages, QueryMessage, QueryResponse, Value,
};
use crate::protocols::RawFrame;
use futures::stream::{self, FuturesUnordered, StreamExt};

use redis::cluster_async::{ClusterClientBuilder, ClusterConnection, RoutingInfo};
use redis::{cmd as redis_cmd, Cmd, ErrorKind};
use redis::{Pipeline, RedisError, RedisResult, Value as RValue};

use tracing::{trace, warn};

use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};
use itertools::Itertools;
use std::collections::HashMap;

use crate::transforms::redis_transforms::redis_cluster::PipelineOrError::ClientError;
use rand::seq::IteratorRandom;
use tokio::time::Duration;

// use tokio::stream::StreamExt as TStreamExt;

// TODO this may be worth implementing with a redis codec destination
// but... the driver already supported a ton of stuff that didn't make sense
// to reimplement. It may be worth reworking the redis driver and redis protocol
// to use the same types. j

const MASTER_STRING: &str = "<<ALL>>";

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionDetails {
    pub first_contact_points: Vec<String>,
    pub password: Option<String>,
    pub username: Option<String>,
}

pub struct RedisCluster {
    pub name: &'static str,
    pub client: ConnectionDetails,
    pub connection: Option<ClusterConnection>,
    reset_connection: bool,
    reset_backoff: u64,
    strict_close_mode: bool,
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

    async fn retry_mapped_commands(
        &mut self,
        moved_list: Vec<(usize, Cmd)>,
        ask_list: Vec<(String, usize, Cmd)>,
        remapped_pipe: &mut HashMap<String, Vec<(usize, Cmd)>>,
    ) -> RedisResult<bool> {
        let mut retry = false;
        if !moved_list.is_empty() {
            trace!("Retrying the following MOVE responses {:?}", ask_list);
            let connection = self.connection.as_mut().unwrap();
            connection.refresh_slots().await?;
            for (order, cmd) in moved_list {
                route_command(connection, false, remapped_pipe, cmd, order, 1).await;
            }
            retry = true;
        }

        if !ask_list.is_empty() {
            trace!("Retrying the following ASK responses {:?}", ask_list);
            let connection = self.connection.as_mut().unwrap();

            for (_host, order, cmd) in ask_list {
                route_command(connection, false, remapped_pipe, cmd, order, 1).await;
            }

            retry = true;
        }
        return Ok(retry);
    }
}

impl Clone for RedisCluster {
    fn clone(&self) -> Self {
        RedisCluster {
            name: self.name,
            client: self.client.clone(),
            connection: None,
            reset_connection: false,
            reset_backoff: 300,
            strict_close_mode: self.strict_close_mode,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisClusterConfig {
    pub first_contact_points: Vec<String>,
    pub strict_close_mode: Option<bool>,
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
            reset_connection: false,
            reset_backoff: 300,
            strict_close_mode: self.strict_close_mode.unwrap_or(true),
        }))
    }
}

fn build_error_from_redis(error: RedisError, count: Option<usize>) -> ChainResponse {
    Ok(build_error_response(
        error.code(),
        error.detail(),
        None,
        count,
    ))
}

fn format_errors(code: Option<&str>, description: Option<&str>) -> Value {
    Value::Strings(format!(
        "{} {}",
        code.unwrap_or("ERR").to_string(),
        description
            .unwrap_or("Redis error from driver, couldn't determine cause")
            .to_string()
    ))
}

fn build_error_response(
    code: Option<&str>,
    description: Option<&str>,
    original: Option<QueryMessage>,
    count: Option<usize>,
) -> Messages {
    match count {
        None => Messages::new_single_response(
            QueryResponse::empty_with_error(Some(format_errors(code, description))),
            true,
            RawFrame::NONE,
        ),
        Some(i) => (0..i)
            .into_iter()
            .map(|_| {
                Message::new(
                    MessageDetails::Response(QueryResponse {
                        matching_query: original.clone(),
                        result: None,
                        error: Some(format_errors(code, description)),
                        response_meta: None,
                    }),
                    true,
                    RawFrame::NONE,
                )
            })
            .collect(),
    }
}

enum PipelineOrError {
    Pipeline(HashMap<String, Vec<(usize, Cmd)>>),
    ClientError(ChainResponse),
}

#[inline(always)]
async fn route_command(
    connection: &mut ClusterConnection,
    use_slots: bool,
    cmd_map: &mut HashMap<String, Vec<(usize, Cmd)>>,
    redis_command: Cmd,
    order: usize,
    error_len: usize,
) -> Option<ChainResponse> {
    match RoutingInfo::for_packed_command(&redis_command) {
        Some(RoutingInfo::Slot(slot)) => {
            let bucket = if use_slots {
                format!("{}", slot)
            } else if let Some(server) = connection.get_connection_string(slot).await {
                server
            } else {
                return Some(Ok(build_error_response(
                    Some("ERR"),
                    Some(&*format!(
                        "Could not route request to redis node (couldn't calculate slot): {}",
                        String::from_utf8_lossy(&*redis_command.get_packed_command())
                    )),
                    None,
                    Some(error_len),
                )));
            };

            if let Some(cmds) = cmd_map.get_mut(&bucket) {
                cmds.push((order, redis_command));
            } else {
                cmd_map.insert(bucket.to_string(), vec![(order, redis_command)]);
            }
        }
        Some(RoutingInfo::AllNodes) | Some(RoutingInfo::AllMasters) => {
            if let Some(cmds) = cmd_map.get_mut(MASTER_STRING) {
                cmds.push((order, redis_command.clone()));
            } else {
                cmd_map.insert(
                    MASTER_STRING.to_string(),
                    vec![(order, redis_command.clone())],
                );
            }
        }
        Some(RoutingInfo::Random) => {
            // these commands are special and need to go in their own bucket so the driver can work it out
            let bucket = connection
                .connections
                .keys()
                .choose(&mut connection.rng)
                .unwrap()
                .to_string();
            if let Some(cmds) = cmd_map.get_mut(&bucket) {
                cmds.push((order, redis_command));
            } else {
                cmd_map.insert(bucket.to_string(), vec![(order, redis_command)]);
            }
        }
        None => {
            return Some(Ok(build_error_response(
                Some("ERR"),
                Some("Could not route request"),
                None,
                Some(error_len),
            )));
        }
    }
    None
}

#[inline(always)]
async fn remap_cluster_commands<'a>(
    connection: &'a mut ClusterConnection,
    qd: Wrapper<'a>,
    use_slots: bool,
) -> PipelineOrError {
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

                        if let Some(response) = route_command(
                            connection,
                            use_slots,
                            &mut cmd_map,
                            redis_command,
                            i,
                            error_len,
                        )
                        .await
                        {
                            return PipelineOrError::ClientError(response);
                        }
                    }
                }
            }
        }
    }
    return PipelineOrError::Pipeline(cmd_map);
}

#[inline(always)]
fn handle_result(
    result: RedisResult<Vec<RedisResult<RValue>>>,
    mut redis_pipe: Pipeline,
    order: Vec<usize>,
    results_to_populate: &mut Vec<Vec<(usize, QueryResponse)>>,
    retry_list: &mut Vec<(usize, Cmd)>,
    ask_list: &mut Vec<(String, usize, Cmd)>,
) -> Result<(), RedisError> {
    match result {
        Ok(rv) => {
            results_to_populate.push(
                order
                    .into_iter()
                    .zip(rv.into_iter().enumerate())
                    .filter_map(|(u, (i, v))| {
                        return match v {
                            Ok(v) => Some((u, QueryResponse::just_result(parse_redis(&v)))),
                            Err(e) => {
                                match e.kind() {
                                    ErrorKind::Moved => {
                                        retry_list.push((u, redis_pipe.commands.remove(i)));
                                        return None;
                                    }
                                    ErrorKind::Ask => {
                                        ask_list.push((
                                            e.redirect_node().unwrap().0.to_string(),
                                            u,
                                            redis_pipe.commands.remove(i),
                                        ));
                                        return None;
                                    }
                                    ErrorKind::TryAgain => {
                                        retry_list.push((u, redis_pipe.commands.remove(i)));
                                        return None;
                                    }
                                    _ => {}
                                }

                                Some((
                                    u,
                                    QueryResponse::empty_with_error(Some(format_errors(
                                        e.code(),
                                        e.detail(),
                                    ))),
                                ))
                            }
                        };
                    })
                    .collect_vec(),
            );
        }
        Err(error) => {
            results_to_populate.push(
                order
                    .into_iter()
                    .map(|u| {
                        (
                            u,
                            QueryResponse::empty_with_error(Some(format_errors(
                                error.code(),
                                error.detail(),
                            ))),
                        )
                    })
                    .collect_vec(),
            );
            if ErrorKind::IoError == error.kind() || ErrorKind::ResponseError == error.kind() {
                // self.connection.take(); // Turns this to a None
                warn!("Redis Cluster Connection reset, reconnecting on next request");
                return Err(error);
            }
        }
    }
    Ok(())
}

#[async_trait]
impl Transform for RedisCluster {
    async fn transform<'a>(&'a mut self, mut qd: Wrapper<'a>) -> ChainResponse {
        if self.reset_connection {
            if self.reset_backoff > 2400 || self.strict_close_mode {
                return Err(anyhow!("too many connection resets, killing chain"));
            }
            warn!("resetting connection");
            tokio::time::delay_for(Duration::from_millis(self.reset_backoff)).await;
            warn!("connection reset");
            self.reset_backoff *= 2;
            self.connection = None;
            self.reset_connection = false;
        }

        let expected_response = qd.message.messages.len();
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

                            trace!(command = %command_string, connection = ?self.client);

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
                warn!(error = ?error);
                return build_error_from_redis(error, Some(expected_response));
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

        // if let Some(connection) = &mut self.connection {
        if self.connection.is_some() {
            let mut remapped_pipe =
                match remap_cluster_commands(self.connection.as_mut().unwrap(), qd, false).await {
                    PipelineOrError::Pipeline(v) => v,
                    ClientError(e) => {
                        return e;
                    }
                };

            let mut redis_results: Vec<Vec<(usize, QueryResponse)>> = vec![];
            let mut try_count: i32 = 0;

            while try_count < 5 {
                try_count += 1;
                if try_count > 1 {
                    warn!("retrying operation {}", try_count);
                }
                trace!("remapped_pipe: {:?}", remapped_pipe);
                let mut moved_list: Vec<(usize, Cmd)> = Vec::new();
                let mut ask_list: Vec<(String, usize, Cmd)> = Vec::new();

                if let Some(ordered_pipe) = remapped_pipe.remove(MASTER_STRING) {
                    // We let the driver take care of hitting all nodes
                    let (order, pipe): (Vec<usize>, Vec<Cmd>) = ordered_pipe.into_iter().unzip();
                    let result: RedisResult<Vec<RedisResult<_>>>;

                    // Why do we handle these differently? Well the driver unpacks single cmds in a pipeline differently and we don't want to have to handle it.
                    // But we still need to fake it being a Vec of results

                    let redis_pipe;
                    if pipe.len() == 1 {
                        let cmd = pipe.get(0).unwrap();
                        let q_result: RedisResult<_> =
                            cmd.query_async(self.connection.as_mut().unwrap()).await;
                        result = Ok(vec![q_result]);
                        redis_pipe = Pipeline {
                            commands: pipe,
                            transaction_mode: false,
                        };
                    } else {
                        redis_pipe = Pipeline {
                            commands: pipe,
                            transaction_mode: false,
                        };

                        result = redis_pipe
                            .execute_pipelined_async_raw(self.connection.as_mut().unwrap())
                            .await;
                        trace!("returned redis result ALL_MASTERS - {:?}", result);
                    }

                    if let Err(error) = handle_result(
                        result,
                        redis_pipe,
                        order,
                        &mut redis_results,
                        &mut moved_list,
                        &mut ask_list,
                    ) {
                        self.reset_connection = true;
                        return build_error_from_redis(error, Some(expected_response));
                    }
                }

                // scoping the mutable borrow of the connections
                {
                    let mut result_future = FuturesUnordered::new();

                    for (key, ordered_pipe) in remapped_pipe.into_iter() {
                        match self.connection.as_mut().unwrap().connections.remove(&key) {
                            Some(mut connection_reference) => {
                                result_future.push(async move {
                                    let (order, pipe): (Vec<usize>, Vec<Cmd>) =
                                        ordered_pipe.into_iter().unzip();
                                    trace!("order: {:?}", order);
                                    trace!("pipe: {:?}", pipe);

                                    let result: RedisResult<Vec<RedisResult<_>>>;

                                    // Why do we handle these differently? Well the driver unpacks single cmds in a pipeline differently and we don't want to have to handle it.
                                    // But we still need to fake it being a Vec of results
                                    let redis_pipe;
                                    if pipe.len() == 1 {
                                        let cmd = pipe.get(0).unwrap();
                                        let q_result: RedisResult<_> =
                                            cmd.query_async(&mut connection_reference).await;
                                        result = Ok(vec![q_result]);
                                        redis_pipe = Pipeline {
                                            commands: pipe,
                                            transaction_mode: false,
                                        };
                                    // result = q_result.map(|r| vec![r]);
                                    } else {
                                        redis_pipe = Pipeline {
                                            commands: pipe,
                                            transaction_mode: false,
                                        };

                                        result = redis_pipe
                                            .execute_pipelined_async_raw(&mut connection_reference)
                                            .await;

                                        trace!("returned redis result {} - {:?}", key, result);
                                    }
                                    (order, (result, redis_pipe), (key, connection_reference))
                                });
                            }
                            None => {
                                let (order, _pipe): (Vec<usize>, Vec<Cmd>) =
                                    ordered_pipe.into_iter().unzip();

                                self.reset_connection = true;

                                redis_results.push(
                                    order
                                        .into_iter()
                                        .map(|u| {
                                            (
                                                u,
                                                QueryResponse::empty_with_error(Some(format_errors(
                                                    Some("Err"),
                                                    Some("Couldn't get upstream master connection"),
                                                ))),
                                            )
                                        })
                                        .collect_vec(),
                                );
                            }
                        }
                    }

                    while let Some((order, (result, redis_pipe), (key, connection))) =
                        result_future.next().await
                    {
                        match handle_result(
                            result,
                            redis_pipe,
                            order,
                            &mut redis_results,
                            &mut moved_list,
                            &mut ask_list,
                        ) {
                            Err(error) => {
                                self.reset_connection = true;
                                warn!("handle error {}", error);
                                // return build_error_from_redis(error, Some(expected_response));
                            }
                            Ok(..) => {
                                self.connection.as_mut().map(|cc| {
                                    cc.connections.insert(key, connection);
                                });
                            }
                        }
                    }
                }
                trace!("Got results {:?}", redis_results);

                if !moved_list.is_empty() || !ask_list.is_empty() {
                    remapped_pipe = HashMap::new();

                    match self
                        .retry_mapped_commands(moved_list, ask_list, &mut remapped_pipe)
                        .await
                    {
                        Ok(b) => {
                            if b {
                                continue;
                            }
                        }
                        Err(e) => {
                            warn!("handle error {} - retry map commands", e);

                            return build_error_from_redis(e, Some(expected_response));
                        }
                    }
                }

                let ordered_results = redis_results
                    .into_iter()
                    .kmerge_by(|(a_order, _), (b_order, _)| a_order < b_order)
                    .map(|(_order, value)| value)
                    .collect_vec();
                trace!("Reordered {:?}", ordered_results);
                trace!(
                    "Connections {}",
                    self.connection.as_ref().unwrap().connections.len()
                );
                assert_eq!(expected_response, ordered_results.len());
                // assert_eq!(remapped_pipe.len(), 0);

                return Ok(Messages {
                    messages: stream::iter(ordered_results)
                        .then(|v| async move { Message::new_response(v, true, RawFrame::NONE) })
                        .collect()
                        .await,
                });
            }
        }

        Ok(build_error_response(
            Some("ERR"),
            Some("Shotover couldn't connect to upstream redis cluster"),
            None,
            None,
        ))
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
