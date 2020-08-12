use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{ASTHolder, Message, QueryResponse, Value};
use crate::protocols::RawFrame;
use crate::transforms::chain::{Transform, TransformChain, Wrapper};

use redis::cluster::{ClusterClient, ClusterConnection};
use redis::ErrorKind;
use redis::RedisResult;

use tracing::info;

use crate::transforms::{Transforms, TransformsFromConfig};
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::Mutex;

//TODO this may be worth implementing with a redis codec destination
// buttt... the driver already supported a ton of stuff that didn't make sense
// to reimplement. It may be worth reworking the redis driver and redis protocol
// to use the same types. j

pub struct RedisCluster {
    pub name: &'static str,
    pub client: ClusterClient,
    pub connection: Arc<Mutex<ClusterConnection>>,
}

impl Clone for RedisCluster {
    fn clone(&self) -> Self {
        let connection = self.client.get_connection().unwrap();
        return RedisCluster {
            name: self.name.clone(),
            client: self.client.clone(),
            connection: Arc::new(Mutex::new(connection)),
        };
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisClusterConfig {
    pub first_contact_points: Vec<String>,
}

#[async_trait]
impl TransformsFromConfig for RedisClusterConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        let client = ClusterClient::open(self.first_contact_points.clone()).unwrap();

        let connection = client.get_connection().unwrap();

        Ok(Transforms::RedisCluster(RedisCluster {
            name: "RedisCluster",
            client,
            connection: Arc::new(Mutex::new(connection)),
        }))
    }
}

#[async_trait]
impl Transform for RedisCluster {
    async fn transform(&self, qd: Wrapper, _: &TransformChain) -> ChainResponse {
        if let Message::Query(qm) = qd.message {
            let original = qm.clone();
            if let Some(ASTHolder::Commands(Value::List(mut commands))) = qm.ast {
                if commands.len() > 0 {
                    let command = commands.remove(0);
                    if let Value::Bytes(b) = &command {
                        let command_string = String::from_utf8(b.to_vec())
                            .unwrap_or_else(|_| "couldn't decode".to_string());
                        if let Ok(mut conn) = self.connection.try_lock() {
                            let mut cmd = redis::cmd(command_string.as_str());
                            for args in commands {
                                cmd.arg(args);
                            }
                            let response_res: RedisResult<Value> = cmd.query(conn.deref_mut());
                            return match response_res {
                                Ok(result) => {
                                    info!("{:#?}", result);
                                    Ok(Message::Modified(Box::new(Message::Response(
                                        QueryResponse {
                                            matching_query: Some(original),
                                            original: RawFrame::NONE,
                                            result: Some(result),
                                            error: None,
                                            response_meta: None,
                                        },
                                    ))))
                                }
                                Err(error) => {
                                    info!("e: {}", error);
                                    match error.kind() {
                                        ErrorKind::MasterDown
                                        | ErrorKind::IoError
                                        | ErrorKind::ClientError
                                        | ErrorKind::ExtensionError => Err(anyhow!(
                                            "Got connection error with cluster {}",
                                            error
                                        )),
                                        _ => Ok(Message::Modified(Box::new(Message::Response(
                                            QueryResponse {
                                                matching_query: Some(original),
                                                original: RawFrame::NONE,
                                                result: None,
                                                error: Some(Value::Strings(format!(
                                                    "{} {}",
                                                    error.code().unwrap_or("ERR").to_string(),
                                                    error
                                                        .detail()
                                                        .unwrap_or("something went wrong?")
                                                        .to_string(),
                                                ))),
                                                response_meta: None,
                                            },
                                        )))),
                                    }
                                }
                            };
                        }
                    }
                }
            }
        }
        return Err(anyhow!(
            "Redis Cluster transform did not have enough information to build a request"
        ));
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod scatter_transform_tests {}
