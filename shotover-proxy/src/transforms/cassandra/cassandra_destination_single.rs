use async_trait::async_trait;
use serde::Deserialize;

use crate::config::topology::TopicHolder;
use crate::message::{Message, Messages, QueryResponse};
use crate::protocols::cassandra_protocol2::CassandraCodec2;
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};
use std::collections::HashMap;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::{info, trace};

use crate::concurrency::FuturesOrdered;
use crate::error::ChainResponse;
use crate::message;
use crate::protocols::RawFrame;
use crate::transforms::util::unordered_cluster_connection_pool::OwnedUnorderedConnectionPool;
use crate::transforms::util::Request;
use anyhow::{anyhow, Result};
use std::time::Duration;
use tokio::sync::oneshot::Receiver;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraDestinationSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub bypass_result_processing: bool,
}

#[async_trait]
impl TransformsFromConfig for CassandraDestinationSingleConfig {
    async fn get_source(&self, _: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::CassandraDestinationSingle(
            CassandraDestinationSingle::new(self.address.clone(), self.bypass_result_processing),
        ))
    }
}

#[derive(Debug)]
pub struct CassandraDestinationSingle {
    address: String,
    outbound: Option<OwnedUnorderedConnectionPool<CassandraCodec2>>,
    cassandra_ks: HashMap<String, Vec<String>>,
    bypass: bool,
}

impl Clone for CassandraDestinationSingle {
    fn clone(&self) -> Self {
        CassandraDestinationSingle::new(self.address.clone(), self.bypass)
    }
}

impl CassandraDestinationSingle {
    pub fn new(address: String, bypass: bool) -> CassandraDestinationSingle {
        CassandraDestinationSingle {
            address,
            outbound: None,
            cassandra_ks: HashMap::new(),
            bypass,
        }
    }
}

impl CassandraDestinationSingle {
    async fn send_message(&mut self, messages: Messages) -> ChainResponse {
        loop {
            match self.outbound {
                None => {
                    trace!("creating outbound connection {:?}", self.address);
                    let mut conn_pool = OwnedUnorderedConnectionPool::new(
                        self.address.clone(),
                        CassandraCodec2::new(self.cassandra_ks.clone(), self.bypass),
                    );
                    // we should either connect and set the value of outbound, or return an error... so we shouldn't loop more than 2 times
                    conn_pool.connect(1).await?;
                    self.outbound.replace(conn_pool);
                }
                Some(ref mut outbound_framed_codec) => {
                    trace!("sending frame upstream");
                    let sender = outbound_framed_codec
                        .connections
                        .get_mut(0)
                        .expect("No connections found");
                    let expected_size = messages.messages.len();
                    let results: Result<FuturesOrdered<Receiver<(Message, ChainResponse)>>> =
                        messages
                            .into_iter()
                            .map(|m| {
                                let (return_chan_tx, return_chan_rx) =
                                    tokio::sync::oneshot::channel();
                                let stream;
                                if let RawFrame::Cassandra(frame) = &m.original {
                                    stream = frame.stream;
                                } else {
                                    info!("no cassandra frame found");
                                    return Err(anyhow!("no cassandra frame found"));
                                }

                                let req: Request = Request {
                                    messages: m,
                                    return_chan: Some(return_chan_tx),
                                    message_id: Some(stream),
                                };
                                sender.send(req)?;

                                Ok(return_chan_rx)
                            })
                            .collect();

                    let mut responses = Vec::with_capacity(expected_size);
                    let mut results = results?;

                    loop {
                        match timeout(Duration::from_secs(5), results.next()).await {
                            Ok(Some(prelim)) => {
                                match prelim? {
                                    (_, Ok(mut resp)) => responses.append(&mut resp.messages),
                                    (m, Err(err)) => {
                                        responses.push(Message::new_response(
                                            QueryResponse::empty_with_error(Some(
                                                message::Value::Strings(format!("{}", err)),
                                            )),
                                            true,
                                            m.original,
                                        ));
                                    }
                                };
                            }
                            Ok(None) => break,
                            Err(_) => {
                                info!(
                                    "timed out waiting for results got - {:?} expected - {:?}",
                                    responses.len(),
                                    expected_size
                                );
                                info!(
                                    "timed out waiting for results - {:?} - {:?}",
                                    responses, results
                                );
                            }
                        }
                    }

                    return Ok(Messages {
                        messages: responses,
                    });
                }
            }
        }
    }
}

#[async_trait]
impl Transform for CassandraDestinationSingle {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.send_message(message_wrapper.message).await
    }

    fn get_name(&self) -> &'static str {
        "CassandraDestinationSingle"
    }
}
