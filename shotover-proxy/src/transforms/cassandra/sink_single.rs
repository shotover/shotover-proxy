use crate::concurrency::FuturesOrdered;
use crate::error::ChainResponse;
use crate::message;
use crate::message::{Message, Messages, QueryResponse};
use crate::protocols::cassandra_codec::CassandraCodec;
use crate::protocols::RawFrame;
use crate::transforms::util::unordered_cluster_connection_pool::OwnedUnorderedConnectionPool;
use crate::transforms::util::Request;
use crate::transforms::{Transform, Transforms, Wrapper};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use cassandra_protocol::frame::Frame;
use metrics::{counter, register_counter};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot::Receiver;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::{info, trace};

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraSinkSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub result_processing: bool,
}

impl CassandraSinkSingleConfig {
    pub async fn get_source(&self, chain_name: String) -> Result<Transforms> {
        Ok(Transforms::CassandraSinkSingle(CassandraSinkSingle::new(
            self.address.clone(),
            self.result_processing,
            chain_name,
        )))
    }
}

#[derive(Debug)]
pub struct CassandraSinkSingle {
    address: String,
    outbound: Option<OwnedUnorderedConnectionPool<CassandraCodec>>,
    cassandra_ks: HashMap<String, Vec<String>>,
    bypass: bool,
    chain_name: String,
}

impl Clone for CassandraSinkSingle {
    fn clone(&self) -> Self {
        CassandraSinkSingle::new(self.address.clone(), self.bypass, self.chain_name.clone())
    }
}

impl CassandraSinkSingle {
    pub fn new(address: String, bypass: bool, chain_name: String) -> CassandraSinkSingle {
        let sink_single = CassandraSinkSingle {
            address,
            outbound: None,
            cassandra_ks: HashMap::new(),
            bypass,
            chain_name: chain_name.clone(),
        };

        register_counter!("failed_requests", "chain" => chain_name, "transform" => sink_single.get_name());

        sink_single
    }

    fn get_name(&self) -> &'static str {
        "CassandraSinkSingle"
    }
}

impl CassandraSinkSingle {
    async fn send_message(&mut self, messages: Messages) -> ChainResponse {
        loop {
            match self.outbound {
                None => {
                    trace!("creating outbound connection {:?}", self.address);
                    let mut conn_pool = OwnedUnorderedConnectionPool::new(
                        self.address.clone(),
                        CassandraCodec::new(self.cassandra_ks.clone(), self.bypass),
                    );
                    // we should either connect and set the value of outbound, or return an error... so we shouldn't loop more than 2 times
                    conn_pool.connect(1).await?;
                    self.outbound = Some(conn_pool);
                }
                Some(ref mut outbound_framed_codec) => {
                    trace!("sending frame upstream");
                    let sender = outbound_framed_codec
                        .connections
                        .get_mut(0)
                        .expect("No connections found");
                    let expected_size = messages.len();
                    let results: Result<FuturesOrdered<Receiver<(Message, ChainResponse)>>> =
                        messages
                            .into_iter()
                            .map(|m| {
                                let (return_chan_tx, return_chan_rx) =
                                    tokio::sync::oneshot::channel();
                                let stream = if let RawFrame::Cassandra(frame) = &m.original {
                                    frame.stream_id
                                } else {
                                    info!("no cassandra frame found");
                                    return Err(anyhow!("no cassandra frame found"));
                                };

                                sender.send(Request {
                                    messages: m,
                                    return_chan: Some(return_chan_tx),
                                    message_id: Some(stream),
                                })?;

                                Ok(return_chan_rx)
                            })
                            .collect();

                    let mut responses = Vec::with_capacity(expected_size);
                    let mut results = results?;

                    loop {
                        match timeout(Duration::from_secs(5), results.next()).await {
                            Ok(Some(prelim)) => {
                                match prelim? {
                                    (_, Ok(mut resp)) => {
                                        for message in &resp {
                                            if let RawFrame::Cassandra(Frame {
                                                opcode: cassandra_protocol::frame::Opcode::Error,
                                                ..
                                            }) = &message.original
                                            {
                                                counter!("failed_requests", 1, "chain" => self.chain_name.clone(), "transform" => self.get_name());
                                            }
                                        }
                                        responses.append(&mut resp);
                                    }
                                    (m, Err(err)) => {
                                        responses.push(Message::new_response(
                                            QueryResponse::empty_with_error(Some(
                                                message::Value::Strings(format!("{err}")),
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

                    return Ok(responses);
                }
            }
        }
    }
}

#[async_trait]
impl Transform for CassandraSinkSingle {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.send_message(message_wrapper.messages).await
    }

    fn is_terminating(&self) -> bool {
        true
    }
}
