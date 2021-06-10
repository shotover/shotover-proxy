use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use shotover_transforms::{Message, Messages, QueryResponse, RawFrame, TopicHolder, Value};
use shotover_transforms::{Transform, TransformsFromConfig, Wrapper};
use std::collections::HashMap;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::{info, trace};

use anyhow::{anyhow, Result};
use shotover_protocols::cassandra_protocol2::CassandraCodec2;
use shotover_transforms::concurrency::FuturesOrdered;
use shotover_transforms::util::unordered_cluster_connection_pool::OwnedUnorderedConnectionPool;
use shotover_transforms::util::Request;
use shotover_transforms::ChainResponse;
use std::time::Duration;
use tokio::sync::oneshot::Receiver;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct CassandraCodecConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub bypass_result_processing: bool,
}

#[typetag::serde]
#[async_trait]
impl TransformsFromConfig for CassandraCodecConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
        Ok(Box::new(CodecDestination::new(
            self.address.clone(),
            self.bypass_result_processing,
        )))
    }
}

#[derive(Debug)]
pub struct CodecDestination {
    name: &'static str,
    address: String,
    // outbound: Arc<Mutex<Option<Framed<TcpStream, CassandraCodec2>>>>,
    outbound: Option<OwnedUnorderedConnectionPool<CassandraCodec2>>,
    cassandra_ks: HashMap<String, Vec<String>>,
    bypass: bool,
}

impl Clone for CodecDestination {
    fn clone(&self) -> Self {
        CodecDestination::new(self.address.clone(), self.bypass)
    }
}

impl CodecDestination {
    pub fn new(address: String, bypass: bool) -> CodecDestination {
        CodecDestination {
            address,
            outbound: None,
            name: "CodecDestination",
            cassandra_ks: HashMap::new(),
            bypass,
        }
    }
}

impl CodecDestination {
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
                                            QueryResponse::empty_with_error(Some(Value::Strings(
                                                format!("{}", err),
                                            ))),
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
impl Transform for CodecDestination {
    // #[instrument]
    async fn transform<'a>(&'a mut self, wrapped_messages: Wrapper<'a>) -> ChainResponse {
        self.send_message(wrapped_messages.message).await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
