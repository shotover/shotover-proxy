use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::config::topology::TopicHolder;
use crate::message::{Messages, QueryMessage};
use crate::protocols::cassandra_protocol2::CassandraCodec2;
use crate::transforms::{Transforms, TransformsFromConfig};
use futures::{FutureExt, SinkExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::stream::StreamExt;
use tokio::sync::Mutex;
use tracing::trace;

use crate::error::ChainResponse;
use anyhow::{anyhow, Result};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct CodecConfiguration {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub bypass_result_processing: bool,
}

#[async_trait]
impl TransformsFromConfig for CodecConfiguration {
    async fn get_source(&self, _: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::CodecDestination(CodecDestination::new(
            self.address.clone(),
            self.bypass_result_processing,
        )))
    }
}

#[derive(Debug)]
pub struct CodecDestination {
    name: &'static str,
    address: String,
    outbound: Arc<Mutex<Option<Framed<TcpStream, CassandraCodec2>>>>,
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
            outbound: Arc::new(Mutex::new(None)),
            name: "CodecDestination",
            cassandra_ks: HashMap::new(),
            bypass,
        }
    }
}

/*
TODO:
it may be worthwhile putting the inbound and outbound tcp streams behind a
multi-consumer, single producer threadsafe queue
*/

impl CodecDestination {
    async fn send_message(
        &self,
        message: Messages,
        _matching_query: Option<QueryMessage>,
    ) -> ChainResponse {
        trace!("waiting for mutex lock");
        if let Ok(mut mg) = self.outbound.try_lock() {
            trace!("got mutex lock");
            match *mg {
                None => {
                    trace!("creating outbound connection {:?}", self.address);
                    let outbound_stream = TcpStream::connect(self.address.clone()).await.unwrap();
                    // outbound_stream.set_nodelay(true);
                    // outbound_stream.set_send_buffer_size(15*1000);
                    let mut outbound_framed_codec = Framed::new(
                        outbound_stream,
                        CassandraCodec2::new(self.cassandra_ks.clone(), self.bypass),
                    );
                    trace!("sending frame upstream");
                    let _ = outbound_framed_codec.send(message).await;
                    trace!("frame sent");

                    trace!("getting response");
                    if let Some(o) = outbound_framed_codec.next().fuse().await {
                        if let Ok(_resp) = &o {
                            trace!("resp received");
                            mg.replace(outbound_framed_codec);
                            drop(mg);
                            return o;
                        }
                    }
                    mg.replace(outbound_framed_codec);
                    drop(mg);
                }
                Some(ref mut outbound_framed_codec) => {
                    trace!("sending frame upstream");
                    let _ = outbound_framed_codec.send(message).await;
                    trace!("frame sent");
                    trace!("getting response");
                    let _rv = outbound_framed_codec
                        .next()
                        .fuse()
                        .await
                        .ok_or_else(|| anyhow!("couldnt get frame"))?;
                    trace!("resp received");
                }
            }
        }
        ChainResponse::Err(anyhow!("Something went wrong sending to cassandra"))
    }
}

#[async_trait]
impl Transform for CodecDestination {
    // #[instrument]
    async fn transform(&self, qd: Wrapper, _: &TransformChain) -> ChainResponse {
        let return_query = match &qd.message {
            Messages::Query(q) => Some(q.clone()),
            _ => None,
        };
        self.send_message(qd.message, return_query).await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
