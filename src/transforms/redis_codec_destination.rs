use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::config::topology::TopicHolder;
use crate::message::{Message, QueryMessage};
use crate::protocols::redis_codec::RedisCodec;
use crate::transforms::{Transforms, TransformsFromConfig};
use tracing::trace;

use tokio::sync::Mutex;
use tokio::stream::StreamExt;

use std::sync::Arc;
use futures::{FutureExt, SinkExt};

use crate::error::{ChainResponse};
use anyhow::{anyhow, Result};
use tracing::instrument;



#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisCodecConfiguration {
    #[serde(rename = "remote_address")]
    pub address: String,
}

#[async_trait]
impl TransformsFromConfig for RedisCodecConfiguration {
    async fn get_source(
        &self,
        _: &TopicHolder,
    ) -> Result<Transforms> {
        Ok(Transforms::RedisCodecDestination(RedisCodecDestination::new(
            self.address.clone(),
        )))
    }
}

#[derive(Debug)]
pub struct RedisCodecDestination {
    name: &'static str,
    address: String,
    outbound: Arc<Mutex<Option<Framed<TcpStream, RedisCodec>>>>,
}


impl Clone for RedisCodecDestination {
    fn clone(&self) -> Self {
        RedisCodecDestination::new(self.address.clone())
    }
}

impl RedisCodecDestination {
    pub fn new(address: String) -> RedisCodecDestination {
        RedisCodecDestination {
            address,
            outbound: Arc::new(Mutex::new(None)),
            name: "CodecDestination",
        }
    }

    async fn send_message(
        &self,
        message: Message,
        _matching_query: Option<QueryMessage>,
    ) -> ChainResponse {
        trace!("      C -> S {:?}", message);
        if let Ok(mut mg) = self.outbound.try_lock() {
            match *mg {
                None => {
                    let outbound_stream = TcpStream::connect(self.address.clone()).await.unwrap();
                    let mut outbound_framed_codec =
                        Framed::new(outbound_stream, RedisCodec::new());
                    let _ = outbound_framed_codec.send(message).await;
                    if let Some(o) = outbound_framed_codec.next().fuse().await {
                        if let Ok(resp) = &o {
                            trace!("      S -> C {:?}", resp);
                            mg.replace(outbound_framed_codec);
                            drop(mg);
                            return o;
                        }
                    }
                    mg.replace(outbound_framed_codec);
                    drop(mg);
                }
                Some(ref mut outbound_framed_codec) => {
                    let _ = outbound_framed_codec.send(message).await;
                    return outbound_framed_codec.next().fuse().await.ok_or(anyhow!("couldnt get frame"))?;
                }
            }
        }
        return ChainResponse::Err(anyhow!("Something went wrong sending frame to Redis"));
    }

}

#[async_trait]
impl Transform for RedisCodecDestination {
    #[instrument]
    async fn transform(&self, qd: Wrapper, _: &TransformChain) -> ChainResponse {
        let return_query = match &qd.message {
            Message::Query(q) => {Some(q.clone())},
            _ => {None},
        };
        self.send_message(qd.message, return_query).await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
