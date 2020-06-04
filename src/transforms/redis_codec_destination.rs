use crate::transforms::chain::{ChainResponse, Transform, TransformChain, Wrapper};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::config::topology::TopicHolder;
use crate::config::ConfigError;
use crate::message::{Message, QueryMessage, QueryResponse, RawMessage, Value};
use crate::protocols::redis_codec::RedisCodec;
use crate::protocols::cassandra_protocol2::RawFrame;
use crate::transforms::chain::RequestError;
use crate::transforms::{Transforms, TransformsFromConfig};
use slog::Logger;
use slog::trace;

use tokio::sync::Mutex;
use tokio::stream::StreamExt;

use std::sync::Arc;
use redis_protocol::prelude::Frame;
use futures::{FutureExt, SinkExt};

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
        logger: &Logger,
    ) -> Result<Transforms, ConfigError> {
        Ok(Transforms::RedisCodecDestination(RedisCodecDestination::new(
            self.address.clone(),
            logger,
        )))
    }
}

#[derive(Debug)]
pub struct RedisCodecDestination {
    name: &'static str,
    address: String,
    outbound: Arc<Mutex<Option<Framed<TcpStream, RedisCodec>>>>,
    logger: Logger,
}


impl Clone for RedisCodecDestination {
    fn clone(&self) -> Self {
        RedisCodecDestination::new(self.address.clone(), &self.logger)
    }
}

fn build_response_message(frame: Frame, matching_query: Option<QueryMessage>) -> ChainResponse {
    let result: Option<Value> = None;
    let error: Option<Value> = None;

    //TODO fill out result and error fields from redis Frame

    return Ok(Message::Response(QueryResponse {
        matching_query,
        original: RawFrame::Redis(frame),
        result,
        error
    }))
}

impl RedisCodecDestination {
    pub fn new(address: String, logger: &Logger) -> RedisCodecDestination {
        RedisCodecDestination {
            address,
            outbound: Arc::new(Mutex::new(None)),
            name: "CodecDestination",
            logger: logger.clone(),
        }
    }

    async fn send_frame(
        &self,
        frame: Frame,
        matching_query: Option<QueryMessage>,
    ) -> ChainResponse {
        trace!(self.logger, "      C -> S {:?}", frame.kind());
        if let Ok(mut mg) = self.outbound.try_lock() {
            match *mg {
                None => {
                    let outbound_stream = TcpStream::connect(self.address.clone()).await.unwrap();
                    let mut outbound_framed_codec =
                        Framed::new(outbound_stream, RedisCodec::new(self.logger.clone()));
                    let _ = outbound_framed_codec.send(frame).await;
                    if let Some(o) = outbound_framed_codec.next().fuse().await {
                        if let Ok(resp) = o {
                            trace!(self.logger, "      S -> C {:?}", resp.kind());
                            mg.replace(outbound_framed_codec);
                            drop(mg);
                            return build_response_message(resp, matching_query);
                            //TODO - this codec should return a response/query, but a transform should be responsible
                            // for encriching / creating the meta data about it
                            // that way users can easily just build straight proxies that are not messing with frames as
                            // they transit the chain
                            // return ChainResponse::Ok(Message::Bypass(RawMessage {
                            //     original: RawFrame::CASSANDRA(resp),
                            // }));
                        }
                    }
                    mg.replace(outbound_framed_codec);
                    drop(mg);
                }
                Some(ref mut outbound_framed_codec) => {
                    let _ = outbound_framed_codec.send(frame).await;
                    if let Some(o) = outbound_framed_codec.next().fuse().await {
                        if let Ok(resp) = o {
                            trace!(self.logger, "      S -> C {:?}", resp.kind());
                            return ChainResponse::Ok(Message::Bypass(RawMessage {
                                original: RawFrame::Redis(resp),
                            }));
                        }
                    }
                }
            }
        }
        return ChainResponse::Err(RequestError {});
    }
}

#[async_trait]
impl Transform for RedisCodecDestination {
    async fn transform(&self, qd: Wrapper, _: &TransformChain) -> ChainResponse {
        // let return_query = qd.message.clone();
        match qd.message {
            Message::Bypass(rm) => {
                if let RawFrame::Redis(frame) = rm.original {
                    return self.send_frame(frame, None).await;
                }
            }
            Message::Query(qm) => {
                let return_query = qm.clone();
                if qd.modified {
                    return self
                        .send_frame(
                            RedisCodec::build_redis_query_frame(qm),
                            Some(return_query),
                        )
                        .await;
                }
                if let RawFrame::Redis(frame) = qm.original {
                    return self.send_frame(frame, Some(return_query)).await;
                }
            }
            Message::Response(_) => {}
        }
        return ChainResponse::Err(RequestError {});
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
