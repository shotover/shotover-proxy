use crate::codec::redis::RedisCodec;
use crate::error::ChainResponse;
use crate::frame::Frame;
use crate::frame::RedisFrame;
use crate::tls::{AsyncStream, TlsConfig, TlsConnector};
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{FutureExt, SinkExt};
use metrics::{register_counter, Counter};
use serde::Deserialize;
use std::fmt::Debug;
use std::pin::Pin;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

#[derive(Deserialize, Debug, Clone)]
pub struct RedisSinkSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub tls: Option<TlsConfig>,
}

impl RedisSinkSingleConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<Transforms> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Transforms::RedisSinkSingle(RedisSinkSingle::new(
            self.address.clone(),
            tls,
            chain_name,
        )))
    }
}

pub struct RedisSinkSingle {
    address: String,
    tls: Option<TlsConnector>,
    outbound: Option<Framed<Pin<Box<dyn AsyncStream + Send + Sync>>, RedisCodec>>,
    chain_name: String,
    failed_requests: Counter,
}

impl Clone for RedisSinkSingle {
    fn clone(&self) -> Self {
        RedisSinkSingle {
            address: self.address.clone(),
            tls: self.tls.clone(),
            outbound: None,
            chain_name: self.chain_name.clone(),
            failed_requests: self.failed_requests.clone(),
        }
    }
}

impl RedisSinkSingle {
    pub fn new(address: String, tls: Option<TlsConnector>, chain_name: String) -> RedisSinkSingle {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name.clone(), "transform" => "RedisSinkSingle");

        RedisSinkSingle {
            address,
            tls,
            outbound: None,
            chain_name,
            failed_requests,
        }
    }
}

#[async_trait]
impl Transform for RedisSinkSingle {
    fn is_terminating(&self) -> bool {
        true
    }

    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        // Return immediately if we have no messages.
        // If we tried to send no messages we would block forever waiting for a reply that will will never come.
        if message_wrapper.messages.is_empty() {
            return Ok(message_wrapper.messages);
        }

        if self.outbound.is_none() {
            let tcp_stream = TcpStream::connect(self.address.clone()).await?;
            let generic_stream = if let Some(tls) = self.tls.as_mut() {
                let tls_stream = tls.connect_unverified_hostname(tcp_stream).await?;
                Box::pin(tls_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>
            } else {
                Box::pin(tcp_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>
            };
            self.outbound = Some(Framed::new(generic_stream, RedisCodec::new()));
        }

        // self.outbound is gauranteed to be Some by the previous block
        let outbound_framed_codec = self.outbound.as_mut().unwrap();
        outbound_framed_codec
            .send(message_wrapper.messages)
            .await
            .ok();

        match outbound_framed_codec.next().fuse().await {
            Some(mut a) => {
                if let Ok(messages) = &mut a {
                    for message in messages {
                        if let Some(Frame::Redis(RedisFrame::Error(_))) = message.frame() {
                            self.failed_requests.increment(1);
                        }
                    }
                }
                a
            }
            None => Err(anyhow!("couldnt get frame")),
        }
    }
}
