use std::fmt::Debug;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{FutureExt, SinkExt};
use serde::Deserialize;
use std::pin::Pin;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use crate::error::ChainResponse;
use crate::protocols::redis_codec::{DecodeType, RedisCodec};
use crate::tls::{AsyncStream, TlsConfig, TlsConnector};
use crate::transforms::{Transform, Transforms, Wrapper};

#[derive(Deserialize, Debug, Clone)]
pub struct RedisSinkSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub tls: Option<TlsConfig>,
}

impl RedisSinkSingleConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Transforms::RedisSinkSingle(RedisSinkSingle::new(
            self.address.clone(),
            tls,
        )))
    }
}

pub struct RedisSinkSingle {
    address: String,
    tls: Option<TlsConnector>,
    outbound: Option<Framed<Pin<Box<dyn AsyncStream + Send + Sync>>, RedisCodec>>,
}

impl Clone for RedisSinkSingle {
    fn clone(&self) -> Self {
        RedisSinkSingle::new(self.address.clone(), self.tls.clone())
    }
}

impl RedisSinkSingle {
    pub fn new(address: String, tls: Option<TlsConnector>) -> RedisSinkSingle {
        RedisSinkSingle {
            address,
            tls,
            outbound: None,
        }
    }
}

#[async_trait]
impl Transform for RedisSinkSingle {
    fn is_terminating(&self) -> bool {
        true
    }

    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        if self.outbound.is_none() {
            let tcp_stream = TcpStream::connect(self.address.clone()).await.unwrap();
            let generic_stream = if let Some(tls) = self.tls.as_mut() {
                let tls_stream = tls.connect(tcp_stream).await.unwrap();
                Box::pin(tls_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>
            } else {
                Box::pin(tcp_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>
            };
            self.outbound = Some(Framed::new(
                generic_stream,
                RedisCodec::new(DecodeType::Response),
            ));
        }

        // self.outbound is gauranteed to be Some by the previous block
        let outbound_framed_codec = self.outbound.as_mut().unwrap();
        outbound_framed_codec
            .send(message_wrapper.messages)
            .await
            .ok();

        match outbound_framed_codec.next().fuse().await {
            Some(a) => a,
            None => Err(anyhow!("couldnt get frame")),
        }
    }
}

#[cfg(test)]
mod test {
    // #[tokio::test(flavor = "multi_thread")]
    // pub async fn test_clock_wrap() -> Result<()> {
    //     let address = "".to_string();
    //
    //     let mut stream = stream::iter(1..=10);
    //
    //     let _ = maybe_fastforward::<_, i32>(
    //         &address,
    //         &mut stream,
    //         Wrapping(u32::MIN),
    //         &mut Wrapping(u32::MAX),
    //     )
    //     .await;
    //
    //     assert_eq!(stream.next().await, Some(1));
    //
    //     let mut stream = stream::iter(1..=10);
    //
    //     let _ = maybe_fastforward::<_, i32>(
    //         &address,
    //         &mut stream,
    //         Wrapping(1),
    //         &mut Wrapping(u32::MAX),
    //     )
    //     .await;
    //
    //     assert_eq!(stream.next().await, Some(2));
    //
    //     let mut stream = stream::iter(1..=10);
    //
    //     let _ = maybe_fastforward::<_, i32>(
    //         &address,
    //         &mut stream,
    //         Wrapping(1),
    //         &mut Wrapping(u32::MIN),
    //     )
    //     .await;
    //
    //     assert_eq!(stream.next().await, Some(1));
    //
    //     let mut stream = stream::iter(1..=10);
    //
    //     let _ = maybe_fastforward::<_, i32>(
    //         &address,
    //         &mut stream,
    //         Wrapping(2),
    //         &mut Wrapping(u32::MIN),
    //     )
    //     .await;
    //
    //     assert_eq!(stream.next().await, Some(2));
    //
    //     Ok(())
    // }
}
