use std::fmt::Debug;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{FutureExt, SinkExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use shotover_protocols::redis_codec::RedisCodec;
use shotover_transforms::TopicHolder;
use shotover_transforms::{ChainResponse, Messages, Transform, TransformsFromConfig, Wrapper};

use crate::transforms::InternalTransform;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisCodecConfiguration {
    #[serde(rename = "remote_address")]
    pub address: String,
}

#[typetag::serde]
#[async_trait]
impl TransformsFromConfig for RedisCodecConfiguration {
    async fn get_source(&self, _: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
        Ok(Box::new(RedisCodecDestination::new(self.address.clone())))
    }
}

#[derive(Debug)]
pub struct RedisCodecDestination {
    name: &'static str,
    address: String,
    outbound: Option<Framed<TcpStream, RedisCodec>>,
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
            outbound: None,
            name: "CodecDestination",
        }
    }
}

#[async_trait]
impl Transform for RedisCodecDestination {
    async fn transform<'a>(&'a mut self, mut qd: Wrapper<'a>) -> ChainResponse {
        match self.outbound {
            None => {
                let outbound_stream = TcpStream::connect(self.address.clone()).await.unwrap();
                // TODO: Make this configurable
                let mut outbound_framed_codec =
                    Framed::new(outbound_stream, RedisCodec::new(true, 1));
                let _ = outbound_framed_codec.send(qd.message).await;
                if let Some(o) = outbound_framed_codec.next().fuse().await {
                    if let Ok(_resp) = &o {
                        self.outbound.replace(outbound_framed_codec);
                        return o;
                    }
                }
                self.outbound.replace(outbound_framed_codec);
            }
            Some(ref mut outbound_framed_codec) => {
                let _ = outbound_framed_codec.send(qd.message).await;

                let result = outbound_framed_codec
                    .next()
                    .fuse()
                    .await
                    .ok_or_else(|| anyhow!("couldnt get frame"))?;

                return result;
            }
        }
        ChainResponse::Err(anyhow!("Something went wrong sending frame to Redis"))
    }

    fn get_name(&self) -> &'static str {
        self.name
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
