use std::fmt::Debug;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{FutureExt, SinkExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::protocols::redis_codec::RedisCodec;
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisCodecConfiguration {
    #[serde(rename = "remote_address")]
    pub address: String,
}

#[async_trait]
impl TransformsFromConfig for RedisCodecConfiguration {
    async fn get_source(&self, _: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::RedisCodecDestination(
            RedisCodecDestination::new(self.address.clone()),
        ))
    }
}

#[derive(Debug)]
pub struct RedisCodecDestination {
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
        }
    }
}

#[async_trait]
impl Transform for RedisCodecDestination {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        if self.outbound.is_none() {
            let outbound_stream = TcpStream::connect(self.address.clone()).await.unwrap();
            // TODO: Make this configurable
            self.outbound = Some(Framed::new(outbound_stream, RedisCodec::new(true, 1)));
        }

        // self.outbound is gauranteed to be Some by the previous block
        let outbound_framed_codec = self.outbound.as_mut().unwrap();
        outbound_framed_codec
            .send(message_wrapper.message)
            .await
            .ok();

        match outbound_framed_codec.next().fuse().await {
            Some(a) => a,
            None => Err(anyhow!("couldnt get frame")),
        }
    }

    fn get_name(&self) -> &'static str {
        "RedisCodecDestination"
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
