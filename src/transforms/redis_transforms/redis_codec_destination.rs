use std::borrow::Borrow;
use std::fmt::Debug;
use std::num::Wrapping;
use std::ops::Sub;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{FutureExt, SinkExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::stream::{Stream, StreamExt};
use tokio_util::codec::Framed;
use tracing::debug;
use tracing::warn;

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::Messages;
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
    name: &'static str,
    address: String,
    outbound: Option<Framed<TcpStream, RedisCodec>>,
    last_processed_clock: Wrapping<u32>,
}

impl Clone for RedisCodecDestination {
    fn clone(&self) -> Self {
        RedisCodecDestination::new(self.address.clone())
    }
}

// pub(crate) async fn maybe_fastforward<T, I>(
//     address: &str,
//     outbound_framed_codec: &mut T,
//     current_clock: Wrapping<u32>,
//     last_processed_clock: &mut Wrapping<u32>,
// ) where
//     I: Debug,
//     T: StreamExt + Stream<Item = I> + Unpin,
// {
//     while current_clock.sub(last_processed_clock.borrow()) > Wrapping(1) {
//         let r = outbound_framed_codec.next().await;
//         debug!("{} Discarding frame {:?}", address, r);
//         *last_processed_clock += Wrapping(1);
//     }
// }

impl RedisCodecDestination {
    pub fn new(address: String) -> RedisCodecDestination {
        RedisCodecDestination {
            address,
            outbound: None,
            name: "CodecDestination",
            last_processed_clock: Wrapping(0),
        }
    }

    async fn send_message(
        &mut self,
        message: Messages,
        // message_clock: Wrapping<u32>,
    ) -> ChainResponse {
        match self.outbound {
            None => {
                let outbound_stream = TcpStream::connect(self.address.clone()).await.unwrap();
                // TODO: Make this configurable
                let mut outbound_framed_codec =
                    Framed::new(outbound_stream, RedisCodec::new(true, 1));
                let _ = outbound_framed_codec.send(message).await;
                if let Some(o) = outbound_framed_codec.next().fuse().await {
                    if let Ok(resp) = &o {
                        self.outbound.replace(outbound_framed_codec);
                        return o;
                    }
                }
                self.outbound.replace(outbound_framed_codec);
            }
            Some(ref mut outbound_framed_codec) => {
                let _ = outbound_framed_codec.send(message).await;

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
}

#[async_trait]
impl Transform for RedisCodecDestination {
    // #[instrument]
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        let r = self.send_message(qd.message).await;
        r
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod test {
    use std::num::Wrapping;

    use anyhow::Result;
    use futures::stream::{self};
    use tokio::stream::StreamExt;

    // #[tokio::test(threaded_scheduler)]
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
