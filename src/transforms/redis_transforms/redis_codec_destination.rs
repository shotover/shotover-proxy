use std::borrow::Borrow;
use std::fmt::Debug;
use std::num::Wrapping;
use std::ops::Sub;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{FutureExt, SinkExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::stream::{Stream, StreamExt};
use tokio::sync::Mutex;
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
    outbound: Arc<Mutex<Option<(Framed<TcpStream, RedisCodec>, Wrapping<u32>)>>>,
    // last_processed_clock: Arc<Wrapping<u32>>,
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
            // last_processed_clock: Arc::new(Wrapping(0)),
        }
    }

    async fn maybe_fastforward<T, I>(
        &self,
        outbound_framed_codec: &mut T,
        current_clock: Wrapping<u32>,
        last_processed_clock: &mut Wrapping<u32>,
    ) where
        I: Debug,
        T: StreamExt + Stream<Item = I> + Unpin,
    {
        while current_clock.sub(last_processed_clock.borrow()) > Wrapping(1) {
            let r = outbound_framed_codec.next().await;
            debug!("{} Discarding frame {:?}", self.address, r);
            *last_processed_clock += Wrapping(1);
        }
    }

    async fn send_message(&self, message: Messages, message_clock: Wrapping<u32>) -> ChainResponse {
        if let Ok(mut mg) = self.outbound.try_lock() {
            match *mg {
                None => {
                    let outbound_stream = TcpStream::connect(self.address.clone()).await.unwrap();
                    // TODO: Make this configurable
                    let mut outbound_framed_codec =
                        Framed::new(outbound_stream, RedisCodec::new(true, 1));
                    let _ = outbound_framed_codec.send(message).await;
                    if let Some(o) = outbound_framed_codec.next().fuse().await {
                        if let Ok(_resp) = &o {
                            mg.replace((outbound_framed_codec, message_clock));
                            drop(mg);
                            return o;
                        }
                    }
                    mg.replace((outbound_framed_codec, Wrapping(0)));
                    drop(mg);
                }
                Some((ref mut outbound_framed_codec, ref mut last_processed_clock)) => {
                    let _ = outbound_framed_codec.send(message).await;

                    debug!(
                        "{} Redis message clock: current {} - last processed {}",
                        self.address, message_clock, last_processed_clock.0
                    );

                    self.maybe_fastforward(
                        outbound_framed_codec,
                        message_clock,
                        last_processed_clock,
                    )
                    .await;

                    if message_clock.0 < last_processed_clock.0 {
                        warn!(
                            "{} Out of order detected current {}, last processed {}",
                            self.address, message_clock, last_processed_clock
                        )
                    }

                    let result = outbound_framed_codec
                        .next()
                        .fuse()
                        .await
                        .ok_or_else(|| anyhow!("couldnt get frame"))?;

                    last_processed_clock.0 += 1;

                    debug!(
                        "Post answer - {} Redis message clock: current {} - last processed {}",
                        self.address, message_clock, last_processed_clock.0
                    );

                    return result;
                }
            }
        }
        ChainResponse::Err(anyhow!("Something went wrong sending frame to Redis"))
    }
}

#[async_trait]
impl Transform for RedisCodecDestination {
    // #[instrument]
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        let r = self.send_message(qd.message, qd.clock).await;
        r
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod test {
    use std::num::Wrapping;
    use std::sync::Arc;

    use anyhow::Result;
    use futures::stream::{self};
    use tokio::stream::StreamExt;
    use tokio::sync::Mutex;

    use crate::transforms::redis_transforms::redis_codec_destination::RedisCodecDestination;

    #[tokio::test(threaded_scheduler)]
    pub async fn test_clock_wrap() -> Result<()> {
        let codec = RedisCodecDestination {
            name: "",
            address: "".to_string(),
            outbound: Arc::new(Mutex::new(None)),
        };

        let mut stream = stream::iter(1..=10);

        let _ = codec
            .maybe_fastforward::<_, i32>(&mut stream, Wrapping(u32::MIN), &mut Wrapping(u32::MAX))
            .await;

        assert_eq!(stream.next().await, Some(1));

        let mut stream = stream::iter(1..=10);

        let _ = codec
            .maybe_fastforward::<_, i32>(&mut stream, Wrapping(1), &mut Wrapping(u32::MAX))
            .await;

        assert_eq!(stream.next().await, Some(2));

        let mut stream = stream::iter(1..=10);

        let _ = codec
            .maybe_fastforward::<_, i32>(&mut stream, Wrapping(1), &mut Wrapping(u32::MIN))
            .await;

        assert_eq!(stream.next().await, Some(1));

        let mut stream = stream::iter(1..=10);

        let _ = codec
            .maybe_fastforward::<_, i32>(&mut stream, Wrapping(2), &mut Wrapping(u32::MIN))
            .await;

        assert_eq!(stream.next().await, Some(2));

        Ok(())
    }
}
