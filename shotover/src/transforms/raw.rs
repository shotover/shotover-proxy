use crate::codec::{
    raw::{RawCodecBuilder, RawDecoder, RawEncoder},
    CodecBuilder, Direction,
};
use crate::message::Message;
use crate::tcp;
use crate::tls::{AsyncStream, TlsConnector, TlsConnectorConfig};
use crate::transforms::{
    Messages, Transform, TransformBuilder, TransformConfig, Transforms, Wrapper,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{FutureExt, SinkExt, StreamExt};
use serde::Deserialize;
use std::pin::Pin;
use std::time::Duration;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{trace, Instrument};

type PinStream = Pin<Box<dyn AsyncStream + Send + Sync>>;

#[derive(Deserialize, Debug)]
pub struct RawSinkSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub tls: Option<TlsConnectorConfig>,
    pub connect_timeout_ms: u64,
}

#[typetag::deserialize(name = "RawSinkSingle")]
#[async_trait(?Send)]
impl TransformConfig for RawSinkSingleConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Box::new(RawSinkSingleBuilder::new(
            self.address.clone(),
            tls,
            self.connect_timeout_ms,
        )))
    }
}

#[derive(Clone)]
pub struct RawSinkSingleBuilder {
    address: String,
    tls: Option<TlsConnector>,
    connect_timeout: Duration,
    codec_builder: RawCodecBuilder,
}

impl RawSinkSingleBuilder {
    pub fn new(address: String, tls: Option<TlsConnector>, connect_timeout_ms: u64) -> Self {
        let codec_builder = RawCodecBuilder::new(Direction::Sink);

        Self {
            address,
            tls,
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            codec_builder,
        }
    }
}

impl TransformBuilder for RawSinkSingleBuilder {
    fn build(&self) -> Transforms {
        Transforms::RawSinkSingle(RawSinkSingle {
            connection: None,
            address: self.address.clone(),
            tls: self.tls.clone(),
            connect_timeout: self.connect_timeout,
            codec_builder: self.codec_builder.clone(),
        })
    }

    fn get_name(&self) -> &'static str {
        "RawSinkSingle"
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

pub struct RawSinkSingle {
    address: String,
    connection: Option<Connection>,
    tls: Option<TlsConnector>,
    connect_timeout: Duration,
    codec_builder: RawCodecBuilder,
}

#[async_trait]
impl Transform for RawSinkSingle {
    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        if self.connection.is_none() {
            trace!("creating outbound connection {:?}", self.address);

            let generic_stream = if let Some(tls) = self.tls.as_mut() {
                let tls_stream = tls
                    .connect(self.connect_timeout, self.address.clone())
                    .await?;
                Box::pin(tls_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>
            } else {
                let tcp_stream =
                    tcp::tcp_stream(self.connect_timeout, self.address.clone()).await?;
                Box::pin(tcp_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>
            };

            let (decoder, encoder) = self.codec_builder.build();
            let (stream_rx, stream_tx) = tokio::io::split(generic_stream);
            let outbound_tx = FramedWrite::new(stream_tx, encoder);
            let outbound_rx = FramedRead::new(stream_rx, decoder);
            let (response_messages_tx, response_messages_rx) = mpsc::unbounded_channel();

            tokio::spawn(
                server_response_processing_task(outbound_rx, response_messages_tx)
                    .in_current_span(),
            );
            self.connection = Some(Connection {
                response_messages_rx,
                outbound_tx,
            })
        }

        let connection = self.connection.as_mut().unwrap();

        let messages_len = requests_wrapper.requests.len();

        let mut result = Vec::with_capacity(messages_len);
        for message in requests_wrapper.requests {
            connection
                .outbound_tx
                .send(vec![message])
                .await
                .map_err(|err| anyhow!("Failed to send messages to raw destination: {err:?}"))?;

            let message = connection
                .response_messages_rx
                .recv()
                .await
                .ok_or_else(|| anyhow!("Failed to receive message because RawSinkSingle response processing task is dead"))?;
            result.push(message);
        }

        Ok(result)
    }
}

struct Connection {
    outbound_tx: FramedWrite<WriteHalf<PinStream>, RawEncoder>,
    response_messages_rx: mpsc::UnboundedReceiver<Message>,
}

async fn server_response_processing_task(
    mut outbound_rx: FramedRead<ReadHalf<PinStream>, RawDecoder>,
    response_messages_tx: mpsc::UnboundedSender<Message>,
) {
    loop {
        tokio::select! {
            responses = outbound_rx.next().fuse() => {
                match responses {
                    Some(Ok(messages)) => {
                        for message in messages {
                            if let Err(mpsc::error::SendError(_)) = response_messages_tx.send(message) {
                                // RawSinkSingle dropped after a message was received from server,
                                // request processor task shutting down
                                return;
                            }
                        }
                    }
                    Some(Err(err)) => {
                        tracing::error!("encountered error in RawSinkSingle stream: {err:?}");
                        return;
                    }
                    None => {
                        tracing::debug!("sink stream ended, redis single subscription task shutting down");
                        return;
                    }
                }
            },
            _ = response_messages_tx.closed() => {
                // sink stream ended, shutdown the response processing task
                return;
            },
        }
    }
}
