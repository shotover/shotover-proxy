use crate::codec::{
    opensearch::{OpenSearchCodecBuilder, OpenSearchDecoder, OpenSearchEncoder},
    CodecBuilder, Direction,
};
use crate::message::Message;
use crate::tcp;
use crate::tls::AsyncStream;
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
pub struct OpenSearchSinkConfig {
    #[serde(rename = "remote_address")]
    address: String,
    connect_timeout_ms: u64,
}

#[typetag::deserialize(name = "OpenSearchSink")]
#[async_trait(?Send)]
impl TransformConfig for OpenSearchSinkConfig {
    async fn get_builder(&self, chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(OpenSearchSinkBuilder::new(
            self.address.clone(),
            chain_name,
            self.connect_timeout_ms,
        )))
    }
}

#[derive(Clone)]
pub struct OpenSearchSinkBuilder {
    address: String,
    connect_timeout: Duration,
}

impl OpenSearchSinkBuilder {
    pub fn new(address: String, _chain_name: String, connect_timeout_ms: u64) -> Self {
        let connect_timeout = Duration::from_millis(connect_timeout_ms);

        Self {
            address,
            connect_timeout,
        }
    }
}

impl TransformBuilder for OpenSearchSinkBuilder {
    fn build(&self) -> Transforms {
        Transforms::OpenSearchSink(OpenSearchSink {
            address: self.address.clone(),
            connect_timeout: self.connect_timeout,
            codec_builder: OpenSearchCodecBuilder::new(Direction::Sink),
            connection: None,
        })
    }

    fn get_name(&self) -> &'static str {
        "OpenSearchSink"
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

pub struct OpenSearchSink {
    address: String,
    connection: Option<Connection>,
    connect_timeout: Duration,
    codec_builder: OpenSearchCodecBuilder,
}

#[async_trait]
impl Transform for OpenSearchSink {
    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        // Return immediately if we have no messages.
        // If we tried to send no messages we would block forever waiting for a reply that will never come.
        if requests_wrapper.requests.is_empty() {
            return Ok(requests_wrapper.requests);
        }

        if self.connection.is_none() {
            trace!("creating outbound connection {:?}", self.address);

            let tcp_stream = tcp::tcp_stream(self.connect_timeout, self.address.clone()).await?;
            let stream = Box::pin(tcp_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>;

            let (decoder, encoder) = self.codec_builder.build();
            let (stream_rx, stream_tx) = tokio::io::split(stream);
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
                .map_err(|err| {
                    anyhow!("Failed to send messages to OpenSearch destination: {err:?}")
                })?;

            let message = connection
                .response_messages_rx
                .recv()
                .await
                .ok_or_else(|| anyhow!("Failed to receive message because OpenSearchSink response processing task is dead"))?;
            result.push(message);
        }

        Ok(result)
    }
}

struct Connection {
    outbound_tx: FramedWrite<WriteHalf<PinStream>, OpenSearchEncoder>,
    response_messages_rx: mpsc::UnboundedReceiver<Message>,
}

async fn server_response_processing_task(
    mut outbound_rx: FramedRead<ReadHalf<PinStream>, OpenSearchDecoder>,
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
                        tracing::error!("encountered error in OpenSearchSink stream: {err:?}");
                        return;
                    }
                    None => {
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
