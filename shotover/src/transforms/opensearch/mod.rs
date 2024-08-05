use super::{DownChainProtocol, TransformContextBuilder, TransformContextConfig, UpChainProtocol};
use crate::frame::MessageType;
use crate::tcp;
use crate::transforms::{Messages, Transform, TransformBuilder, TransformConfig, Wrapper};
use crate::{
    codec::{opensearch::OpenSearchCodecBuilder, CodecBuilder, Direction},
    transforms::util::{
        cluster_connection_pool::{spawn_read_write_tasks, Connection},
        Request,
    },
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::trace;

#[derive(Serialize, Deserialize, Debug)]
pub struct OpenSearchSinkSingleConfig {
    #[serde(rename = "remote_address")]
    address: String,
    connect_timeout_ms: u64,
}

const NAME: &str = "OpenSearchSinkSingle";
#[typetag::serde(name = "OpenSearchSinkSingle")]
#[async_trait(?Send)]
impl TransformConfig for OpenSearchSinkSingleConfig {
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(OpenSearchSinkSingleBuilder::new(
            self.address.clone(),
            transform_context.chain_name,
            self.connect_timeout_ms,
        )))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::MustBeOneOf(vec![MessageType::OpenSearch])
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::Terminating
    }
}

pub struct OpenSearchSinkSingleBuilder {
    address: String,
    connect_timeout: Duration,
}

impl OpenSearchSinkSingleBuilder {
    pub fn new(address: String, _chain_name: String, connect_timeout_ms: u64) -> Self {
        let connect_timeout = Duration::from_millis(connect_timeout_ms);

        Self {
            address,
            connect_timeout,
        }
    }
}

impl TransformBuilder for OpenSearchSinkSingleBuilder {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(OpenSearchSinkSingle {
            address: self.address.clone(),
            connect_timeout: self.connect_timeout,
            codec_builder: OpenSearchCodecBuilder::new(Direction::Sink, self.get_name().to_owned()),
            connection: None,
        })
    }

    fn get_name(&self) -> &'static str {
        NAME
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

pub struct OpenSearchSinkSingle {
    address: String,
    connection: Option<Connection>,
    connect_timeout: Duration,
    codec_builder: OpenSearchCodecBuilder,
}

#[async_trait]
impl Transform for OpenSearchSinkSingle {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(
        &'a mut self,
        requests_wrapper: &'a mut Wrapper<'a>,
    ) -> Result<Messages> {
        // Return immediately if we have no messages.
        // If we tried to send no messages we would block forever waiting for a reply that will never come.
        if requests_wrapper.requests.is_empty() {
            return Ok(vec![]);
        }

        if self.connection.is_none() {
            trace!("creating outbound connection {:?}", self.address);

            let tcp_stream = tcp::tcp_stream(self.connect_timeout, self.address.clone()).await?;
            let (rx, tx) = tcp_stream.into_split();
            self.connection = Some(spawn_read_write_tasks(&self.codec_builder, rx, tx));
        }

        let connection = self.connection.as_mut().unwrap();

        let messages_len = requests_wrapper.requests.len();

        let mut result = Vec::with_capacity(messages_len);
        for message in requests_wrapper.requests.drain(..) {
            let (tx, rx) = oneshot::channel();

            connection
                .send(Request {
                    message,
                    return_chan: Some(tx),
                })
                .map_err(|_| anyhow!("Failed to send"))?;

            let message = rx.await?.response?;
            result.push(message);
        }

        Ok(result)
    }
}
