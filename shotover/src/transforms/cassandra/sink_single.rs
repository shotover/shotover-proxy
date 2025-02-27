use crate::codec::{CodecBuilder, Direction, cassandra::CassandraCodecBuilder};
use crate::connection::SinkConnection;
use crate::frame::MessageType;
use crate::frame::cassandra::CassandraMetadata;
use crate::message::{Messages, Metadata};
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::{
    ChainState, DownChainProtocol, Transform, TransformBuilder, TransformConfig,
    TransformContextBuilder, TransformContextConfig, UpChainProtocol,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use cassandra_protocol::frame::{Opcode, Version};
use metrics::{Counter, counter};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::timeout;
use tracing::trace;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct CassandraSinkSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub tls: Option<TlsConnectorConfig>,
    pub connect_timeout_ms: u64,
    pub read_timeout: Option<u64>,
}

const NAME: &str = "CassandraSinkSingle";
#[typetag::serde(name = "CassandraSinkSingle")]
#[async_trait(?Send)]
impl TransformConfig for CassandraSinkSingleConfig {
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        let tls = self.tls.as_ref().map(TlsConnector::new).transpose()?;
        Ok(Box::new(CassandraSinkSingleBuilder::new(
            self.address.clone(),
            transform_context.chain_name,
            tls,
            self.connect_timeout_ms,
            self.read_timeout,
        )))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::MustBeOneOf(vec![MessageType::Cassandra])
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::Terminating
    }
}

struct CassandraSinkSingleBuilder {
    version: Option<Version>,
    address: String,
    failed_requests: Counter,
    tls: Option<TlsConnector>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    codec_builder: CassandraCodecBuilder,
}

impl CassandraSinkSingleBuilder {
    fn new(
        address: String,
        chain_name: String,
        tls: Option<TlsConnector>,
        connect_timeout_ms: u64,
        timeout: Option<u64>,
    ) -> CassandraSinkSingleBuilder {
        let failed_requests = counter!("shotover_failed_requests_count", "chain" => chain_name, "transform" => "CassandraSinkSingle");
        let receive_timeout = timeout.map(Duration::from_secs);
        let codec_builder =
            CassandraCodecBuilder::new(Direction::Sink, "CassandraSinkSingle".to_owned());

        CassandraSinkSingleBuilder {
            version: None,
            address,
            failed_requests,
            tls,
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            read_timeout: receive_timeout,
            codec_builder,
        }
    }
}

impl TransformBuilder for CassandraSinkSingleBuilder {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(CassandraSinkSingle {
            connection: None,
            version: self.version,
            address: self.address.clone(),
            tls: self.tls.clone(),
            failed_requests: self.failed_requests.clone(),
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
            codec_builder: self.codec_builder.clone(),
            force_run_chain: transform_context.force_run_chain,
        })
    }

    fn get_name(&self) -> &'static str {
        NAME
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

struct CassandraSinkSingle {
    version: Option<Version>,
    address: String,
    connection: Option<SinkConnection>,
    failed_requests: Counter,
    tls: Option<TlsConnector>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    codec_builder: CassandraCodecBuilder,
    force_run_chain: Arc<Notify>,
}

impl CassandraSinkSingle {
    async fn send_message(&mut self, requests: Messages) -> Result<Messages> {
        if self.version.is_none() {
            if let Some(message) = requests.first() {
                if let Ok(Metadata::Cassandra(CassandraMetadata { version, .. })) =
                    message.metadata()
                {
                    self.version = Some(version);
                } else {
                    return Err(anyhow!(
                        "Failed to extract cassandra version from incoming message: Not a valid cassandra message"
                    ));
                }
            } else {
                // It's an invariant that self.version is Some.
                // Since we were unable to set it, we need to return immediately.
                // This is ok because if there are no messages then we have no work to do anyway.
                return Ok(vec![]);
            }
        }

        if self.connection.is_none() {
            trace!("creating outbound connection {:?}", self.address);
            self.connection = Some(
                SinkConnection::new(
                    self.address.clone(),
                    self.codec_builder.clone(),
                    &self.tls,
                    self.connect_timeout,
                    self.force_run_chain.clone(),
                    self.read_timeout,
                )
                .await?,
            );
        }

        let mut responses = vec![];
        if requests.is_empty() {
            // there are no requests, so no point sending any, but we should check for any responses without awaiting
            self.connection
                .as_mut()
                .unwrap()
                .try_recv_into(&mut responses)?;
        } else {
            let connection = self.connection.as_mut().unwrap();

            let requests_count = requests.len();
            connection.send(requests)?;

            let mut responses_count = 0;
            while responses_count < requests_count {
                let responses_len_old = responses.len();
                if let Some(read_timeout) = self.read_timeout {
                    timeout(read_timeout, connection.recv_into(&mut responses)).await?
                } else {
                    connection.recv_into(&mut responses).await
                }?;
                for response in &mut responses[responses_len_old..] {
                    if response.request_id().is_some() {
                        responses_count += 1;
                    }
                }
            }
        };

        for response in &responses {
            if let Ok(Metadata::Cassandra(CassandraMetadata {
                opcode: Opcode::Error,
                ..
            })) = response.metadata()
            {
                self.failed_requests.increment(1);
            }
        }

        Ok(responses)
    }
}

#[async_trait]
impl Transform for CassandraSinkSingle {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        chain_state: &'shorter mut ChainState<'longer>,
    ) -> Result<Messages> {
        self.send_message(std::mem::take(&mut chain_state.requests))
            .await
    }
}
