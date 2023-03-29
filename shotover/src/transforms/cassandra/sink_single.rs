use super::connection::CassandraConnection;
use crate::codec::{cassandra::CassandraCodecBuilder, CodecBuilder, Direction};
use crate::error::ChainResponse;
use crate::frame::cassandra::CassandraMetadata;
use crate::message::{Messages, Metadata};
use crate::tlsls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::cassandra::connection::Response;
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Transforms, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use cassandra_protocol::frame::Version;
use futures::stream::FuturesOrdered;
use metrics::{register_counter, Counter};
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::trace;

#[derive(Deserialize, Debug)]
pub struct CassandraSinkSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub tls: Option<TlsConnectorConfig>,
    pub connect_timeout_ms: u64,
    pub read_timeout: Option<u64>,
}

#[typetag::deserialize(name = "CassandraSinkSingle")]
#[async_trait(?Send)]
impl TransformConfig for CassandraSinkSingleConfig {
    async fn get_builder(&self, chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Box::new(CassandraSinkSingleBuilder::new(
            self.address.clone(),
            chain_name,
            tls,
            self.connect_timeout_ms,
            self.read_timeout,
        )))
    }
}

#[derive(Clone)]
pub struct CassandraSinkSingleBuilder {
    version: Option<Version>,
    address: String,
    failed_requests: Counter,
    tls: Option<TlsConnector>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
}

impl CassandraSinkSingleBuilder {
    pub fn new(
        address: String,
        chain_name: String,
        tls: Option<TlsConnector>,
        connect_timeout_ms: u64,
        timeout: Option<u64>,
    ) -> CassandraSinkSingleBuilder {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name, "transform" => "CassandraSinkSingle");
        let receive_timeout = timeout.map(Duration::from_secs);

        CassandraSinkSingleBuilder {
            version: None,
            address,
            failed_requests,
            tls,
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            read_timeout: receive_timeout,
        }
    }
}

impl TransformBuilder for CassandraSinkSingleBuilder {
    fn build(&self) -> Transforms {
        Transforms::CassandraSinkSingle(CassandraSinkSingle {
            outbound: None,
            version: self.version,
            address: self.address.clone(),
            tls: self.tls.clone(),
            failed_requests: self.failed_requests.clone(),
            pushed_messages_tx: None,
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
        })
    }

    fn get_name(&self) -> &'static str {
        "CassandraSinkSingle"
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

pub struct CassandraSinkSingle {
    version: Option<Version>,
    address: String,
    outbound: Option<CassandraConnection>,
    failed_requests: Counter,
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
}

impl CassandraSinkSingle {
    async fn send_message(&mut self, messages: Messages) -> ChainResponse {
        if self.version.is_none() {
            if let Some(message) = messages.first() {
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

        if self.outbound.is_none() {
            trace!("creating outbound connection {:?}", self.address);
            self.outbound = Some(
                CassandraConnection::new(
                    self.connect_timeout,
                    self.address.clone(),
                    CassandraCodecBuilder::new(Direction::Sink),
                    self.tls.clone(),
                    self.pushed_messages_tx.clone(),
                )
                .await?,
            );
        }
        trace!("sending frame upstream");

        let outbound = self.outbound.as_mut().unwrap();
        let responses_future: Result<FuturesOrdered<oneshot::Receiver<Response>>> =
            messages.into_iter().map(|m| outbound.send(m)).collect();

        super::connection::receive(self.read_timeout, &self.failed_requests, responses_future?)
            .await
            .map(|responses| {
                responses
                    .into_iter()
                    .map(|response| match response {
                        Ok(response) => response,
                        Err(error) => error.to_response(self.version.unwrap()),
                    })
                    .collect()
            })
    }
}

#[async_trait]
impl Transform for CassandraSinkSingle {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.send_message(message_wrapper.messages).await
    }

    fn set_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }
}
