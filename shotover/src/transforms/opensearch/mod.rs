use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::{
    Messages, Transform, TransformBuilder, TransformConfig, Transforms, Wrapper,
};
use anyhow::Result;
use async_trait::async_trait;
use metrics::{register_counter, Counter};
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize, Debug)]
pub struct OpenSearchSinkConfig {
    #[serde(rename = "remote_address")]
    address: String,
    tls: Option<TlsConnectorConfig>,
    connect_timeout_ms: u64,
}

#[typetag::deserialize(name = "OpenSearchSink")]
#[async_trait(?Send)]
impl TransformConfig for OpenSearchSinkConfig {
    async fn get_builder(&self, chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Box::new(OpenSearchSinkBuilder::new(
            self.address.clone(),
            tls,
            chain_name,
            self.connect_timeout_ms,
        )))
    }
}

#[derive(Clone)]
pub struct OpenSearchSinkBuilder {
    address: String,
    tls: Option<TlsConnector>,
    failed_requests: Counter,
    connect_timeout: Duration,
}

impl OpenSearchSinkBuilder {
    pub fn new(
        address: String,
        tls: Option<TlsConnector>,
        chain_name: String,
        connect_timeout_ms: u64,
    ) -> Self {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name, "transform" => "OpenSearchSink");
        let connect_timeout = Duration::from_millis(connect_timeout_ms);

        Self {
            address,
            tls,
            failed_requests,
            connect_timeout,
        }
    }
}

impl TransformBuilder for OpenSearchSinkBuilder {
    fn build(&self) -> Transforms {
        Transforms::OpenSearchSink(OpenSearchSink {
            address: self.address.clone(),
            tls: self.tls.clone(),
            failed_requests: self.failed_requests.clone(),
            connect_timeout: self.connect_timeout,
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
    tls: Option<TlsConnector>,
    failed_requests: Counter,
    connect_timeout: Duration,
}

#[async_trait]
impl Transform for OpenSearchSink {
    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        tracing::info!("{:?}", requests_wrapper);
        todo!();
    }
}
