use crate::codec::{CodecBuilder, Direction};
use crate::connection::Connection;
use crate::frame::{Frame, RedisFrame};
use crate::message::Messages;
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::{
    Transform, TransformBuilder, TransformConfig, TransformContextBuilder, Wrapper,
};
use crate::{codec::redis::RedisCodecBuilder, transforms::TransformContextConfig};
use anyhow::Result;
use async_trait::async_trait;
use metrics::{counter, Counter};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct RedisSinkSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub tls: Option<TlsConnectorConfig>,
    pub connect_timeout_ms: u64,
}

const NAME: &str = "RedisSinkSingle";
#[typetag::serde(name = "RedisSinkSingle")]
#[async_trait(?Send)]
impl TransformConfig for RedisSinkSingleConfig {
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Box::new(RedisSinkSingleBuilder::new(
            self.address.clone(),
            tls,
            transform_context.chain_name,
            self.connect_timeout_ms,
        )))
    }
}

#[derive(Clone)]
pub struct RedisSinkSingleBuilder {
    address: String,
    tls: Option<TlsConnector>,
    failed_requests: Counter,
    connect_timeout: Duration,
}

impl RedisSinkSingleBuilder {
    pub fn new(
        address: String,
        tls: Option<TlsConnector>,
        chain_name: String,
        connect_timeout_ms: u64,
    ) -> Self {
        let failed_requests = counter!("shotover_failed_requests_count", "chain" => chain_name, "transform" => "RedisSinkSingle");
        let connect_timeout = Duration::from_millis(connect_timeout_ms);

        RedisSinkSingleBuilder {
            address,
            tls,
            failed_requests,
            connect_timeout,
        }
    }
}

impl TransformBuilder for RedisSinkSingleBuilder {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(RedisSinkSingle {
            address: self.address.clone(),
            tls: self.tls.clone(),
            connection: None,
            failed_requests: self.failed_requests.clone(),
            connect_timeout: self.connect_timeout,
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

pub struct RedisSinkSingle {
    address: String,
    tls: Option<TlsConnector>,
    connection: Option<Connection>,
    failed_requests: Counter,
    connect_timeout: Duration,
    force_run_chain: Arc<Notify>,
}

#[async_trait]
impl Transform for RedisSinkSingle {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        if self.connection.is_none() {
            let codec = RedisCodecBuilder::new(Direction::Sink, "RedisSinkSingle".to_owned());
            self.connection = Some(
                Connection::new(
                    &self.address,
                    codec,
                    &mut self.tls,
                    self.connect_timeout,
                    Some(self.force_run_chain.clone()),
                )
                .await?,
            );
        }

        if requests_wrapper.requests.is_empty() {
            // there are no requests, so no point sending any, but we should check for any responses without awaiting
            if let Ok(mut responses) = self.connection.as_mut().unwrap().try_recv() {
                for response in &mut responses {
                    if let Some(Frame::Redis(RedisFrame::Error(_))) = response.frame() {
                        self.failed_requests.increment(1);
                    }
                }
                Ok(responses)
            } else {
                Ok(vec![])
            }
        } else {
            let requests_count = requests_wrapper.requests.len();
            self.connection
                .as_mut()
                .unwrap()
                .send(requests_wrapper.requests)?;

            let mut result = vec![];
            let mut responses_count = 0;
            while responses_count < requests_count {
                let mut responses = self.connection.as_mut().unwrap().recv().await?;

                for response in &mut responses {
                    if let Some(Frame::Redis(RedisFrame::Error(_))) = response.frame() {
                        self.failed_requests.increment(1);
                    }
                    if response.request_id().is_some() {
                        responses_count += 1;
                    }
                }
                result.extend(responses);
            }
            Ok(result)
        }
    }
}
