use crate::codec::{CodecBuilder, Direction};
use crate::frame::{Frame, RedisFrame};
use crate::message::{Message, Messages};
use crate::server::spawn_read_write_tasks;
use crate::tcp;
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::{
    Transform, TransformBuilder, TransformConfig, TransformContextBuilder, Wrapper,
};
use crate::{codec::redis::RedisCodecBuilder, transforms::TransformContextConfig};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use metrics::{counter, Counter};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::split;
use tokio::sync::{mpsc, Notify};

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

struct Connection {
    in_rx: mpsc::Receiver<Vec<Message>>,
    out_tx: mpsc::UnboundedSender<Vec<Message>>,
}

#[async_trait]
impl Transform for RedisSinkSingle {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        if self.connection.is_none() {
            let (in_tx, in_rx) = mpsc::channel::<Messages>(10_000);
            let (out_tx, out_rx) = mpsc::unbounded_channel::<Messages>();
            let codec = RedisCodecBuilder::new(Direction::Sink, "RedisSinkSingle".to_owned());
            if let Some(tls) = self.tls.as_mut() {
                let tls_stream = tls.connect(self.connect_timeout, &self.address).await?;
                let (rx, tx) = split(tls_stream);
                spawn_read_write_tasks(
                    codec,
                    rx,
                    tx,
                    in_tx,
                    out_rx,
                    out_tx.clone(),
                    Some(self.force_run_chain.clone()),
                );
            } else {
                let tcp_stream = tcp::tcp_stream(self.connect_timeout, &self.address).await?;
                let (rx, tx) = tcp_stream.into_split();
                spawn_read_write_tasks(
                    codec,
                    rx,
                    tx,
                    in_tx,
                    out_rx,
                    out_tx.clone(),
                    Some(self.force_run_chain.clone()),
                );
            }
            self.connection = Some(Connection { in_rx, out_tx });
        }

        if requests_wrapper.requests.is_empty() {
            // there are no requests, so no point sending any, but we should check for any responses without awaiting
            if let Ok(mut responses) = self.connection.as_mut().unwrap().in_rx.try_recv() {
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
                .out_tx
                .send(requests_wrapper.requests)
                .map_err(|err| anyhow!("Failed to send messages to redis destination: {err:?}"))?;

            let mut result = vec![];
            let mut responses_count = 0;
            while responses_count < requests_count {
                let mut responses = self
                    .connection
                    .as_mut()
                    .unwrap()
                    .in_rx
                    .recv()
                    .await
                    .ok_or_else(|| {
                        anyhow!("Failed to receive message because recv task is dead")
                    })?;

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
