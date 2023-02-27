use crate::codec::kafka::{Direction, KafkaCodecBuilder};
use crate::error::ChainResponse;
use crate::frame::kafka::{KafkaFrame, ResponseBody};
use crate::frame::Frame;
use crate::message::Messages;
use crate::tcp;
use crate::transforms::util::cluster_connection_pool::{spawn_read_write_tasks, Connection};
use crate::transforms::util::{Request, Response};
use crate::transforms::{Transform, TransformBuilder, Transforms, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

#[derive(Deserialize, Debug, Clone)]
pub struct KafkaSinkSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub connect_timeout_ms: u64,
    pub read_timeout: Option<u64>,
}

impl KafkaSinkSingleConfig {
    pub async fn get_builder(&self, chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(KafkaSinkSingleBuilder::new(
            self.address.clone(),
            chain_name,
            self.connect_timeout_ms,
            self.read_timeout,
        )))
    }
}

#[derive(Clone)]
pub struct KafkaSinkSingleBuilder {
    address: String,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
}

impl KafkaSinkSingleBuilder {
    pub fn new(
        address: String,
        _chain_name: String,
        connect_timeout_ms: u64,
        timeout: Option<u64>,
    ) -> KafkaSinkSingleBuilder {
        let receive_timeout = timeout.map(Duration::from_secs);

        KafkaSinkSingleBuilder {
            address,
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            read_timeout: receive_timeout,
        }
    }
}

impl TransformBuilder for KafkaSinkSingleBuilder {
    fn build(&self) -> Transforms {
        Transforms::KafkaSinkSingle(KafkaSinkSingle {
            outbound: None,
            address: self.address.clone(),
            pushed_messages_tx: None,
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
        })
    }

    fn get_name(&self) -> &'static str {
        "KafkaSinkSingle"
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

pub struct KafkaSinkSingle {
    address: String,
    outbound: Option<Connection>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
}

#[async_trait]
impl Transform for KafkaSinkSingle {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        if self.outbound.is_none() {
            let codec = KafkaCodecBuilder::new(Direction::Sink);
            let tcp_stream = tcp::tcp_stream(self.connect_timeout, &self.address).await?;
            let (rx, tx) = tcp_stream.into_split();
            self.outbound = Some(spawn_read_write_tasks(&codec, rx, tx));
        }

        let outbound = self.outbound.as_mut().unwrap();
        let responses: Result<Vec<_>> = message_wrapper
            .messages
            .into_iter()
            .map(|message| {
                let (tx, rx) = oneshot::channel();
                outbound
                    .send(Request {
                        message,
                        return_chan: Some(tx),
                    })
                    .map(|_| rx)
                    .map_err(|_| anyhow!("Failed to send"))
            })
            .collect();
        let responses = responses?;

        // TODO: since kafka will never send requests out of order I wonder if it would be faster to use an mpsc instead of a oneshot or maybe just directly run the sending/receiving here?
        let mut responses = if let Some(read_timeout) = self.read_timeout {
            timeout(read_timeout, read_responses(responses)).await?
        } else {
            read_responses(responses).await
        }?;

        // Rewrite FindCoordinator responses messages to use shotovers port instead of kafkas port
        for response in &mut responses {
            if let Some(Frame::Kafka(KafkaFrame::Response {
                body: ResponseBody::FindCoordinator(find_coordinator),
                ..
            })) = response.frame()
            {
                let port = message_wrapper.local_addr.port() as i32;
                find_coordinator.port = port;
                for coordinator in &mut find_coordinator.coordinators {
                    coordinator.port = port;
                }
                response.invalidate_cache();
            }
        }

        Ok(responses)
    }

    fn set_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }
}

async fn read_responses(responses: Vec<oneshot::Receiver<Response>>) -> Result<Messages> {
    let mut result = Vec::with_capacity(responses.len());
    for response in responses {
        result.push(response.await.unwrap().response?);
    }
    Ok(result)
}
