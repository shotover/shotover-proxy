use crate::codec::{kafka::KafkaCodecBuilder, CodecBuilder, Direction};
use crate::frame::kafka::{KafkaFrame, RequestBody, ResponseBody};
use crate::frame::Frame;
use crate::message::{Message, Messages};
use crate::tcp;
use crate::transforms::kafka::common::produce_channel;
use crate::transforms::util::cluster_connection_pool::{spawn_read_write_tasks, Connection};
use crate::transforms::util::{Request, Response};
use crate::transforms::{Transform, TransformBuilder, Transforms, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct KafkaSinkSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub connect_timeout_ms: u64,
    pub read_timeout: Option<u64>,
}

#[cfg(feature = "alpha-transforms")]
use crate::transforms::TransformConfig;

#[cfg(feature = "alpha-transforms")]
#[typetag::serde(name = "KafkaSinkSingle")]
#[async_trait(?Send)]
impl TransformConfig for KafkaSinkSingleConfig {
    async fn get_builder(&self, chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(KafkaSinkSingleBuilder::new(
            self.address.clone(),
            chain_name,
            self.connect_timeout_ms,
            self.read_timeout,
        )))
    }
}

pub struct KafkaSinkSingleBuilder {
    // contains address and port
    address: String,
    address_port: u16,
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
        let address_port = address
            .rsplit(':')
            .next()
            .and_then(|str| str.parse().ok())
            .unwrap_or(9092);

        KafkaSinkSingleBuilder {
            address,
            address_port,
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
            address_port: self.address_port,
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
    address_port: u16,
    outbound: Option<Connection>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
}

#[async_trait]
impl Transform for KafkaSinkSingle {
    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        if self.outbound.is_none() {
            let codec = KafkaCodecBuilder::new(Direction::Sink);
            let tcp_stream = tcp::tcp_stream(self.connect_timeout, &self.address).await?;
            let (rx, tx) = tcp_stream.into_split();
            self.outbound = Some(spawn_read_write_tasks(&codec, rx, tx));
        }

        // Rewrite requests to use kafkas port instead of shotovers port
        for request in &mut requests_wrapper.requests {
            if let Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::LeaderAndIsr(leader_and_isr),
                ..
            })) = request.frame()
            {
                for leader in &mut leader_and_isr.live_leaders {
                    leader.port = self.address_port as i32;
                }
                request.invalidate_cache();
            }
        }

        let responses = self.send_requests(requests_wrapper.requests)?;

        // TODO: since kafka will never send requests out of order I wonder if it would be faster to use an mpsc instead of a oneshot or maybe just directly run the sending/receiving here?
        let mut responses = if let Some(read_timeout) = self.read_timeout {
            timeout(read_timeout, read_responses(responses)).await?
        } else {
            read_responses(responses).await
        }?;

        // Rewrite responses to use shotovers port instead of kafkas port
        for response in &mut responses {
            let port = requests_wrapper.local_addr.port() as i32;
            match response.frame() {
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::FindCoordinator(find_coordinator),
                    version,
                    ..
                })) => {
                    if *version <= 3 {
                        find_coordinator.port = port;
                    } else {
                        for coordinator in &mut find_coordinator.coordinators {
                            coordinator.port = port;
                        }
                    }
                    response.invalidate_cache();
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Metadata(metadata),
                    ..
                })) => {
                    for broker in &mut metadata.brokers {
                        broker.1.port = port;
                    }
                    response.invalidate_cache();
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::DescribeCluster(describe_cluster),
                    ..
                })) => {
                    for broker in &mut describe_cluster.brokers {
                        broker.1.port = port;
                    }
                    response.invalidate_cache();
                }
                _ => {}
            }
        }

        Ok(responses)
    }

    fn set_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }
}

impl KafkaSinkSingle {
    pub fn send_requests(
        &self,
        messages: Vec<Message>,
    ) -> Result<Vec<oneshot::Receiver<Response>>> {
        let outbound = self.outbound.as_ref().unwrap();
        messages
            .into_iter()
            .map(|mut message| {
                let (return_chan, rx) = if let Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Produce(produce),
                    ..
                })) = message.frame()
                {
                    produce_channel(produce)
                } else {
                    let (tx, rx) = oneshot::channel();
                    (Some(tx), rx)
                };
                outbound
                    .send(Request {
                        message,
                        return_chan,
                    })
                    .map(|_| rx)
                    .map_err(|_| anyhow!("Failed to send"))
            })
            .collect()
    }
}

async fn read_responses(responses: Vec<oneshot::Receiver<Response>>) -> Result<Messages> {
    let mut result = Vec::with_capacity(responses.len());
    for response in responses {
        result.push(response.await.unwrap().response?);
    }
    Ok(result)
}
