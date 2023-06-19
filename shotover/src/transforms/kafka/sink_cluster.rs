use crate::codec::{kafka::KafkaCodecBuilder, CodecBuilder, Direction};
use crate::frame::kafka::{strbytes, KafkaFrame, ResponseBody};
use crate::frame::Frame;
use crate::message::Messages;
use crate::tcp;
use crate::transforms::kafka::common::send_requests;
use crate::transforms::util::cluster_connection_pool::{spawn_read_write_tasks, Connection};
use crate::transforms::util::Response;
use crate::transforms::{Transform, TransformBuilder, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use kafka_protocol::protocol::StrBytes;
use serde::Deserialize;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

#[derive(Deserialize, Debug)]
pub struct KafkaSinkClusterConfig {
    pub first_contact_points: Vec<String>,
    pub shotover_nodes: Vec<String>,
    pub connect_timeout_ms: u64,
    pub read_timeout: Option<u64>,
}

#[cfg(feature = "alpha-transforms")]
use crate::transforms::TransformConfig;

#[cfg(feature = "alpha-transforms")]
#[typetag::deserialize(name = "KafkaSinkCluster")]
#[async_trait(?Send)]
impl TransformConfig for KafkaSinkClusterConfig {
    async fn get_builder(&self, chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(KafkaSinkClusterBuilder::new(
            self.first_contact_points.clone(),
            self.shotover_nodes.clone(),
            chain_name,
            self.connect_timeout_ms,
            self.read_timeout,
        )))
    }
}

pub struct KafkaSinkClusterBuilder {
    // contains address and port
    first_contact_points: Vec<String>,
    shotover_nodes: Vec<KafkaAddress>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
}

impl KafkaSinkClusterBuilder {
    pub fn new(
        first_contact_points: Vec<String>,
        shotover_nodes: Vec<String>,
        _chain_name: String,
        connect_timeout_ms: u64,
        timeout: Option<u64>,
    ) -> KafkaSinkClusterBuilder {
        let receive_timeout = timeout.map(Duration::from_secs);

        let shotover_nodes = shotover_nodes
            .into_iter()
            .map(|node| {
                let address: SocketAddr = node.parse().unwrap();
                KafkaAddress {
                    host: strbytes(&address.ip().to_string()),
                    port: address.port() as i32,
                }
            })
            .collect();

        KafkaSinkClusterBuilder {
            first_contact_points,
            shotover_nodes,
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            read_timeout: receive_timeout,
        }
    }
}

impl TransformBuilder for KafkaSinkClusterBuilder {
    fn build(&self) -> Transforms {
        Transforms::KafkaSinkCluster(KafkaSinkCluster {
            outbound: None,
            first_contact_points: self.first_contact_points.clone(),
            shotover_nodes: self.shotover_nodes.clone(),
            pushed_messages_tx: None,
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
        })
    }

    fn get_name(&self) -> &'static str {
        "KafkaClusterSingle"
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct KafkaAddress {
    host: StrBytes,
    port: i32,
}

pub struct KafkaSinkCluster {
    first_contact_points: Vec<String>,
    shotover_nodes: Vec<KafkaAddress>,
    outbound: Option<Connection>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
}

#[async_trait]
impl Transform for KafkaSinkCluster {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> Result<Messages> {
        if self.outbound.is_none() {
            let codec = KafkaCodecBuilder::new(Direction::Sink);
            let tcp_stream = tcp::tcp_stream(
                self.connect_timeout,
                self.first_contact_points.first().unwrap(),
            )
            .await?;
            let (rx, tx) = tcp_stream.into_split();
            self.outbound = Some(spawn_read_write_tasks(&codec, rx, tx));
        }

        let outbound = self.outbound.as_mut().unwrap();
        let responses = send_requests(message_wrapper.messages, outbound)?;

        // TODO: since kafka will never send requests out of order I wonder if it would be faster to use an mpsc instead of a oneshot or maybe just directly run the sending/receiving here?
        let mut responses = if let Some(read_timeout) = self.read_timeout {
            timeout(read_timeout, read_responses(responses)).await?
        } else {
            read_responses(responses).await
        }?;

        // Rewrite responses to use shotovers port instead of kafkas port
        for response in &mut responses {
            match response.frame() {
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::FindCoordinator(find_coordinator),
                    version,
                    ..
                })) => {
                    if *version <= 3 {
                        rewrite_address(
                            &self.shotover_nodes,
                            &mut find_coordinator.host,
                            &mut find_coordinator.port,
                        )
                    } else {
                        for coordinator in &mut find_coordinator.coordinators {
                            rewrite_address(
                                &self.shotover_nodes,
                                &mut coordinator.host,
                                &mut coordinator.port,
                            )
                        }
                    }
                    response.invalidate_cache();
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Metadata(metadata),
                    ..
                })) => {
                    for broker in &mut metadata.brokers {
                        rewrite_address(
                            &self.shotover_nodes,
                            &mut broker.1.host,
                            &mut broker.1.port,
                        )
                    }
                    response.invalidate_cache();
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::DescribeCluster(describe_cluster),
                    ..
                })) => {
                    for broker in &mut describe_cluster.brokers {
                        rewrite_address(
                            &self.shotover_nodes,
                            &mut broker.1.host,
                            &mut broker.1.port,
                        )
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

async fn read_responses(responses: Vec<oneshot::Receiver<Response>>) -> Result<Messages> {
    let mut result = Vec::with_capacity(responses.len());
    for response in responses {
        result.push(response.await.unwrap().response?);
    }
    Ok(result)
}

fn hash_address(host: &str, port: i32) -> u64 {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.write(host.as_bytes());
    hasher.write(&port.to_be_bytes());
    hasher.finish()
}

fn rewrite_address(shotover_nodes: &[KafkaAddress], host: &mut StrBytes, port: &mut i32) {
    // do not attempt to rewrite if the port is not provided (-1)
    // this is known to occur in an error response
    if *port >= 0 {
        let shotover_node =
            &shotover_nodes[hash_address(host, *port) as usize % shotover_nodes.len()];
        *host = shotover_node.host.clone();
        *port = shotover_node.port;
    }
}
