use crate::codec::{kafka::KafkaCodecBuilder, CodecBuilder, Direction};
use crate::connection::SinkConnection;
use crate::frame::kafka::{KafkaFrame, RequestBody, ResponseBody};
use crate::frame::{Frame, MessageType};
use crate::message::Messages;
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::{
    ChainState, DownChainTransforms, Transform, TransformBuilder, TransformContextBuilder,
    TransformContextConfig,
};
use crate::transforms::{DownChainProtocol, TransformConfig, UpChainProtocol};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::timeout;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
/// KafkaSinkSingle is designed solely for the use case of running a shotover instance on the same machine as each kafka instance.
/// The kafka instance and shotover instance must run on seperate ports.
pub struct KafkaSinkSingleConfig {
    pub destination_port: u16,
    pub connect_timeout_ms: u64,
    pub read_timeout: Option<u64>,
    pub tls: Option<TlsConnectorConfig>,
}

const NAME: &str = "KafkaSinkSingle";
#[typetag::serde(name = "KafkaSinkSingle")]
#[async_trait(?Send)]
impl TransformConfig for KafkaSinkSingleConfig {
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        let tls = self.tls.as_ref().map(TlsConnector::new).transpose()?;
        Ok(Box::new(KafkaSinkSingleBuilder::new(
            self.destination_port,
            transform_context.chain_name,
            self.connect_timeout_ms,
            self.read_timeout,
            tls,
        )))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::MustBeOneOf(vec![MessageType::Kafka])
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::Terminating
    }
}

struct KafkaSinkSingleBuilder {
    // contains address and port
    address_port: u16,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    tls: Option<TlsConnector>,
}

impl KafkaSinkSingleBuilder {
    fn new(
        address_port: u16,
        _chain_name: String,
        connect_timeout_ms: u64,
        timeout: Option<u64>,
        tls: Option<TlsConnector>,
    ) -> KafkaSinkSingleBuilder {
        let receive_timeout = timeout.map(Duration::from_secs);

        KafkaSinkSingleBuilder {
            address_port,
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            read_timeout: receive_timeout,
            tls,
        }
    }
}

impl TransformBuilder for KafkaSinkSingleBuilder {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(KafkaSinkSingle {
            connection: None,
            address_port: self.address_port,
            connect_timeout: self.connect_timeout,
            tls: self.tls.clone(),
            read_timeout: self.read_timeout,
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

struct KafkaSinkSingle {
    address_port: u16,
    connection: Option<SinkConnection>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    tls: Option<TlsConnector>,
    force_run_chain: Arc<Notify>,
}

#[async_trait]
impl Transform for KafkaSinkSingle {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform(
        &mut self,
        chain_state: &mut ChainState,
        _down_chain: DownChainTransforms<'_>,
    ) -> Result<Messages> {
        if self.connection.is_none() {
            let codec = KafkaCodecBuilder::new(Direction::Sink, "KafkaSinkSingle".to_owned());
            let address = (chain_state.local_addr.ip(), self.address_port);
            self.connection = Some(
                SinkConnection::new(
                    address,
                    codec,
                    &self.tls,
                    self.connect_timeout,
                    self.force_run_chain.clone(),
                    self.read_timeout,
                )
                .await?,
            );
        }

        let mut responses = vec![];
        if chain_state.requests.is_empty() {
            // there are no requests, so no point sending any, but we should check for any responses without awaiting
            self.connection
                .as_mut()
                .unwrap()
                .try_recv_into(&mut responses)?;
        } else {
            // send requests and wait until we have responses for all of them

            // Rewrite requests to use kafkas port instead of shotovers port
            for request in &mut chain_state.requests {
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

            // send
            let connection = self.connection.as_mut().unwrap();
            let requests_count = chain_state.requests.len();
            connection.send(std::mem::take(&mut chain_state.requests))?;

            // receive
            while responses.len() < requests_count {
                if let Some(read_timeout) = self.read_timeout {
                    timeout(read_timeout, connection.recv_into(&mut responses)).await?
                } else {
                    connection.recv_into(&mut responses).await
                }?;
            }
        }

        // Rewrite responses to use shotovers port instead of kafkas port
        for response in &mut responses {
            let port = chain_state.local_addr.port() as i32;
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
}
