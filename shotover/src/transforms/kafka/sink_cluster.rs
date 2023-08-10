use super::common::produce_channel;
use crate::codec::{kafka::KafkaCodecBuilder, CodecBuilder, Direction};
use crate::frame::kafka::{strbytes, KafkaFrame, RequestBody, ResponseBody};
use crate::frame::Frame;
use crate::message::{Message, Messages};
use crate::tcp;
use crate::transforms::util::cluster_connection_pool::{spawn_read_write_tasks, Connection};
use crate::transforms::util::{Request, Response};
use crate::transforms::{Transform, TransformBuilder, Transforms, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{
    ApiKey, FindCoordinatorRequest, GroupId, HeartbeatRequest, JoinGroupRequest, MetadataRequest,
    MetadataResponse, OffsetFetchRequest, RequestHeader, SyncGroupRequest, TopicName,
};
use kafka_protocol::protocol::{Builder, StrBytes};
use rand::rngs::SmallRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::SeedableRng;
use serde::Deserialize;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::timeout;
use uuid::Uuid;

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
    coordinator_broker_id: Arc<DashMap<StrBytes, i32>>,
    topics: Arc<DashMap<StrBytes, Topic>>,
    nodes_shared: Arc<RwLock<Vec<KafkaNode>>>,
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
            coordinator_broker_id: Arc::new(DashMap::new()),
            topics: Arc::new(DashMap::new()),
            nodes_shared: Arc::new(RwLock::new(vec![])),
        }
    }
}

impl TransformBuilder for KafkaSinkClusterBuilder {
    fn build(&self) -> Transforms {
        Transforms::KafkaSinkCluster(KafkaSinkCluster {
            first_contact_points: self.first_contact_points.clone(),
            shotover_nodes: self.shotover_nodes.clone(),
            pushed_messages_tx: None,
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
            nodes: vec![],
            nodes_shared: self.nodes_shared.clone(),
            coordinator_broker_id: self.coordinator_broker_id.clone(),
            topics: self.topics.clone(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
        })
    }

    fn get_name(&self) -> &'static str {
        "KafkaSinkCluster"
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

pub struct KafkaSinkCluster {
    first_contact_points: Vec<String>,
    shotover_nodes: Vec<KafkaAddress>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    nodes: Vec<KafkaNode>,
    nodes_shared: Arc<RwLock<Vec<KafkaNode>>>,
    coordinator_broker_id: Arc<DashMap<StrBytes, i32>>,
    topics: Arc<DashMap<StrBytes, Topic>>,
    rng: SmallRng,
}

#[async_trait]
impl Transform for KafkaSinkCluster {
    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        if self.nodes.is_empty() {
            let nodes: Result<Vec<KafkaNode>> = self
                .first_contact_points
                .iter()
                .map(|address| {
                    Ok(KafkaNode {
                        connection: None,
                        kafka_address: KafkaAddress::from_str(address)?,
                        broker_id: -1,
                    })
                })
                .collect();
            self.nodes = nodes?;
        }

        self.update_local_nodes().await;

        let mut find_coordinator_requests = vec![];
        for (index, request) in requests_wrapper.requests.iter_mut().enumerate() {
            if let Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::FindCoordinator(find_coordinator),
                ..
            })) = request.frame()
            {
                find_coordinator_requests.push(FindCoordinator {
                    index,
                    key: find_coordinator.key.clone(),
                    key_type: find_coordinator.key_type,
                });
            }
        }

        let responses = self.send_requests(requests_wrapper.requests).await?;
        self.receive_responses(&find_coordinator_requests, responses)
            .await
    }

    fn set_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }
}

impl KafkaSinkCluster {
    fn store_topic(&self, topics: &mut Vec<StrBytes>, topic: TopicName) {
        if self.topics.get(&topic.0).is_none() && !topics.contains(&topic.0) {
            topics.push(topic.0);
        }
    }

    fn store_group(&self, groups: &mut Vec<StrBytes>, group_id: GroupId) {
        if self.coordinator_broker_id.get(&group_id.0).is_none() && !groups.contains(&group_id.0) {
            groups.push(group_id.0);
        }
    }

    async fn update_local_nodes(&mut self) {
        for shared_node in self.nodes_shared.read().await.iter() {
            let mut found = false;
            for node in &mut self.nodes {
                if shared_node.kafka_address == node.kafka_address {
                    found = true;
                    node.broker_id = shared_node.broker_id;
                }
            }
            if !found {
                self.nodes.push(shared_node.clone())
            }
        }
    }

    async fn send_requests(
        &mut self,
        mut requests: Vec<Message>,
    ) -> Result<Vec<oneshot::Receiver<Response>>> {
        let mut results = Vec::with_capacity(requests.len());
        let mut topics = vec![];
        let mut groups = vec![];
        for request in &mut requests {
            match request.frame() {
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Produce(produce),
                    ..
                })) => {
                    for (name, _) in &produce.topic_data {
                        self.store_topic(&mut topics, name.clone());
                    }
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Fetch(fetch),
                    ..
                })) => {
                    for topic in &fetch.topics {
                        self.store_topic(&mut topics, topic.topic.clone());
                    }
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body:
                        RequestBody::Heartbeat(HeartbeatRequest { group_id, .. })
                        | RequestBody::SyncGroup(SyncGroupRequest { group_id, .. })
                        | RequestBody::OffsetFetch(OffsetFetchRequest { group_id, .. })
                        | RequestBody::JoinGroup(JoinGroupRequest { group_id, .. }),
                    ..
                })) => {
                    self.store_group(&mut groups, group_id.clone());
                }
                _ => {}
            }
        }

        for group in groups {
            let request = Message::from_frame(Frame::Kafka(KafkaFrame::Request {
                header: RequestHeader::builder()
                    .request_api_key(ApiKey::FindCoordinatorKey as i16)
                    .request_api_version(2)
                    .correlation_id(0)
                    .client_id(None)
                    .unknown_tagged_fields(Default::default())
                    .build()
                    .unwrap(),
                body: RequestBody::FindCoordinator(
                    FindCoordinatorRequest::builder()
                        .coordinator_keys(vec![])
                        .key_type(0)
                        .key(group.clone())
                        .unknown_tagged_fields(Default::default())
                        .build()
                        .unwrap(),
                ),
            }));

            let connection = self
                .nodes
                .choose_mut(&mut self.rng)
                .unwrap()
                .get_connection(self.connect_timeout)
                .await?;
            let (tx, rx) = oneshot::channel();
            connection
                .send(Request {
                    message: request,
                    return_chan: Some(tx),
                })
                .map_err(|_| anyhow!("Failed to send"))?;
            let mut response = rx.await.unwrap().response.unwrap();
            match response.frame() {
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::FindCoordinator(coordinator),
                    ..
                })) => {
                    self.coordinator_broker_id
                        .insert(group, coordinator.node_id.0);
                }
                other => {
                    return Err(anyhow!(
                        "Unexpected message returned to metadata request {other:?}"
                    ))
                }
            }
        }

        if !topics.is_empty() {
            let request = Message::from_frame(Frame::Kafka(KafkaFrame::Request {
                header: RequestHeader::builder()
                    .request_api_key(ApiKey::MetadataKey as i16)
                    .request_api_version(4)
                    .correlation_id(0)
                    .client_id(None)
                    .unknown_tagged_fields(Default::default())
                    .build()
                    .unwrap(),
                body: RequestBody::Metadata(
                    MetadataRequest::builder()
                        .topics(Some(
                            topics
                                .into_iter()
                                .map(|name| {
                                    MetadataRequestTopic::builder()
                                        .name(Some(TopicName(name)))
                                        .topic_id(Uuid::nil())
                                        .unknown_tagged_fields(Default::default())
                                        .build()
                                        .unwrap()
                                })
                                .collect(),
                        ))
                        .allow_auto_topic_creation(false)
                        .include_cluster_authorized_operations(false)
                        .include_topic_authorized_operations(false)
                        .unknown_tagged_fields(Default::default())
                        .build()
                        .unwrap(),
                ),
            }));

            let connection = self
                .nodes
                .choose_mut(&mut self.rng)
                .unwrap()
                .get_connection(self.connect_timeout)
                .await?;
            let (tx, rx) = oneshot::channel();
            connection
                .send(Request {
                    message: request,
                    return_chan: Some(tx),
                })
                .map_err(|_| anyhow!("Failed to send"))?;
            let mut response = rx.await.unwrap().response.unwrap();
            match response.frame() {
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Metadata(metadata),
                    ..
                })) => self.process_metadata(metadata).await,
                other => {
                    return Err(anyhow!(
                        "Unexpected message returned to metadata request {other:?}"
                    ))
                }
            }
        }

        for mut message in requests {
            match message.frame() {
                // route to partition leader
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Produce(produce),
                    ..
                })) => {
                    let mut connection = None;
                    // assume that all topics in this message have the same routing requirements
                    let (topic_name, topic_data) = produce
                        .topic_data
                        .iter()
                        .next()
                        .ok_or_else(|| anyhow!("No topics in produce message"))?;
                    if let Some(topic) = self.topics.get(&topic_name.0) {
                        // assume that all partitions in this topic have the same routing requirements
                        let partition = &topic.partitions[topic_data
                            .partition_data
                            .first()
                            .ok_or_else(|| anyhow!("No partitions in topic"))?
                            .index
                            as usize];
                        for node in &mut self.nodes {
                            if node.broker_id == partition.leader_id {
                                connection =
                                    Some(node.get_connection(self.connect_timeout).await?.clone());
                            }
                        }
                    }
                    let connection = match connection {
                        Some(connection) => connection,
                        None => {
                            tracing::warn!("no known partition leader for {topic_name:?}, routing message to a random node so that a NOT_LEADER_OR_FOLLOWER or similar error is returned to the client");
                            self.nodes
                                .choose_mut(&mut self.rng)
                                .unwrap()
                                .get_connection(self.connect_timeout)
                                .await?
                                .clone()
                        }
                    };

                    let (return_chan, rx) = produce_channel(produce);

                    connection
                        .send(Request {
                            message,
                            return_chan,
                        })
                        .map_err(|_| anyhow!("Failed to send"))?;
                    results.push(rx);
                }

                // route to random partition replica
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Fetch(fetch),
                    ..
                })) => {
                    // assume that all topics in this message have the same routing requirements
                    let topic = fetch
                        .topics
                        .first()
                        .ok_or_else(|| anyhow!("No topics in produce message"))?;
                    let connection = if let Some(topic_meta) = self.topics.get(&topic.topic.0) {
                        // assume that all partitions in this topic have the same routing requirements
                        let partition = &topic_meta.partitions[topic
                            .partitions
                            .first()
                            .ok_or_else(|| anyhow!("No partitions in topic"))?
                            .partition
                            as usize];
                        self.nodes
                            .iter_mut()
                            .filter(|node| partition.replica_nodes.contains(&node.broker_id))
                            .choose(&mut self.rng)
                            .unwrap()
                            .get_connection(self.connect_timeout)
                            .await?
                            .clone()
                    } else {
                        let topic = &topic.topic;
                        tracing::warn!("no known partition replica for {topic:?}, routing message to a random node so that a NOT_LEADER_OR_FOLLOWER or similar error is returned to the client");
                        self.nodes
                            .choose_mut(&mut self.rng)
                            .unwrap()
                            .get_connection(self.connect_timeout)
                            .await?
                            .clone()
                    };

                    let (tx, rx) = oneshot::channel();
                    connection
                        .send(Request {
                            message,
                            return_chan: Some(tx),
                        })
                        .map_err(|_| anyhow!("Failed to send"))?;
                    results.push(rx);
                }

                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Heartbeat(heartbeat),
                    ..
                })) => {
                    let group_id = heartbeat.group_id.0.clone();
                    results.push(self.route_to_coordinator(message, group_id).await?);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::SyncGroup(sync_group),
                    ..
                })) => {
                    let group_id = sync_group.group_id.0.clone();
                    results.push(self.route_to_coordinator(message, group_id).await?);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::OffsetFetch(offset_fetch),
                    ..
                })) => {
                    let group_id = offset_fetch.group_id.0.clone();
                    results.push(self.route_to_coordinator(message, group_id).await?);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::JoinGroup(join_group),
                    ..
                })) => {
                    let group_id = join_group.group_id.0.clone();
                    results.push(self.route_to_coordinator(message, group_id).await?);
                }

                // route to random node
                _ => {
                    let connection = self
                        .nodes
                        .choose_mut(&mut self.rng)
                        .unwrap()
                        .get_connection(self.connect_timeout)
                        .await?;
                    let (tx, rx) = oneshot::channel();
                    connection
                        .send(Request {
                            message,
                            return_chan: Some(tx),
                        })
                        .map_err(|_| anyhow!("Failed to send"))?;
                    results.push(rx);
                }
            }
        }
        Ok(results)
    }

    async fn receive_responses(
        &mut self,
        find_coordinator_requests: &[FindCoordinator],
        responses: Vec<oneshot::Receiver<Response>>,
    ) -> Result<Vec<Message>> {
        // TODO: since kafka will never send requests out of order I wonder if it would be faster to use an mpsc instead of a oneshot or maybe just directly run the sending/receiving here?
        let mut responses = if let Some(read_timeout) = self.read_timeout {
            timeout(read_timeout, read_responses(responses)).await?
        } else {
            read_responses(responses).await
        }?;

        // TODO: Handle errors like NOT_COORDINATOR by removing element from self.topics and self.coordinator_broker_id

        // Rewrite responses to use shotovers port instead of kafkas port
        for (i, response) in responses.iter_mut().enumerate() {
            match response.frame() {
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::FindCoordinator(find_coordinator),
                    version,
                    ..
                })) => {
                    let request = find_coordinator_requests
                        .iter()
                        .find(|x| x.index == i)
                        .ok_or_else(|| anyhow!("Received find_coordinator but not requested"))?;

                    if *version <= 3 {
                        if request.key_type == 0 {
                            self.coordinator_broker_id
                                .insert(request.key.clone(), find_coordinator.node_id.0);
                        }
                        rewrite_address(
                            &self.shotover_nodes,
                            &mut find_coordinator.host,
                            &mut find_coordinator.port,
                        )
                    } else {
                        for coordinator in &mut find_coordinator.coordinators {
                            if request.key_type == 0 {
                                self.coordinator_broker_id
                                    .insert(coordinator.key.clone(), find_coordinator.node_id.0);
                            }
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
                    self.process_metadata(metadata).await;

                    for (_, broker) in &mut metadata.brokers {
                        rewrite_address(&self.shotover_nodes, &mut broker.host, &mut broker.port)
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

    async fn route_to_coordinator(
        &mut self,
        message: Message,
        group_name: StrBytes,
    ) -> Result<oneshot::Receiver<Response>> {
        let mut connection = None;
        for node in &mut self.nodes {
            if let Some(broker_id) = self.coordinator_broker_id.get(&group_name) {
                if node.broker_id == *broker_id {
                    connection = Some(node.get_connection(self.connect_timeout).await?.clone());
                }
            }
        }
        let connection = match connection {
            Some(connection) => connection,
            None => {
                tracing::warn!("no known coordinator for {group_name:?}, routing message to a random node so that a NOT_COORDINATOR or similar error is returned to the client");
                self.nodes
                    .choose_mut(&mut self.rng)
                    .unwrap()
                    .get_connection(self.connect_timeout)
                    .await?
                    .clone()
            }
        };
        let (tx, rx) = oneshot::channel();
        connection
            .send(Request {
                message,
                return_chan: Some(tx),
            })
            .map_err(|_| anyhow!("Failed to send"))?;
        Ok(rx)
    }

    async fn process_metadata(&mut self, metadata: &MetadataResponse) {
        for (id, broker) in &metadata.brokers {
            let new = self
                .nodes_shared
                .read()
                .await
                .iter()
                .all(|node| node.broker_id != **id);
            if new {
                let host = broker.host.clone();
                let port = broker.port;
                let node = KafkaNode {
                    broker_id: **id,
                    kafka_address: KafkaAddress { host, port },
                    connection: None,
                };
                self.nodes_shared.write().await.push(node);

                self.update_local_nodes().await;
            }
        }

        for topic in &metadata.topics {
            self.topics.insert(
                topic.0.clone().0,
                Topic {
                    partitions: topic
                        .1
                        .partitions
                        .iter()
                        .map(|partition| Partition {
                            leader_id: *partition.leader_id,
                            replica_nodes: partition.replica_nodes.iter().map(|x| x.0).collect(),
                        })
                        .collect(),
                },
            );
        }
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

#[derive(Clone)]
struct KafkaNode {
    broker_id: i32,
    kafka_address: KafkaAddress,
    connection: Option<Connection>,
}

impl KafkaNode {
    async fn get_connection(&mut self, connect_timeout: Duration) -> Result<&Connection> {
        if self.connection.is_none() {
            let codec = KafkaCodecBuilder::new(Direction::Sink);
            let tcp_stream = tcp::tcp_stream(
                connect_timeout,
                (
                    self.kafka_address.host.to_string(),
                    self.kafka_address.port as u16,
                ),
            )
            .await?;
            let (rx, tx) = tcp_stream.into_split();
            self.connection = Some(spawn_read_write_tasks(&codec, rx, tx));
        }
        Ok(self.connection.as_ref().unwrap())
    }
}

struct Topic {
    partitions: Vec<Partition>,
}
struct Partition {
    leader_id: i32,
    replica_nodes: Vec<i32>,
}

#[derive(Clone, PartialEq)]
struct KafkaAddress {
    host: StrBytes,
    port: i32,
}

impl KafkaAddress {
    fn from_str(address: &str) -> Result<Self> {
        let mut address_iter = address.split(':');
        Ok(KafkaAddress {
            host: strbytes(
                address_iter
                    .next()
                    .ok_or_else(|| anyhow!("Address must include ':' seperator"))?,
            ),
            port: address_iter
                .next()
                .ok_or_else(|| anyhow!("Address must include port after ':'"))?
                .parse()
                .map_err(|_| anyhow!("Failed to parse address port as integer"))?,
        })
    }
}

struct FindCoordinator {
    index: usize,
    key: StrBytes,
    key_type: i8,
}
