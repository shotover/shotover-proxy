use super::common::produce_channel;
use crate::frame::kafka::{KafkaFrame, RequestBody, ResponseBody};
use crate::frame::Frame;
use crate::message::{Message, MessageIdMap, Messages};
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::util::{Request, Response};
use crate::transforms::{Transform, TransformBuilder, TransformContextBuilder, Wrapper};
use crate::transforms::{TransformConfig, TransformContextConfig};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
use kafka_protocol::messages::{
    ApiKey, BrokerId, FindCoordinatorRequest, FindCoordinatorResponse, GroupId, HeartbeatRequest,
    JoinGroupRequest, MetadataRequest, MetadataResponse, OffsetFetchRequest, RequestHeader,
    SyncGroupRequest, TopicName,
};
use kafka_protocol::protocol::{Builder, StrBytes};
use node::{ConnectionFactory, KafkaAddress, KafkaNode};
use rand::rngs::SmallRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::timeout;
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
enum FindCoordinatorError {
    #[error("Coordinator not available")]
    CoordinatorNotAvailable,
    #[error("{0:?}")]
    Unrecoverable(#[from] anyhow::Error),
}

mod node;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct KafkaSinkClusterConfig {
    pub first_contact_points: Vec<String>,
    pub shotover_nodes: Vec<ShotoverNodeConfig>,
    pub connect_timeout_ms: u64,
    pub read_timeout: Option<u64>,
    pub tls: Option<TlsConnectorConfig>,
    pub sasl_enabled: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct ShotoverNodeConfig {
    pub address: SocketAddr,
    pub rack: String,
    pub broker_id: i32,
}

impl ShotoverNodeConfig {
    fn build(self) -> ShotoverNode {
        let address = KafkaAddress {
            host: StrBytes::from_string(self.address.ip().to_string()),
            port: self.address.port() as i32,
        };
        ShotoverNode {
            address,
            rack: StrBytes::from_string(self.rack),
            broker_id: BrokerId(self.broker_id),
        }
    }
}

#[derive(Clone)]
pub struct ShotoverNode {
    pub address: KafkaAddress,
    pub rack: StrBytes,
    pub broker_id: BrokerId,
}

const NAME: &str = "KafkaSinkCluster";
#[typetag::serde(name = "KafkaSinkCluster")]
#[async_trait(?Send)]
impl TransformConfig for KafkaSinkClusterConfig {
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Box::new(KafkaSinkClusterBuilder::new(
            self.first_contact_points.clone(),
            self.shotover_nodes.clone(),
            transform_context.chain_name,
            self.connect_timeout_ms,
            self.read_timeout,
            tls,
            self.sasl_enabled.unwrap_or(false),
        )))
    }
}

pub struct KafkaSinkClusterBuilder {
    // contains address and port
    first_contact_points: Vec<String>,
    shotover_nodes: Vec<ShotoverNode>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    controller_broker: Arc<AtomicBrokerId>,
    group_to_coordinator_broker: Arc<DashMap<GroupId, BrokerId>>,
    topic_by_name: Arc<DashMap<TopicName, Topic>>,
    topic_by_id: Arc<DashMap<Uuid, Topic>>,
    nodes_shared: Arc<RwLock<Vec<KafkaNode>>>,
    tls: Option<TlsConnector>,
    sasl_enabled: bool,
}

impl KafkaSinkClusterBuilder {
    pub fn new(
        first_contact_points: Vec<String>,
        shotover_nodes: Vec<ShotoverNodeConfig>,
        _chain_name: String,
        connect_timeout_ms: u64,
        timeout: Option<u64>,
        tls: Option<TlsConnector>,
        sasl_enabled: bool,
    ) -> KafkaSinkClusterBuilder {
        let receive_timeout = timeout.map(Duration::from_secs);

        let mut shotover_nodes: Vec<_> = shotover_nodes
            .into_iter()
            .map(ShotoverNodeConfig::build)
            .collect();
        shotover_nodes.sort_by_key(|x| x.broker_id);

        KafkaSinkClusterBuilder {
            first_contact_points,
            shotover_nodes,
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            read_timeout: receive_timeout,
            controller_broker: Arc::new(AtomicBrokerId::new()),
            group_to_coordinator_broker: Arc::new(DashMap::new()),
            topic_by_name: Arc::new(DashMap::new()),
            topic_by_id: Arc::new(DashMap::new()),
            nodes_shared: Arc::new(RwLock::new(vec![])),
            tls,
            sasl_enabled,
        }
    }
}

impl TransformBuilder for KafkaSinkClusterBuilder {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(KafkaSinkCluster {
            first_contact_points: self.first_contact_points.clone(),
            shotover_nodes: self.shotover_nodes.clone(),
            pushed_messages_tx: None,
            read_timeout: self.read_timeout,
            nodes: vec![],
            nodes_shared: self.nodes_shared.clone(),
            controller_broker: self.controller_broker.clone(),
            group_to_coordinator_broker: self.group_to_coordinator_broker.clone(),
            topic_by_name: self.topic_by_name.clone(),
            topic_by_id: self.topic_by_id.clone(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            sasl_status: SaslStatus::new(self.sasl_enabled),
            connection_factory: ConnectionFactory::new(self.tls.clone(), self.connect_timeout),
            first_contact_node: None,
            fetch_session_id_to_broker: HashMap::new(),
            fetch_request_destinations: Default::default(),
        })
    }

    fn get_name(&self) -> &'static str {
        NAME
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

struct AtomicBrokerId(AtomicI64);

impl AtomicBrokerId {
    fn new() -> Self {
        AtomicBrokerId(i64::MAX.into())
    }

    fn set(&self, value: BrokerId) {
        self.0
            .store(value.0.into(), std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns `None` when set has never been called.
    /// Otherwise returns `Some` containing the latest set value.
    fn get(&self) -> Option<BrokerId> {
        match self.0.load(std::sync::atomic::Ordering::Relaxed) {
            i64::MAX => None,
            other => Some(BrokerId(other as i32)),
        }
    }
}

#[derive(Debug)]
struct SaslStatus {
    enabled: bool,
    handshake_complete: bool,
}

impl SaslStatus {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            handshake_complete: false,
        }
    }

    fn set_handshake_complete(&mut self) {
        self.handshake_complete = true;
    }

    fn is_handshake_complete(&self) -> bool {
        if self.enabled {
            self.handshake_complete
        } else {
            true
        }
    }
}

pub struct KafkaSinkCluster {
    first_contact_points: Vec<String>,
    shotover_nodes: Vec<ShotoverNode>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    read_timeout: Option<Duration>,
    nodes: Vec<KafkaNode>,
    nodes_shared: Arc<RwLock<Vec<KafkaNode>>>,
    controller_broker: Arc<AtomicBrokerId>,
    group_to_coordinator_broker: Arc<DashMap<GroupId, BrokerId>>,
    topic_by_name: Arc<DashMap<TopicName, Topic>>,
    topic_by_id: Arc<DashMap<Uuid, Topic>>,
    rng: SmallRng,
    sasl_status: SaslStatus,
    connection_factory: ConnectionFactory,
    first_contact_node: Option<BrokerId>,
    // its not clear from the docs if this cache needs to be accessed cross connection:
    // https://cwiki.apache.org/confluence/display/KAFKA/KIP-227%3A+Introduce+Incremental+FetchRequests+to+Increase+Partition+Scalability
    fetch_session_id_to_broker: HashMap<i32, BrokerId>,
    // for use with fetch_session_id_to_broker
    fetch_request_destinations: MessageIdMap<BrokerId>,
}

#[async_trait]
impl Transform for KafkaSinkCluster {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        if requests_wrapper.requests.is_empty() {
            return Ok(vec![]);
        }

        if self.nodes.is_empty() {
            let nodes: Result<Vec<KafkaNode>> = self
                .first_contact_points
                .iter()
                .map(|address| {
                    Ok(KafkaNode::new(
                        BrokerId(-1),
                        KafkaAddress::from_str(address)?,
                        None,
                    ))
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
    fn store_topic(&self, topics: &mut Vec<TopicName>, topic: TopicName) {
        if self.topic_by_name.get(&topic).is_none() && !topics.contains(&topic) {
            topics.push(topic);
        }
    }

    fn store_group(&self, groups: &mut Vec<GroupId>, group_id: GroupId) {
        if self.group_to_coordinator_broker.get(&group_id).is_none() && !groups.contains(&group_id)
        {
            groups.push(group_id);
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
                    // TODO: Handle topics that only have an ID
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
            match self.find_coordinator_of_group(group.clone()).await {
                Ok(node) => {
                    self.group_to_coordinator_broker
                        .insert(group, node.broker_id);
                    self.add_node_if_new(node).await;
                }
                Err(FindCoordinatorError::CoordinatorNotAvailable) => {
                    // We cant find the coordinator so do nothing so that the request will be routed to a random node:
                    // * If it happens to be the coordinator all is well
                    // * If its not the coordinator then it will return a COORDINATOR_NOT_AVAILABLE message to
                    //   the client prompting it to retry the whole process again.
                }
                Err(FindCoordinatorError::Unrecoverable(err)) => Err(err)?,
            }
        }

        // request and process metadata if we are missing topics or the controller broker id
        if (!topics.is_empty() || self.controller_broker.get().is_none())
            && self.sasl_status.is_handshake_complete()
        {
            let mut metadata = self.get_metadata_of_topics(topics).await?;
            match metadata.frame() {
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Metadata(metadata),
                    ..
                })) => self.process_metadata_response(metadata).await,
                other => {
                    return Err(anyhow!(
                        "Unexpected message returned to metadata request {other:?}"
                    ))
                }
            }
        }

        for mut message in requests {
            // This routing is documented in transforms.md so make sure to update that when making changes here.
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
                    if let Some(topic) = self.topic_by_name.get(&topic_name.0) {
                        // assume that all partitions in this topic have the same routing requirements
                        let partition = &topic.partitions[topic_data
                            .partition_data
                            .first()
                            .ok_or_else(|| anyhow!("No partitions in topic"))?
                            .index
                            as usize];
                        for node in &mut self.nodes {
                            if node.broker_id == partition.leader_id {
                                connection = Some(
                                    node.get_connection(&self.connection_factory).await?.clone(),
                                );
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
                                .get_connection(&self.connection_factory)
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
                    let node = if fetch.session_id == 0 {
                        // assume that all topics in this message have the same routing requirements
                        let topic = fetch
                            .topics
                            .first()
                            .ok_or_else(|| anyhow!("No topics in fetch message"))?;

                        // This way of constructing topic_meta is kind of crazy, but it works around borrow checker limitations
                        // Old clients only specify the topic name and some newer clients only specify the topic id.
                        // So we need to check the id first and then fallback to the name.
                        let topic_name = &topic.topic;
                        let topic_by_id = self.topic_by_id.get(&topic.topic_id);
                        let topic_by_name;
                        let mut topic_meta = topic_by_id.as_deref();
                        if topic_meta.is_none() {
                            topic_by_name = self.topic_by_name.get(&topic.topic);
                            topic_meta = topic_by_name.as_deref();
                        }

                        let node = if let Some(topic_meta) = topic_meta {
                            let partition_index = topic
                                .partitions
                                .first()
                                .ok_or_else(|| anyhow!("No partitions in topic"))?
                                .partition
                                as usize;
                            // assume that all partitions in this topic have the same routing requirements
                            if let Some(partition) = topic_meta.partitions.get(partition_index) {
                                self.nodes
                                    .iter_mut()
                                    .filter(|node| {
                                        partition.replica_nodes.contains(&node.broker_id)
                                    })
                                    .choose(&mut self.rng)
                                    .unwrap()
                            } else {
                                let partition_len = topic_meta.partitions.len();
                                tracing::warn!("no known partition replica for {topic_name:?} at partition index {partition_index} out of {partition_len} partitions, routing message to a random node so that a NOT_LEADER_OR_FOLLOWER or similar error is returned to the client");
                                self.nodes.choose_mut(&mut self.rng).unwrap()
                            }
                        } else {
                            tracing::warn!("no known partition replica for {topic_name:?}, routing message to a random node so that a NOT_LEADER_OR_FOLLOWER or similar error is returned to the client");
                            self.nodes.choose_mut(&mut self.rng).unwrap()
                        };
                        self.fetch_request_destinations
                            .insert(message.id(), node.broker_id);
                        node
                    } else {
                        // route via session id
                        if let Some(destination) =
                            self.fetch_session_id_to_broker.get(&fetch.session_id)
                        {
                            self.nodes
                                .iter_mut()
                                .find(|x| &x.broker_id == destination)
                                .unwrap()
                        } else {
                            todo!()
                        }
                    };
                    let connection = node.get_connection(&self.connection_factory).await?.clone();

                    let (tx, rx) = oneshot::channel();
                    connection
                        .send(Request {
                            message,
                            return_chan: Some(tx),
                        })
                        .map_err(|_| anyhow!("Failed to send"))?;
                    results.push(rx);
                }

                // route to group coordinator
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Heartbeat(heartbeat),
                    ..
                })) => {
                    let group_id = heartbeat.group_id.clone();
                    results.push(self.route_to_coordinator(message, group_id).await?);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::SyncGroup(sync_group),
                    ..
                })) => {
                    let group_id = sync_group.group_id.clone();
                    results.push(self.route_to_coordinator(message, group_id).await?);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::OffsetFetch(offset_fetch),
                    header,
                })) => {
                    let group_id = if header.request_api_version <= 7 {
                        offset_fetch.group_id.clone()
                    } else {
                        // This is possibly dangerous.
                        // The client could construct a message which is valid for a specific shotover node, but not for any single kafka broker.
                        // We may need to add some logic to split the request into multiple messages going to different destinations,
                        // and then reconstruct the response back into a single response
                        //
                        // For now just pick the first group as that is sufficient for the simple cases.
                        offset_fetch.groups.first().unwrap().group_id.clone()
                    };
                    results.push(self.route_to_coordinator(message, group_id).await?);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::JoinGroup(join_group),
                    ..
                })) => {
                    let group_id = join_group.group_id.clone();
                    results.push(self.route_to_coordinator(message, group_id).await?);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::DeleteGroups(groups),
                    ..
                })) => {
                    let group_id = groups.groups_names.first().unwrap().clone();
                    results.push(self.route_to_coordinator(message, group_id).await?);
                }

                // route to controller broker
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::CreateTopics(_),
                    ..
                })) => results.push(self.route_to_controller(message).await?),

                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::ApiVersions(_),
                    ..
                })) => {
                    let (tx, rx) = oneshot::channel();
                    self.route_to_first_contact_node(message.clone(), Some(tx))
                        .await?;

                    results.push(rx);
                }

                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::SaslHandshake(_),
                    ..
                })) => {
                    let (tx, rx) = oneshot::channel();
                    self.route_to_first_contact_node(message.clone(), Some(tx))
                        .await?;

                    self.connection_factory
                        .add_handshake_message(message.clone());

                    results.push(rx);
                }

                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::SaslAuthenticate(_),
                    ..
                })) => {
                    let (tx, rx) = oneshot::channel();
                    self.route_to_first_contact_node(message.clone(), Some(tx))
                        .await?;
                    self.connection_factory.add_auth_message(message.clone());
                    results.push(rx);
                    self.sasl_status.set_handshake_complete();
                }

                // route to random node
                _ => {
                    let connection = self
                        .nodes
                        .choose_mut(&mut self.rng)
                        .unwrap()
                        .get_connection(&self.connection_factory)
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

    async fn route_to_first_contact_node(
        &mut self,
        message: Message,
        return_chan: Option<oneshot::Sender<Response>>,
    ) -> Result<()> {
        let node = if let Some(first_contact_node) = self.first_contact_node {
            self.nodes
                .iter_mut()
                .find(|node| node.broker_id == first_contact_node)
                .unwrap()
        } else {
            let node = self.nodes.get_mut(0).unwrap();
            self.first_contact_node = Some(node.broker_id);
            node
        };

        node.get_connection(&self.connection_factory)
            .await?
            .send(Request {
                message,
                return_chan,
            })
            .map_err(|_| anyhow!("Failed to send"))?;

        Ok(())
    }

    async fn find_coordinator_of_group(
        &mut self,
        group: GroupId,
    ) -> Result<KafkaNode, FindCoordinatorError> {
        let request = Message::from_frame(Frame::Kafka(KafkaFrame::Request {
            header: RequestHeader::builder()
                .request_api_key(ApiKey::FindCoordinatorKey as i16)
                .request_api_version(2)
                .correlation_id(0)
                .build()
                .unwrap(),
            body: RequestBody::FindCoordinator(
                FindCoordinatorRequest::builder()
                    .key_type(0)
                    .key(group.0)
                    .build()
                    .unwrap(),
            ),
        }));

        let connection = self
            .nodes
            .choose_mut(&mut self.rng)
            .unwrap()
            .get_connection(&self.connection_factory)
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
                if coordinator.error_code == 0 {
                    Ok(KafkaNode::new(
                        coordinator.node_id,
                        KafkaAddress::new(coordinator.host.clone(), coordinator.port),
                        None,
                    ))
                } else {
                    Err(FindCoordinatorError::CoordinatorNotAvailable)
                }
            }
            other => Err(anyhow!(
                "Unexpected message returned to findcoordinator request {other:?}"
            ))?,
        }
    }

    async fn get_metadata_of_topics(&mut self, topics: Vec<TopicName>) -> Result<Message> {
        let request = Message::from_frame(Frame::Kafka(KafkaFrame::Request {
            header: RequestHeader::builder()
                .request_api_key(ApiKey::MetadataKey as i16)
                .request_api_version(4)
                .correlation_id(0)
                .build()
                .unwrap(),
            body: RequestBody::Metadata(
                MetadataRequest::builder()
                    .topics(Some(
                        topics
                            .into_iter()
                            .map(|name| {
                                MetadataRequestTopic::builder()
                                    .name(Some(name))
                                    .build()
                                    .unwrap()
                            })
                            .collect(),
                    ))
                    .build()
                    .unwrap(),
            ),
        }));

        let connection = self
            .nodes
            .choose_mut(&mut self.rng)
            .unwrap()
            .get_connection(&self.connection_factory)
            .await?;
        let (tx, rx) = oneshot::channel();
        connection
            .send(Request {
                message: request,
                return_chan: Some(tx),
            })
            .map_err(|_| anyhow!("Failed to send"))?;
        Ok(rx.await.unwrap().response.unwrap())
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

        for (i, response) in responses.iter_mut().enumerate() {
            let request_id = response.request_id();
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

                    self.process_find_coordinator_response(*version, request, find_coordinator);
                    self.rewrite_find_coordinator_response(*version, find_coordinator);
                    response.invalidate_cache();
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Metadata(metadata),
                    ..
                })) => {
                    self.process_metadata_response(metadata).await;
                    self.rewrite_metadata_response(metadata)?;
                    response.invalidate_cache();
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Fetch(fetch),
                    ..
                })) => {
                    if let Some(destination) =
                        self.fetch_request_destinations.remove(&request_id.unwrap())
                    {
                        self.fetch_session_id_to_broker
                            .insert(fetch.session_id, destination);
                    }
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::DescribeCluster(_),
                    ..
                })) => {
                    // If clients were to send this we would need to rewrite the broker information.
                    // However I dont think clients actually send this, so just error to ensure we dont break invariants.
                    return Err(anyhow!(
                        "I think this is a raft specific message and never sent by clients"
                    ));
                }
                _ => {}
            }
        }

        Ok(responses)
    }

    async fn route_to_controller(
        &mut self,
        message: Message,
    ) -> Result<oneshot::Receiver<Response>> {
        let broker_id = self.controller_broker.get().unwrap();

        let connection = if let Some(node) =
            self.nodes.iter_mut().find(|x| x.broker_id == *broker_id)
        {
            node.get_connection(&self.connection_factory).await?.clone()
        } else {
            tracing::warn!("no known broker with id {broker_id:?}, routing message to a random node so that a NOT_CONTROLLER or similar error is returned to the client");
            self.nodes
                .choose_mut(&mut self.rng)
                .unwrap()
                .get_connection(&self.connection_factory)
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
        Ok(rx)
    }

    async fn route_to_coordinator(
        &mut self,
        message: Message,
        group_id: GroupId,
    ) -> Result<oneshot::Receiver<Response>> {
        let mut connection = None;
        for node in &mut self.nodes {
            if let Some(broker_id) = self.group_to_coordinator_broker.get(&group_id) {
                if node.broker_id == *broker_id {
                    connection = Some(node.get_connection(&self.connection_factory).await?.clone());
                    break;
                }
            }
        }
        let connection = match connection {
            Some(connection) => connection,
            None => {
                tracing::warn!("no known coordinator for {group_id:?}, routing message to a random node so that a NOT_COORDINATOR or similar error is returned to the client");
                self.nodes
                    .choose_mut(&mut self.rng)
                    .unwrap()
                    .get_connection(&self.connection_factory)
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

    async fn process_metadata_response(&mut self, metadata: &MetadataResponse) {
        for (id, broker) in &metadata.brokers {
            let node = KafkaNode::new(
                *id,
                KafkaAddress::new(broker.host.clone(), broker.port),
                broker.rack.clone(),
            );
            self.add_node_if_new(node).await;
        }

        self.controller_broker.set(metadata.controller_id);

        for topic in &metadata.topics {
            let mut partitions: Vec<_> = topic
                .1
                .partitions
                .iter()
                .map(|partition| Partition {
                    index: partition.partition_index,
                    leader_id: *partition.leader_id,
                    replica_nodes: partition.replica_nodes.iter().map(|x| x.0).collect(),
                })
                .collect();
            partitions.sort_by_key(|x| x.index);
            if !topic.0.is_empty() {
                self.topic_by_name.insert(
                    topic.0.clone(),
                    Topic {
                        partitions: partitions.clone(),
                    },
                );
            }
            if !topic.1.topic_id.is_nil() {
                self.topic_by_id
                    .insert(topic.1.topic_id, Topic { partitions });
            }
        }
    }

    fn process_find_coordinator_response(
        &mut self,
        version: i16,
        request: &FindCoordinator,
        find_coordinator: &FindCoordinatorResponse,
    ) {
        if request.key_type == 0 {
            if version <= 3 {
                if find_coordinator.error_code == 0 {
                    self.group_to_coordinator_broker
                        .insert(GroupId(request.key.clone()), find_coordinator.node_id);
                }
            } else {
                for coordinator in &find_coordinator.coordinators {
                    if coordinator.error_code == 0 {
                        self.group_to_coordinator_broker
                            .insert(GroupId(coordinator.key.clone()), coordinator.node_id);
                    }
                }
            }
        }
    }

    fn rewrite_find_coordinator_response(
        &self,
        version: i16,
        find_coordinator: &mut FindCoordinatorResponse,
    ) {
        if version <= 3 {
            // for version <= 3 we need to make do with only one coordinator.
            // so we just pick the first shotover node in the rack of the coordinator.

            // skip rewriting on error
            if find_coordinator.error_code == 0 {
                let coordinator_rack = &self
                    .nodes
                    .iter()
                    .find(|x| x.broker_id == find_coordinator.node_id)
                    .unwrap()
                    .rack
                    .as_ref();
                let shotover_node = self
                    .shotover_nodes
                    .iter()
                    .find(|shotover_node| {
                        coordinator_rack
                            .map(|rack| rack == &shotover_node.rack)
                            .unwrap_or(true)
                    })
                    .unwrap();
                find_coordinator.host = shotover_node.address.host.clone();
                find_coordinator.port = shotover_node.address.port;
                find_coordinator.node_id = shotover_node.broker_id;
            }
        } else {
            // for version > 3 we can include as many coordinators as we want.
            // so we include all shotover nodes in the rack of the coordinator.
            let mut shotover_coordinators: Vec<Coordinator> = vec![];
            for coordinator in find_coordinator.coordinators.drain(..) {
                if coordinator.error_code == 0 {
                    let coordinator_rack = &self
                        .nodes
                        .iter()
                        .find(|x| x.broker_id == coordinator.node_id)
                        .unwrap()
                        .rack
                        .as_ref();
                    for shotover_node in self.shotover_nodes.iter().filter(|shotover_node| {
                        coordinator_rack
                            .map(|rack| rack == &shotover_node.rack)
                            .unwrap_or(true)
                    }) {
                        if !shotover_coordinators
                            .iter()
                            .any(|x| x.node_id == shotover_node.broker_id)
                        {
                            shotover_coordinators.push(
                                Coordinator::builder()
                                    .node_id(shotover_node.broker_id)
                                    .host(shotover_node.address.host.clone())
                                    .port(shotover_node.address.port)
                                    .build()
                                    .unwrap(),
                            );
                        }
                    }
                } else {
                    // pass errors through untouched
                    shotover_coordinators.push(coordinator)
                }
            }
            find_coordinator.coordinators = shotover_coordinators;
        }
    }

    /// Rewrite metadata response to appear as if the shotover cluster is the real cluster and the real kafka brokers do not exist
    fn rewrite_metadata_response(&self, metadata: &mut MetadataResponse) -> Result<()> {
        // Overwrite list of brokers with the list of shotover nodes
        metadata.brokers = self
            .shotover_nodes
            .iter()
            .map(|shotover_node| {
                (
                    shotover_node.broker_id,
                    MetadataResponseBroker::builder()
                        .host(shotover_node.address.host.clone())
                        .port(shotover_node.address.port)
                        .rack(Some(shotover_node.rack.clone()))
                        .build()
                        .unwrap(),
                )
            })
            .collect();

        // Overwrite the list of partitions to point at all shotover nodes within the same rack
        for (_, topic) in &mut metadata.topics {
            for partition in &mut topic.partitions {
                // Deterministically choose a single shotover node in the rack as leader based on topic + partition id
                let leader_rack = self
                    .nodes
                    .iter()
                    .find(|x| x.broker_id == *partition.leader_id)
                    .map(|x| x.rack.clone())
                    .unwrap();
                let shotover_nodes_in_rack: Vec<_> = self
                    .shotover_nodes
                    .iter()
                    .filter(|shotover_node| {
                        leader_rack
                            .as_ref()
                            .map(|rack| rack == &shotover_node.rack)
                            .unwrap_or(true)
                    })
                    .collect();
                let hash = hash_partition(topic.topic_id, partition.partition_index);
                let shotover_node = &shotover_nodes_in_rack[hash % shotover_nodes_in_rack.len()];
                partition.leader_id = shotover_node.broker_id;

                // Every replica node has its entire corresponding shotover rack included.
                // Since we can set as many replica nodes as we like, we take this all out approach.
                // This ensures that:
                // * metadata is deterministic and therefore the same on all shotover nodes
                // * clients evenly distribute their queries across shotover nodes
                let mut shotover_replica_nodes = vec![];
                for replica_node in &partition.replica_nodes {
                    let rack = self
                        .nodes
                        .iter()
                        .find(|x| x.broker_id == *replica_node)
                        .map(|x| x.rack.clone())
                        .unwrap();
                    for shotover_node in &self.shotover_nodes {
                        // If broker has no rack - use all shotover nodes
                        // If broker has rack - use all shotover nodes with the same rack
                        if rack
                            .as_ref()
                            .map(|rack| rack == &shotover_node.rack)
                            .unwrap_or(true)
                            && !shotover_replica_nodes.contains(&shotover_node.broker_id)
                        {
                            shotover_replica_nodes.push(shotover_node.broker_id);
                        }
                    }
                }
                partition.replica_nodes = shotover_replica_nodes;
            }
        }

        if let Some(controller_node) = self
            .nodes
            .iter()
            .find(|node| node.broker_id == metadata.controller_id)
        {
            // If broker has no rack - use the first shotover node
            // If broker has rack - use the first shotover node with the same rack
            // This is deterministic because the list of shotover nodes is sorted.
            if let Some(shotover_node) = self.shotover_nodes.iter().find(|shotover_node| {
                controller_node
                    .rack
                    .as_ref()
                    .map(|rack| rack == &shotover_node.rack)
                    .unwrap_or(true)
            }) {
                metadata.controller_id = shotover_node.broker_id;
            } else {
                tracing::warn!(
                    "No shotover node configured to handle kafka rack {:?}",
                    controller_node.rack
                );
            }
        } else {
            return Err(anyhow!(
                "Invalid metadata, controller points at unknown node {:?}",
                metadata.controller_id
            ));
        }

        Ok(())
    }

    async fn add_node_if_new(&mut self, new_node: KafkaNode) {
        let new = self
            .nodes_shared
            .read()
            .await
            .iter()
            .all(|node| node.broker_id != new_node.broker_id);
        if new {
            self.nodes_shared.write().await.push(new_node);

            self.update_local_nodes().await;
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

fn hash_partition(topic_id: Uuid, partition_index: i32) -> usize {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.write(topic_id.as_bytes());
    hasher.write(&partition_index.to_be_bytes());
    hasher.finish() as usize
}

#[derive(Debug)]
struct Topic {
    partitions: Vec<Partition>,
}

#[derive(Debug, Clone)]
struct Partition {
    index: i32,
    leader_id: i32,
    replica_nodes: Vec<i32>,
}

struct FindCoordinator {
    index: usize,
    key: StrBytes,
    key_type: i8,
}
