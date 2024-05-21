use crate::connection::SinkConnection;
use crate::frame::kafka::{KafkaFrame, RequestBody, ResponseBody};
use crate::frame::{Frame, MessageType};
use crate::message::{Message, MessageIdMap, Messages};
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::{
    DownChainProtocol, Transform, TransformBuilder, TransformContextBuilder, UpChainProtocol,
    Wrapper,
};
use crate::transforms::{TransformConfig, TransformContextConfig};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use kafka_protocol::messages::fetch_request::FetchTopic;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchRequest, FindCoordinatorRequest, FindCoordinatorResponse, GroupId,
    HeartbeatRequest, JoinGroupRequest, MetadataRequest, MetadataResponse, OffsetFetchRequest,
    RequestHeader, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SyncGroupRequest, TopicName,
};
use kafka_protocol::protocol::{Builder, StrBytes};
use kafka_protocol::ResponseError;
use node::{ConnectionFactory, KafkaAddress, KafkaNode};
use rand::rngs::SmallRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::SeedableRng;
use scram_over_mtls::{
    AuthorizeScramOverMtls, AuthorizeScramOverMtlsBuilder, AuthorizeScramOverMtlsConfig,
    OriginalScramState,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::hash::Hasher;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

mod node;
mod scram_over_mtls;

const SASL_SCRAM_MECHANISMS: [&str; 2] = ["SCRAM-SHA-256", "SCRAM-SHA-512"];

#[derive(thiserror::Error, Debug)]
enum FindCoordinatorError {
    #[error("Coordinator not available")]
    CoordinatorNotAvailable,
    #[error("{0:?}")]
    Unrecoverable(#[from] anyhow::Error),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct KafkaSinkClusterConfig {
    pub first_contact_points: Vec<String>,
    pub shotover_nodes: Vec<ShotoverNodeConfig>,
    pub connect_timeout_ms: u64,
    pub read_timeout: Option<u64>,
    pub tls: Option<TlsConnectorConfig>,
    pub authorize_scram_over_mtls: Option<AuthorizeScramOverMtlsConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct ShotoverNodeConfig {
    pub address: String,
    pub rack: String,
    pub broker_id: i32,
}

impl ShotoverNodeConfig {
    fn build(self) -> Result<ShotoverNode> {
        Ok(ShotoverNode {
            address: KafkaAddress::from_str(&self.address)?,
            rack: StrBytes::from_string(self.rack),
            broker_id: BrokerId(self.broker_id),
        })
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

        let shotover_nodes: Result<Vec<_>> = self
            .shotover_nodes
            .iter()
            .cloned()
            .map(ShotoverNodeConfig::build)
            .collect();
        let mut shotover_nodes = shotover_nodes?;
        shotover_nodes.sort_by_key(|x| x.broker_id);

        Ok(Box::new(KafkaSinkClusterBuilder::new(
            self.first_contact_points.clone(),
            &self.authorize_scram_over_mtls,
            shotover_nodes,
            transform_context.chain_name,
            self.connect_timeout_ms,
            self.read_timeout,
            tls,
        )?))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::MustBeOneOf(vec![MessageType::Kafka])
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::Terminating
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
    authorize_scram_over_mtls: Option<AuthorizeScramOverMtlsBuilder>,

    tls: Option<TlsConnector>,
}

impl KafkaSinkClusterBuilder {
    pub fn new(
        first_contact_points: Vec<String>,
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtlsConfig>,
        shotover_nodes: Vec<ShotoverNode>,
        _chain_name: String,
        connect_timeout_ms: u64,
        timeout: Option<u64>,
        tls: Option<TlsConnector>,
    ) -> Result<KafkaSinkClusterBuilder> {
        let read_timeout = timeout.map(Duration::from_secs);
        let connect_timeout = Duration::from_millis(connect_timeout_ms);

        Ok(KafkaSinkClusterBuilder {
            first_contact_points,
            authorize_scram_over_mtls: authorize_scram_over_mtls
                .as_ref()
                .map(|x| x.get_builder(connect_timeout, read_timeout))
                .transpose()?,
            shotover_nodes,
            connect_timeout,
            read_timeout,
            controller_broker: Arc::new(AtomicBrokerId::new()),
            group_to_coordinator_broker: Arc::new(DashMap::new()),
            topic_by_name: Arc::new(DashMap::new()),
            topic_by_id: Arc::new(DashMap::new()),
            nodes_shared: Arc::new(RwLock::new(vec![])),
            tls,
        })
    }
}

impl TransformBuilder for KafkaSinkClusterBuilder {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(KafkaSinkCluster {
            first_contact_points: self.first_contact_points.clone(),
            shotover_nodes: self.shotover_nodes.clone(),
            nodes: vec![],
            nodes_shared: self.nodes_shared.clone(),
            controller_broker: self.controller_broker.clone(),
            group_to_coordinator_broker: self.group_to_coordinator_broker.clone(),
            topic_by_name: self.topic_by_name.clone(),
            topic_by_id: self.topic_by_id.clone(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            auth_complete: false,
            connection_factory: ConnectionFactory::new(
                self.tls.clone(),
                self.connect_timeout,
                self.read_timeout,
                transform_context.force_run_chain,
            ),
            first_contact_node: None,
            control_connection: None,
            pending_requests: Default::default(),
            find_coordinator_requests: Default::default(),
            temp_responses_buffer: Default::default(),
            sasl_mechanism: None,
            authorize_scram_over_mtls: self.authorize_scram_over_mtls.as_ref().map(|x| x.build()),
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

pub struct KafkaSinkCluster {
    first_contact_points: Vec<String>,
    shotover_nodes: Vec<ShotoverNode>,
    nodes: Vec<KafkaNode>,
    nodes_shared: Arc<RwLock<Vec<KafkaNode>>>,
    controller_broker: Arc<AtomicBrokerId>,
    group_to_coordinator_broker: Arc<DashMap<GroupId, BrokerId>>,
    topic_by_name: Arc<DashMap<TopicName, Topic>>,
    topic_by_id: Arc<DashMap<Uuid, Topic>>,
    rng: SmallRng,
    auth_complete: bool,
    connection_factory: ConnectionFactory,
    first_contact_node: Option<KafkaAddress>,
    control_connection: Option<SinkConnection>,
    /// Maintains the state of each request/response pair.
    /// Ordering must be maintained to ensure responses match up with their request.
    pending_requests: VecDeque<PendingRequest>,
    find_coordinator_requests: MessageIdMap<FindCoordinator>,
    /// A temporary buffer used when receiving responses, only held onto in order to avoid reallocating.
    temp_responses_buffer: Vec<Message>,
    sasl_mechanism: Option<String>,
    authorize_scram_over_mtls: Option<AuthorizeScramOverMtls>,
}

/// State of a Request/Response is maintained by this enum.
/// The state progresses from Routed -> Sent -> Received
#[derive(Debug)]
enum PendingRequestTy {
    /// A route has been determined for this request but it has not yet been sent.
    Routed {
        destination: BrokerId,
        request: Message,
    },
    /// The request has been sent to the specified broker and we are now awaiting a response from that broker.
    Sent {
        destination: BrokerId,
        /// How many responses must be received before this respose is received.
        /// When this is 0 the next response from the broker will be for this request.
        /// This field must be manually decremented when another response for this broker comes through.
        index: usize,
    },
    /// The broker has returned a Response to this request.
    /// Returning this response may be delayed until a response to an earlier request comes back from another broker.
    Received { response: Message },
}

struct PendingRequest {
    ty: PendingRequestTy,
    /// Combine the next N responses into a single response
    /// This message should be considered the base message and will retain the shotover Message::id and kafka correlation_id
    combine_responses: usize,
}

#[async_trait]
impl Transform for KafkaSinkCluster {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
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

        let mut responses = if requests_wrapper.requests.is_empty() {
            // there are no requests, so no point sending any, but we should check for any responses without awaiting
            self.recv_responses()?
        } else {
            self.update_local_nodes().await;

            for request in &mut requests_wrapper.requests {
                let id = request.id();
                if let Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::FindCoordinator(find_coordinator),
                    ..
                })) = request.frame()
                {
                    self.find_coordinator_requests.insert(
                        id,
                        FindCoordinator {
                            key: find_coordinator.key.clone(),
                            key_type: find_coordinator.key_type,
                        },
                    );
                }
            }

            self.route_requests(requests_wrapper.requests).await?;
            self.send_requests().await?;
            self.recv_responses()?
        };

        self.process_responses(&mut responses).await?;
        Ok(responses)
    }
}

impl KafkaSinkCluster {
    /// Send a request over the control connection and immediately receive the response.
    /// Since we always await the response we know for sure that the response will not get mixed up with any other incoming responses.
    async fn control_send_receive(&mut self, requests: Message) -> Result<Message> {
        if self.control_connection.is_none() {
            let address = &self.nodes.choose(&mut self.rng).unwrap().kafka_address;
            self.control_connection = Some(
                self.connection_factory
                    .create_connection(
                        address,
                        &self.authorize_scram_over_mtls,
                        &self.sasl_mechanism,
                    )
                    .await
                    .context("Failed to create control connection")?,
            );
        }
        let connection = self.control_connection.as_mut().unwrap();
        connection.send(vec![requests])?;
        Ok(connection.recv().await?.remove(0))
    }

    fn store_topic_names(&self, topics: &mut Vec<TopicName>, topic: TopicName) {
        if self.topic_by_name.get(&topic).is_none() && !topics.contains(&topic) && !topic.is_empty()
        {
            topics.push(topic);
        }
    }

    fn store_topic_ids(&self, topics: &mut Vec<Uuid>, topic: Uuid) {
        if self.topic_by_id.get(&topic).is_none() && !topics.contains(&topic) && !topic.is_nil() {
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

    async fn route_requests(&mut self, mut requests: Vec<Message>) -> Result<()> {
        if !self.auth_complete {
            let mut handshake_request_count = 0;
            for request in &mut requests {
                match request.frame() {
                    Some(Frame::Kafka(KafkaFrame::Request {
                        body: RequestBody::SaslHandshake(SaslHandshakeRequest { mechanism, .. }),
                        ..
                    })) => {
                        mechanism.as_str();

                        self.sasl_mechanism = Some(mechanism.as_str().to_owned());
                        self.connection_factory.add_auth_request(request.clone());
                        handshake_request_count += 1;
                    }
                    Some(Frame::Kafka(KafkaFrame::Request {
                        body:
                            RequestBody::SaslAuthenticate(SaslAuthenticateRequest {
                                auth_bytes, ..
                            }),
                        ..
                    })) => {
                        if let Some(scram_over_mtls) = &mut self.authorize_scram_over_mtls {
                            if let Some(username) = get_username_from_scram_request(auth_bytes) {
                                scram_over_mtls.delegation_token = scram_over_mtls
                                    .token_task
                                    .get_token_for_user(username)
                                    .await?;
                            }
                        }
                        self.connection_factory.add_auth_request(request.clone());
                        handshake_request_count += 1;
                    }
                    Some(Frame::Kafka(KafkaFrame::Request {
                        body: RequestBody::ApiVersions(_),
                        ..
                    })) => {
                        handshake_request_count += 1;
                    }
                    _ => {
                        // The client is no longer performing authentication, so consider auth completed

                        if let Some(scram_over_mtls) = &self.authorize_scram_over_mtls {
                            // When performing SCRAM over mTLS, we need this security check to ensure that the
                            // client cannot access delegation tokens that it has not succesfully authenticated for.
                            //
                            // If the client were to send a request directly after the SCRAM requests,
                            // without waiting for responses to those scram requests first,
                            // this error would be triggered even if the SCRAM requests were succesful.
                            // However that would be a violation of the SCRAM protocol as the client is supposed to check
                            // the server's signature contained in the server's final message in order to authenticate the server.
                            // So I dont think this problem is worth solving.
                            if !matches!(
                                scram_over_mtls.original_scram_state,
                                OriginalScramState::AuthSuccess
                            ) {
                                return Err(anyhow!("Client attempted to send requests before a succesful auth was completed or after an unsuccesful auth"));
                            }
                        }

                        self.auth_complete = true;
                        break;
                    }
                }
            }
            // route all handshake messages
            for _ in 0..handshake_request_count {
                let request = requests.remove(0);
                self.route_to_first_contact_node(request.clone());
            }

            if requests.is_empty() {
                // all messages received in this batch are handshake messages,
                // so dont continue with regular message handling
                return Ok(());
            } else {
                // the later messages in this batch are not handshake messages,
                // so continue onto the regular message handling
            }
        }

        let mut topic_names = vec![];
        let mut topic_ids = vec![];
        let mut groups = vec![];
        for request in &mut requests {
            match request.frame() {
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Produce(produce),
                    ..
                })) => {
                    for (name, _) in &produce.topic_data {
                        self.store_topic_names(&mut topic_names, name.clone());
                    }
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Fetch(fetch),
                    ..
                })) => {
                    for topic in &fetch.topics {
                        self.store_topic_names(&mut topic_names, topic.topic.clone());
                        self.store_topic_ids(&mut topic_ids, topic.topic_id);
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
                    // * If its not the coordinator then it will return a NOT_COORDINATOR message to
                    //   the client prompting it to retry the whole process again.
                }
                Err(FindCoordinatorError::Unrecoverable(err)) => Err(err)?,
            }
        }

        // request and process metadata if we are missing topics or the controller broker id
        if !topic_names.is_empty()
            || !topic_ids.is_empty()
            || self.controller_broker.get().is_none()
        {
            let mut metadata = self.get_metadata_of_topics(topic_names, topic_ids).await?;
            match metadata.frame() {
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Metadata(metadata),
                    ..
                })) => {
                    for topic in metadata.topics.values() {
                        if let Some(err) = ResponseError::try_from_code(topic.error_code) {
                            return Err(anyhow!(
                                "Kafka responded to Metadata request with error {err:?}"
                            ));
                        }
                    }
                    self.process_metadata_response(metadata).await
                }
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
                    body: RequestBody::Produce(_),
                    ..
                })) => self.route_produce_request(message)?,

                // route to random partition replica
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Fetch(_),
                    ..
                })) => self.route_fetch_request(message)?,

                // route to group coordinator
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Heartbeat(heartbeat),
                    ..
                })) => {
                    let group_id = heartbeat.group_id.clone();
                    self.route_to_coordinator(message, group_id);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::SyncGroup(sync_group),
                    ..
                })) => {
                    let group_id = sync_group.group_id.clone();
                    self.route_to_coordinator(message, group_id);
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
                    self.route_to_coordinator(message, group_id);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::JoinGroup(join_group),
                    ..
                })) => {
                    let group_id = join_group.group_id.clone();
                    self.route_to_coordinator(message, group_id);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::DeleteGroups(groups),
                    ..
                })) => {
                    let group_id = groups.groups_names.first().unwrap().clone();
                    self.route_to_coordinator(message, group_id);
                }

                // route to controller broker
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::CreateTopics(_),
                    ..
                })) => self.route_to_controller(message),
                // route to random node
                _ => {
                    let destination = self.nodes.choose(&mut self.rng).unwrap().broker_id;
                    self.pending_requests.push_back(PendingRequest {
                        ty: PendingRequestTy::Routed {
                            destination,
                            request: message,
                        },
                        combine_responses: 1,
                    });
                }
            }
        }
        Ok(())
    }

    fn route_produce_request(&mut self, mut message: Message) -> Result<()> {
        if let Some(Frame::Kafka(KafkaFrame::Request {
            body: RequestBody::Produce(produce),
            ..
        })) = message.frame()
        {
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
                    .index as usize];
                for node in &mut self.nodes {
                    if node.broker_id == partition.leader_id {
                        connection = Some(node.broker_id);
                    }
                }
            }
            let destination = match connection {
                Some(connection) => connection,
                None => {
                    tracing::warn!("no known partition leader for {topic_name:?}, routing message to a random node so that a NOT_LEADER_OR_FOLLOWER or similar error is returned to the client");
                    self.nodes.choose(&mut self.rng).unwrap().broker_id
                }
            };

            self.pending_requests.push_back(PendingRequest {
                ty: PendingRequestTy::Routed {
                    destination,
                    request: message,
                },
                combine_responses: 1,
            });
        }

        Ok(())
    }

    /// This method removes all topics from the fetch request and returns them split up by their destination
    /// If any topics are unroutable they will have their BrokerId set to -1
    fn split_fetch_request_by_destination(
        &mut self,
        fetch: &mut FetchRequest,
    ) -> HashMap<BrokerId, Vec<FetchTopic>> {
        let mut result: HashMap<BrokerId, Vec<FetchTopic>> = Default::default();

        for mut topic in fetch.topics.drain(..) {
            // This way of constructing topic_meta is kind of crazy, but it works around borrow checker limitations
            // Old clients only specify the topic name and some newer clients only specify the topic id.
            // So we need to check the id first and then fallback to the name.
            let topic_by_id = self.topic_by_id.get(&topic.topic_id);
            let topic_by_name;
            let mut topic_meta = topic_by_id.as_deref();
            if topic_meta.is_none() {
                topic_by_name = self.topic_by_name.get(&topic.topic);
                topic_meta = topic_by_name.as_deref();
            }
            if let Some(topic_meta) = topic_meta {
                for partition in std::mem::take(&mut topic.partitions) {
                    let partition_index = partition.partition as usize;
                    let destination = if let Some(partition) =
                        topic_meta.partitions.get(partition_index)
                    {
                        self.nodes
                            .iter_mut()
                            .filter(|node| partition.replica_nodes.contains(&node.broker_id))
                            .choose(&mut self.rng)
                            .unwrap()
                            .broker_id
                    } else {
                        let partition_len = topic_meta.partitions.len();
                        let topic_name = Self::format_topic_name(&topic);
                        tracing::warn!("no known partition replica for {topic_name} at partition index {partition_index} out of {partition_len} partitions, routing message to a random node so that a NOT_LEADER_OR_FOLLOWER or similar error is returned to the client");
                        BrokerId(-1)
                    };
                    let dest_topics = result.entry(destination).or_default();
                    if let Some(dest_topic) = dest_topics
                        .iter_mut()
                        .find(|x| x.topic_id == topic.topic_id && x.topic == topic.topic)
                    {
                        dest_topic.partitions.push(partition);
                    } else {
                        let mut topic = topic.clone();
                        topic.partitions.push(partition);
                        dest_topics.push(topic);
                    }
                }
            } else {
                let topic_name = Self::format_topic_name(&topic);
                tracing::warn!("no known partition replica for {topic_name}, routing message to a random node so that a NOT_LEADER_OR_FOLLOWER or similar error is returned to the client");
                let destination = BrokerId(-1);
                let dest_topics = result.entry(destination).or_default();
                dest_topics.push(topic);
            }
        }

        result
    }

    fn format_topic_name(fetch_topic: &FetchTopic) -> String {
        if fetch_topic.topic.0.is_empty() {
            format!("topic with id {}", fetch_topic.topic_id)
        } else {
            format!("topic with name {:?}", fetch_topic.topic.0.as_str())
        }
    }

    fn route_fetch_request(&mut self, mut message: Message) -> Result<()> {
        if let Some(Frame::Kafka(KafkaFrame::Request {
            body: RequestBody::Fetch(fetch),
            ..
        })) = message.frame()
        {
            if fetch.session_id == 0 {
                let routing = self.split_fetch_request_by_destination(fetch);

                if routing.is_empty() {
                    // Fetch contains no topics, so we can just pick a random destination.
                    // The message is unchanged so we can just send as is.
                    let destination = self.nodes.choose(&mut self.rng).unwrap().broker_id;

                    self.pending_requests.push_back(PendingRequest {
                        ty: PendingRequestTy::Routed {
                            destination,
                            request: message,
                        },
                        combine_responses: 1,
                    });
                } else if routing.len() == 1 {
                    // Only 1 destination,
                    // so we can just reconstruct the original message as is,
                    // act like this never happened 😎,
                    // we dont even need to invalidate the message's cache.
                    let (destination, topics) = routing.into_iter().next().unwrap();
                    let destination = if destination == -1 {
                        self.nodes.choose(&mut self.rng).unwrap().broker_id
                    } else {
                        destination
                    };

                    fetch.topics = topics;
                    self.pending_requests.push_back(PendingRequest {
                        ty: PendingRequestTy::Routed {
                            destination,
                            request: message,
                        },
                        combine_responses: 1,
                    });
                } else {
                    // The message has been split so it may be delivered to multiple destinations.
                    // We must generate a unique message for each destination.
                    let combine_responses = routing.len();
                    message.invalidate_cache();
                    for (i, (destination, topics)) in routing.into_iter().enumerate() {
                        let destination = if destination == -1 {
                            self.nodes.choose(&mut self.rng).unwrap().broker_id
                        } else {
                            destination
                        };
                        let mut request = if i == 0 {
                            // First message acts as base and retains message id
                            message.clone()
                        } else {
                            message.clone_with_new_id()
                        };
                        if let Some(Frame::Kafka(KafkaFrame::Request {
                            body: RequestBody::Fetch(fetch),
                            ..
                        })) = request.frame()
                        {
                            fetch.topics = topics;
                        }
                        self.pending_requests.push_back(PendingRequest {
                            ty: PendingRequestTy::Routed {
                                destination,
                                request,
                            },
                            combine_responses,
                        });
                    }
                }
            } else {
                // route via session id
                unreachable!("Currently requests should not have session_id set since we remove it from responses. In the future we do want to handle session_id though.")
            };
        }

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

        let mut response = self.control_send_receive(request).await?;
        match response.frame() {
            Some(Frame::Kafka(KafkaFrame::Response {
                body: ResponseBody::FindCoordinator(coordinator),
                ..
            })) => match ResponseError::try_from_code(coordinator.error_code) {
                None => Ok(KafkaNode::new(
                    coordinator.node_id,
                    KafkaAddress::new(coordinator.host.clone(), coordinator.port),
                    None,
                )),
                Some(ResponseError::CoordinatorNotAvailable) => {
                    Err(FindCoordinatorError::CoordinatorNotAvailable)
                }
                Some(err) => Err(FindCoordinatorError::Unrecoverable(anyhow!(
                    "Unexpected server error from FindCoordinator {err}"
                ))),
            },
            other => Err(anyhow!(
                "Unexpected message returned to findcoordinator request {other:?}"
            ))?,
        }
    }

    async fn get_metadata_of_topics(
        &mut self,
        topic_names: Vec<TopicName>,
        topic_ids: Vec<Uuid>,
    ) -> Result<Message> {
        let api_version = if topic_ids.is_empty() { 4 } else { 12 };
        let request = Message::from_frame(Frame::Kafka(KafkaFrame::Request {
            header: RequestHeader::builder()
                .request_api_key(ApiKey::MetadataKey as i16)
                .request_api_version(api_version)
                .correlation_id(0)
                .build()
                .unwrap(),
            body: RequestBody::Metadata(
                MetadataRequest::builder()
                    .topics(Some(
                        topic_names
                            .into_iter()
                            .map(|name| {
                                MetadataRequestTopic::builder()
                                    .name(Some(name))
                                    .build()
                                    .unwrap()
                            })
                            .chain(topic_ids.into_iter().map(|id| {
                                MetadataRequestTopic::builder()
                                    .name(None)
                                    .topic_id(id)
                                    .build()
                                    .unwrap()
                            }))
                            .collect(),
                    ))
                    .build()
                    .unwrap(),
            ),
        }));

        self.control_send_receive(request).await
    }

    /// Convert all PendingRequestTy::Routed into PendingRequestTy::Sent
    async fn send_requests(&mut self) -> Result<()> {
        struct RoutedRequests {
            requests: Vec<Message>,
            already_pending: usize,
        }

        let mut broker_to_routed_requests: HashMap<BrokerId, RoutedRequests> = HashMap::new();
        for i in 0..self.pending_requests.len() {
            if let PendingRequestTy::Routed { destination, .. } = &self.pending_requests[i].ty {
                let routed_requests = broker_to_routed_requests
                    .entry(*destination)
                    .or_insert_with(|| RoutedRequests {
                        requests: vec![],
                        already_pending: self
                            .pending_requests
                            .iter()
                            .filter(|pending_request| {
                                if let PendingRequestTy::Sent {
                                    destination: check_destination,
                                    ..
                                } = &pending_request.ty
                                {
                                    check_destination == destination
                                } else {
                                    false
                                }
                            })
                            .count(),
                    });
                let mut value = PendingRequestTy::Sent {
                    destination: *destination,
                    index: routed_requests.requests.len() + routed_requests.already_pending,
                };
                std::mem::swap(&mut self.pending_requests[i].ty, &mut value);
                if let PendingRequestTy::Routed { request, .. } = value {
                    routed_requests.requests.push(request);
                }
            }
        }

        for (destination, requests) in broker_to_routed_requests {
            self.nodes
                .iter_mut()
                .find(|x| x.broker_id == destination)
                .unwrap()
                .get_connection(
                    &self.connection_factory,
                    &self.authorize_scram_over_mtls,
                    &self.sasl_mechanism,
                )
                .await?
                .send(requests.requests)?;
        }

        Ok(())
    }

    /// Convert some PendingRequestTy::Sent into PendingRequestTy::Received
    fn recv_responses(&mut self) -> Result<Vec<Message>> {
        for node in &mut self.nodes {
            if let Some(connection) = node.get_connection_if_open() {
                self.temp_responses_buffer.clear();
                if let Ok(()) = connection.try_recv_into(&mut self.temp_responses_buffer) {
                    for response in self.temp_responses_buffer.drain(..) {
                        let mut response = Some(response);
                        for pending_request in &mut self.pending_requests {
                            if let PendingRequestTy::Sent { destination, index } =
                                &mut pending_request.ty
                            {
                                if *destination == node.broker_id {
                                    if *index == 0 {
                                        pending_request.ty = PendingRequestTy::Received {
                                            response: response.take().unwrap(),
                                        };
                                    } else {
                                        *index -= 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut responses = vec![];
        while let Some(pending_request) = self.pending_requests.front() {
            let all_combined_received = (0..pending_request.combine_responses).all(|i| {
                matches!(
                    self.pending_requests.get(i),
                    Some(PendingRequest {
                        ty: PendingRequestTy::Received { .. },
                        ..
                    })
                )
            });
            if all_combined_received {
                // The next response we are waiting on has been received, add it to responses
                if pending_request.combine_responses == 1 {
                    if let Some(PendingRequest {
                        ty: PendingRequestTy::Received { response },
                        ..
                    }) = self.pending_requests.pop_front()
                    {
                        responses.push(response);
                    }
                } else {
                    let drain = self
                        .pending_requests
                        .drain(..pending_request.combine_responses)
                        .map(|x| {
                            if let PendingRequest {
                                ty: PendingRequestTy::Received { response },
                                ..
                            } = x
                            {
                                response
                            } else {
                                unreachable!("Guaranteed by all_combined_received")
                            }
                        });
                    responses.push(Self::combine_fetch_responses(drain)?);
                }
            } else {
                // The pending_request is not received, we need to break to maintain response ordering.
                break;
            }
        }
        Ok(responses)
    }

    fn combine_fetch_responses(mut drain: impl Iterator<Item = Message>) -> Result<Message> {
        // Take this response as base.
        // Then iterate over all remaining combined responses and integrate them into the base.
        let mut base = drain.next().unwrap();
        if let Some(Frame::Kafka(KafkaFrame::Response {
            body: ResponseBody::Fetch(base_fetch),
            ..
        })) = base.frame()
        {
            for mut next in drain {
                if let Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Fetch(next_fetch),
                    ..
                })) = next.frame()
                {
                    for next_response in std::mem::take(&mut next_fetch.responses) {
                        if let Some(base_response) =
                            base_fetch.responses.iter_mut().find(|response| {
                                response.topic == next_response.topic
                                    && response.topic_id == next_response.topic_id
                            })
                        {
                            for next_partition in &next_response.partitions {
                                for base_partition in &base_response.partitions {
                                    if next_partition.partition_index
                                        == base_partition.partition_index
                                    {
                                        tracing::warn!("Duplicate partition indexes in combined fetch response, if this ever occurs we should investigate the repercussions")
                                    }
                                }
                            }
                            // A partition can only be contained in one response so there is no risk of duplicating partitions
                            base_response.partitions.extend(next_response.partitions)
                        } else {
                            base_fetch.responses.push(next_response);
                        }
                    }
                } else {
                    return Err(anyhow!(
                        "Combining Fetch messages but received another message type"
                    ));
                }
            }
        } else {
            return Err(anyhow!(
                "Combining of message types other than Fetch is currently unsupported"
            ));
        }

        base.invalidate_cache();
        Ok(base)
    }

    async fn process_responses(&mut self, responses: &mut [Message]) -> Result<()> {
        // TODO: Handle errors like NOT_COORDINATOR by removing element from self.topics and self.coordinator_broker_id
        for response in responses.iter_mut() {
            let request_id = response.request_id().unwrap();
            match response.frame() {
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::FindCoordinator(find_coordinator),
                    version,
                    ..
                })) => {
                    let request = self
                        .find_coordinator_requests
                        .remove(&request_id)
                        .ok_or_else(|| anyhow!("Received find_coordinator but not requested"))?;

                    self.process_find_coordinator_response(*version, request, find_coordinator);
                    self.rewrite_find_coordinator_response(*version, find_coordinator);
                    response.invalidate_cache();
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::SaslHandshake(handshake),
                    ..
                })) => {
                    // If authorize_scram_over_mtls is disabled there is no way that scram can work through KafkaSinkCluster
                    // since it is specifically designed such that replay attacks wont work.
                    // So when authorize_scram_over_mtls is disabled report to the user that SCRAM is not enabled.
                    if self.authorize_scram_over_mtls.is_none() {
                        // remove scram from supported mechanisms
                        handshake
                            .mechanisms
                            .retain(|x| !SASL_SCRAM_MECHANISMS.contains(&x.as_str()));

                        // declare unsupported if the client requested SCRAM
                        if let Some(sasl_mechanism) = &self.sasl_mechanism {
                            if SASL_SCRAM_MECHANISMS.contains(&sasl_mechanism.as_str()) {
                                handshake.error_code =
                                    ResponseError::UnsupportedSaslMechanism.code();
                            }
                        }

                        response.invalidate_cache();
                    }
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::SaslAuthenticate(authenticate),
                    ..
                })) => {
                    self.process_sasl_authenticate(authenticate).await?;
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
                    fetch.session_id = 0;
                    response.invalidate_cache();
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

        Ok(())
    }

    async fn process_sasl_authenticate(
        &mut self,
        authenticate: &mut SaslAuthenticateResponse,
    ) -> Result<()> {
        if let Some(sasl_mechanism) = &self.sasl_mechanism {
            if SASL_SCRAM_MECHANISMS.contains(&sasl_mechanism.as_str()) {
                if let Some(scram_over_mtls) = &mut self.authorize_scram_over_mtls {
                    match scram_over_mtls.original_scram_state {
                        OriginalScramState::WaitingOnServerFirst => {
                            scram_over_mtls.original_scram_state = if authenticate.error_code == 0 {
                                OriginalScramState::WaitingOnServerFinal
                            } else {
                                OriginalScramState::AuthFailed
                            };
                        }
                        OriginalScramState::WaitingOnServerFinal => {
                            scram_over_mtls.original_scram_state = if authenticate.error_code == 0 {
                                OriginalScramState::AuthSuccess
                            } else {
                                OriginalScramState::AuthFailed
                            };
                        }
                        OriginalScramState::AuthSuccess | OriginalScramState::AuthFailed => {
                            return Err(anyhow!(
                                "SCRAM protocol does not allow a third sasl response"
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn route_to_first_contact_node(&mut self, request: Message) {
        let destination = if let Some(first_contact_node) = &self.first_contact_node {
            self.nodes
                .iter_mut()
                .find(|node| node.kafka_address == *first_contact_node)
                .unwrap()
                .broker_id
        } else {
            let node = self.nodes.get_mut(0).unwrap();
            self.first_contact_node = Some(node.kafka_address.clone());
            node.broker_id
        };

        self.pending_requests.push_back(PendingRequest {
            ty: PendingRequestTy::Routed {
                destination,
                request,
            },
            combine_responses: 1,
        });
    }

    fn route_to_controller(&mut self, request: Message) {
        let broker_id = self.controller_broker.get().unwrap();

        let destination = if let Some(node) =
            self.nodes.iter_mut().find(|x| x.broker_id == *broker_id)
        {
            node.broker_id
        } else {
            tracing::warn!("no known broker with id {broker_id:?}, routing message to a random node so that a NOT_CONTROLLER or similar error is returned to the client");
            self.nodes.choose(&mut self.rng).unwrap().broker_id
        };

        self.pending_requests.push_back(PendingRequest {
            ty: PendingRequestTy::Routed {
                destination,
                request,
            },
            combine_responses: 1,
        });
    }

    fn route_to_coordinator(&mut self, request: Message, group_id: GroupId) {
        let destination = self.group_to_coordinator_broker.get(&group_id);
        let destination = match destination {
            Some(destination) => *destination,
            None => {
                tracing::warn!("no known coordinator for {group_id:?}, routing message to a random node so that a NOT_COORDINATOR or similar error is returned to the client");
                self.nodes.choose(&mut self.rng).unwrap().broker_id
            }
        };
        self.pending_requests.push_back(PendingRequest {
            ty: PendingRequestTy::Routed {
                destination,
                request,
            },
            combine_responses: 1,
        });
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
        request: FindCoordinator,
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
            // For version <= 3 we only have one coordinator to replace,
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
            // For version > 3 we have to replace multiple coordinators.
            // It may be tempting to include all shotover nodes in the rack of the coordinator, assuming the client will load balance between them.
            // However it doesnt work like that.
            // AFAIK there can only be one coordinator per unique `FindCoordinatorResponse::key`.
            // In fact the java driver doesnt even support multiple coordinators of different types yet:
            // https://github.com/apache/kafka/blob/4825c89d14e5f1b2da7e1f48dac97888602028d7/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java#L921
            //
            // So, just like with version <= 3, we just pick the first shotover node in the rack of the coordinator for each coordinator.
            for coordinator in &mut find_coordinator.coordinators {
                // skip rewriting on error
                if coordinator.error_code == 0 {
                    let coordinator_rack = &self
                        .nodes
                        .iter()
                        .find(|x| x.broker_id == coordinator.node_id)
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
                    coordinator.host = shotover_node.address.host.clone();
                    coordinator.port = shotover_node.address.port;
                    coordinator.node_id = shotover_node.broker_id;
                }
            }
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
    key: StrBytes,
    key_type: i8,
}

fn get_username_from_scram_request(auth_request: &[u8]) -> Option<String> {
    for s in std::str::from_utf8(auth_request).ok()?.split(',') {
        let mut iter = s.splitn(2, '=');
        if let (Some(key), Some(value)) = (iter.next(), iter.next()) {
            if key == "n" {
                return Some(value.to_owned());
            }
        }
    }
    None
}
