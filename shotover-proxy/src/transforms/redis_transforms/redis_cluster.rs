use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, bail, ensure, Result};
use async_trait::async_trait;
use derivative::Derivative;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt, TryFutureExt};
use hyper::body::Bytes;
use metrics::counter;
use rand::prelude::SmallRng;
use rand::SeedableRng;
use redis_protocol::types::Frame;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio::time::Duration;
use tracing::{debug, error, info, trace, warn};

use crate::concurrency::FuturesOrdered;
use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Message, MessageDetails, Messages, QueryResponse};
use crate::protocols::redis_codec::RedisCodec;
use crate::protocols::RawFrame;
use crate::transforms::redis_transforms::RedisError;
use crate::transforms::redis_transforms::TransformError;
use crate::transforms::util::cluster_connection_pool::{Authenticator, ConnectionPool};
use crate::transforms::util::{Request, Response};
use crate::transforms::ResponseFuture;
use crate::transforms::CONTEXT_CHAIN_NAME;
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};

const SLOT_SIZE: usize = 16384;

type ChannelMap = HashMap<String, Vec<UnboundedSender<Request>>>;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisClusterConfig {
    pub first_contact_points: Vec<String>,
    pub strict_close_mode: Option<bool>,
    connection_count: Option<usize>,
}

#[async_trait]
impl TransformsFromConfig for RedisClusterConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        let authenticator = RedisAuthenticator {};

        let connection_pool =
            ConnectionPool::new_with_auth(RedisCodec::new(true, 3), authenticator);

        let mut cluster = RedisCluster {
            name: "RedisCluster",
            slots: SlotMap::new(),
            channels: ChannelMap::new(),
            load_scores: HashMap::new(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            first_contact_points: self.first_contact_points.clone(),
            connection_count: self.connection_count.unwrap_or(1),
            connection_pool,
            connection_error: None,
            rebuild_slots: false,
            rebuild_connections: true,
            token: None,
        };

        match cluster.build_connections(None).await {
            Ok(()) => {
                info!("connected to upstream cluster");
            }
            Err(TransformError::Upstream(RedisError::NotAuthenticated)) => {
                info!("deferring connection due to auth");
            }
            Err(e) => {
                bail!("failed to connect to upstream: {}", e);
            }
        }

        Ok(Transforms::RedisCluster(cluster))
    }
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct RedisCluster {
    name: &'static str,
    pub slots: SlotMap,
    pub channels: ChannelMap,
    load_scores: HashMap<(String, usize), usize>,
    rng: SmallRng,
    connection_count: usize,
    connection_pool: ConnectionPool<RedisCodec, RedisAuthenticator, UsernamePasswordToken>,
    connection_error: Option<&'static str>,
    rebuild_slots: bool,
    rebuild_connections: bool,
    first_contact_points: Vec<String>,
    token: Option<UsernamePasswordToken>,
}

impl RedisCluster {
    #[inline]
    async fn dispatch_message(&mut self, message: Message) -> Result<ResponseFuture> {
        let command = match message.original {
            RawFrame::Redis(Frame::Array(ref command)) => command,
            _ => bail!("syntax error: bad command"),
        };

        let channels = match self.get_channels(command).await? {
            Ok(channels) => channels,
            Err(command_name) => match command_name {
                Command::AUTH => {
                    return self.on_auth(command).await;
                }
            },
        };

        Ok(match channels.len() {
            0 => {
                let (one_tx, one_rx) = immediate_responder();
                match self.connection_error {
                    Some(message) => {
                        send_error_response(one_tx, message).ok();
                    }
                    None => short_circuit(one_tx),
                };
                Box::pin(one_rx)
            }
            1 => {
                let channel = channels.get(0).unwrap();
                let one_rx = self.choose_and_send(channel, message).await?;
                Box::pin(one_rx.map_err(move |_| anyhow!("no response from single channel")))
            }
            _ => {
                let responses = FuturesUnordered::new();

                for channel in channels {
                    responses.push(self.choose_and_send(&channel, message.clone()).await?);
                }

                // Reassemble upstream responses in any order into an array, as the downstream response.
                // TODO: Improve error messages within the reassembled response. E.g. which channel failed.

                Box::pin(async move {
                    let response = responses
                        .fold(vec![], |mut acc, response| async move {
                            match response {
                                Ok((_, response)) => match response {
                                    Ok(mut messages) => acc.push(messages.messages.pop().map_or(
                                        Frame::Null,
                                        |message| match message.original {
                                            RawFrame::Redis(frame) => frame,
                                            _ => unreachable!(),
                                        },
                                    )),
                                    Err(e) => acc.push(Frame::Error(e.to_string())),
                                },
                                Err(e) => acc.push(Frame::Error(e.to_string())),
                            }
                            acc
                        })
                        .await;

                    Ok((
                        message,
                        ChainResponse::Ok(Messages::new_from_message(Message {
                            details: MessageDetails::Unknown,
                            modified: false,
                            original: RawFrame::Redis(Frame::Array(response)),
                        })),
                    ))
                })
            }
        })
    }

    fn latest_contact_points(&self) -> Vec<String> {
        if !self.slots.nodes.is_empty() {
            // Use latest node addresses as contact points.
            self.slots.nodes.iter().cloned().collect::<Vec<_>>()
        } else {
            // Fallback to initial contact points.
            self.first_contact_points.clone()
        }
    }

    async fn rebuild_slot_map(&mut self) -> Result<(), TransformError> {
        debug!("rebuilding slot map");
        let mut errors = Vec::new();

        for address in self.latest_contact_points() {
            match self
                .connection_pool
                .new_unpooled_connection(&address, &self.token)
                .await
            {
                Ok(sender) => match get_topology_from_node(&sender).await {
                    Ok(slots) => {
                        trace!("successfully mapped cluster: {:?}", slots);
                        self.slots = slots;
                        return Ok(());
                    }
                    Err(e) => {
                        trace!("failed to get slot map from: {}, error: {}", address, e);
                        errors.push(e)
                    }
                },
                Err(e) => errors.push(e.into()),
            }
        }

        debug!("failed to fetch slot map from all hosts");
        Err(TransformError::choose_upstream_or_first(errors).unwrap())
    }

    async fn build_connections(
        &mut self,
        token: Option<UsernamePasswordToken>,
    ) -> Result<(), TransformError> {
        // Backup existing state to restore on failure.
        let old_channels = self.channels.clone();
        let old_token = self.token.clone();

        debug!("building connections with: {:?}", token);
        self.token = token;

        self.rebuild_connections().await.map_err(|e| {
            self.channels = old_channels;
            self.token = old_token;
            e
        })
    }

    async fn rebuild_connections(&mut self) -> Result<(), TransformError> {
        // NOTE: Successful slot rebuild ensures validity of credentials before reusing pool connections.
        self.rebuild_slot_map().await?;

        let mut channels = ChannelMap::new();
        let mut errors = Vec::new();

        // TODO: Eliminate code duplication for master and follower.

        debug!("building master connections");
        for node in self.slots.masters.values() {
            match self
                .connection_pool
                .get_connections(node, &self.token, self.connection_count)
                .await
            {
                Ok(connections) => {
                    channels.insert(node.to_string(), connections);
                }
                Err(e) => {
                    info!("Could not create connection to {} - {}", node, e);
                    errors.push(e.into());
                }
            }
        }

        debug!("building replica connections");
        for node in self.slots.replicas.values() {
            match self
                .connection_pool
                .get_connections(node, &self.token, self.connection_count)
                .await
            {
                Ok(connections) => {
                    channels.insert(node.to_string(), connections);
                }
                Err(e) => {
                    info!("Could not create connection to {} - {}", node, e);
                    errors.push(e.into());
                }
            }
        }

        if channels.is_empty() && !errors.is_empty() {
            debug!("total failure trying to rebuild connections");
            return Err(TransformError::choose_upstream_or_first(errors).unwrap());
        }

        debug!("Connected to cluster: {:?}", channels.keys());
        self.channels = channels;
        Ok(())
    }

    #[inline]
    async fn choose_and_send(
        &mut self,
        host: &str,
        message: Message,
    ) -> Result<oneshot::Receiver<(Message, ChainResponse)>> {
        let (one_tx, one_rx) = oneshot::channel::<Response>();

        let channel = match self.channels.get_mut(host) {
            Some(channels) if channels.len() == 1 => channels.get_mut(0),
            Some(channels) if channels.len() > 1 => {
                let candidates = rand::seq::index::sample(&mut self.rng, channels.len(), 2);
                let aidx = candidates.index(0);
                let bidx = candidates.index(1);

                // TODO: Actually update or remove these "load balancing" scores.
                let aload = *self
                    .load_scores
                    .entry((host.to_string(), aidx))
                    .or_insert(0);
                let bload = *self
                    .load_scores
                    .entry((host.to_string(), bidx))
                    .or_insert(0);

                channels.get_mut(if aload <= bload { aidx } else { bidx })
            }
            _ => None,
        }
        .and_then(|channel| {
            // Treat closed connection as non-existent.
            if channel.is_closed() {
                None
            } else {
                Some(channel)
            }
        });

        let channel = if let Some(channel) = channel {
            channel
        } else {
            debug!("connection {} doesn't exist trying to connect", host);
            if let Ok(result) = timeout(
                Duration::from_millis(40),
                self.connection_pool
                    .get_connections(host, &self.token, self.connection_count),
            )
            .await
            {
                if let Ok(connections) = result {
                    debug!("Found {} live connections for {}", connections.len(), host);
                    self.channels.insert(host.to_string(), connections);
                    self.channels.get_mut(host).unwrap().get_mut(0).unwrap()
                } else {
                    debug!("failed to connect to {}", host);
                    self.rebuild_connections = true;
                    short_circuit(one_tx);
                    return Ok(one_rx);
                }
            } else {
                debug!("timed out connecting to {}", host);
                self.rebuild_connections = true;

                short_circuit(one_tx);
                return Ok(one_rx);
            }
        };

        if let Err(e) = channel.send(Request {
            messages: message,
            return_chan: Some(one_tx),
            message_id: None,
        }) {
            if let Some(error_return) = e.0.return_chan {
                self.rebuild_slots = true;
                short_circuit(error_return);
            }
            self.channels.remove(host);
        }

        Ok(one_rx)
    }

    #[inline(always)]
    async fn get_channels(&mut self, command: &[Frame]) -> Result<Result<Vec<String>, Command>> {
        Ok(match RoutingInfo::for_command_frame(command)? {
            Some(RoutingInfo::Slot(slot)) => Ok(
                if let Some((_, lookup)) = self.slots.masters.range(&slot..).next() {
                    vec![lookup.clone()]
                } else {
                    vec![]
                },
            ),
            Some(RoutingInfo::AllNodes) => Ok(self.slots.nodes.iter().cloned().collect()),
            Some(RoutingInfo::AllMasters) => Ok(self.slots.masters.values().cloned().collect()),
            Some(RoutingInfo::Random) => Ok(self
                .slots
                .masters
                .values()
                .next()
                .map(|key| vec![key.clone()])
                .unwrap_or_default()),
            Some(RoutingInfo::Other(name)) => Err(name),
            None => Ok(vec![]),
        })
    }

    async fn on_auth(&mut self, command: &[Frame]) -> Result<ResponseFuture> {
        let (one_tx, one_rx) = immediate_responder();

        let mut args = command
            .iter()
            .skip(1)
            .rev()
            .flat_map(|f| match f {
                Frame::BulkString(s) => Some(s),
                _ => None,
            })
            // TODO: Remove UTF-8 restriction or send error to client?
            .map(|b| String::from_utf8(b.to_vec()).expect("not utf8"));

        let password = match args.next() {
            Some(password) => password,
            None => {
                debug!("password not supplied");
                send_error_response(one_tx, "ERR syntax error").ok();
                return Ok(Box::pin(one_rx));
            }
        };
        let username = args.next();
        let token = UsernamePasswordToken { username, password };

        match self.build_connections(Some(token)).await {
            Ok(()) => {
                self.connection_error = None;
                send_simple_response(one_tx, "OK")?;
            }
            Err(TransformError::Upstream(RedisError::BadCredentials)) => {
                send_error_response(one_tx, "WRONGPASS invalid username-password")?;
            }
            Err(TransformError::Upstream(RedisError::NotAuthorized)) => {
                send_error_response(one_tx, "NOPERM upstream user lacks required permission")?;
            }
            Err(TransformError::Upstream(e)) => {
                send_error_response(one_tx, e.to_string().as_str())?;
            }
            Err(e) => {
                warn!("failed to build authenticated connections: {:?}", e);
                send_error_response(one_tx, "ERR could not connect to upstream with auth")?;
            }
        }

        Ok(Box::pin(one_rx))
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct SlotMap {
    masters: BTreeMap<u16, String>,
    replicas: BTreeMap<u16, String>,

    // Hide redundant information.
    #[derivative(Debug = "ignore")]
    nodes: HashSet<String>,
}

impl SlotMap {
    fn new() -> Self {
        Self {
            masters: BTreeMap::new(),
            replicas: BTreeMap::new(),
            nodes: HashSet::new(),
        }
    }

    fn from_entries(
        master_entries: Vec<(String, u16, u16)>,
        replica_entries: Vec<(String, u16, u16)>,
    ) -> Self {
        fn to_interval_map(slot_entries: Vec<(String, u16, u16)>) -> BTreeMap<u16, String> {
            slot_entries
                .into_iter()
                .map(|(host, _start, end)| (end, host))
                .collect()
        }

        let nodes: HashSet<_> = master_entries
            .iter()
            .map(|e| &e.0)
            .chain(replica_entries.iter().map(|e| &e.0))
            .cloned()
            .collect();

        Self {
            masters: to_interval_map(master_entries),
            replicas: to_interval_map(replica_entries),
            nodes,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RoutingInfo {
    AllNodes,
    AllMasters,
    Random,
    Slot(u16),
    Other(Command),
}

#[derive(Debug, Clone, Copy)]
pub enum Command {
    AUTH,
}

impl RoutingInfo {
    #[inline(always)]
    pub fn for_command_frame(args: &[Frame]) -> Result<Option<RoutingInfo>> {
        let command_name = match args.get(0) {
            Some(Frame::BulkString(command_name)) => command_name,
            _ => bail!("syntax error: bad command name"),
        };

        Ok(match command_name.as_ref() {
            b"FLUSHALL" | b"FLUSHDB" | b"SCRIPT" | b"ACL" => Some(RoutingInfo::AllMasters),
            b"ECHO" | b"CONFIG" | b"CLIENT" | b"SLOWLOG" | b"DBSIZE" | b"LASTSAVE" | b"PING"
            | b"INFO" | b"BGREWRITEAOF" | b"BGSAVE" | b"CLIENT LIST" | b"SAVE" | b"TIME"
            | b"KEYS" => Some(RoutingInfo::AllNodes),
            b"SCAN" | b"CLIENT SETNAME" | b"SHUTDOWN" | b"SLAVEOF" | b"REPLICAOF"
            | b"SCRIPT KILL" | b"MOVE" | b"BITOP" => None,
            b"EVALSHA" | b"EVAL" => {
                //TODO: Appears the the codec is not decoding integers correctly
                match args.get(2) {
                    Some(Frame::Integer(key_count)) => Some(*key_count),
                    Some(Frame::BulkString(key_count)) => String::from_utf8(key_count.to_vec())
                        .unwrap_or_else(|_| "0".to_string())
                        .parse::<i64>()
                        .ok(),
                    _ => None,
                }
                .and_then(|key_count| {
                    if key_count == 0 {
                        Some(RoutingInfo::Random)
                    } else {
                        args.get(3).and_then(RoutingInfo::for_key)
                    }
                })
            }
            b"XGROUP" | b"XINFO" => args.get(2).and_then(RoutingInfo::for_key),
            b"XREAD" | b"XREADGROUP" => args
                .iter()
                .position(|a| match a {
                    Frame::BulkString(a) => a.as_ref() == b"STREAMS",
                    _ => false,
                })
                .and_then(|streams_position| {
                    args.get(streams_position + 1)
                        .and_then(RoutingInfo::for_key)
                }),
            b"AUTH" => Some(RoutingInfo::Other(Command::AUTH)),
            _ => match args.get(1) {
                Some(key) => RoutingInfo::for_key(key),
                None => Some(RoutingInfo::Random),
            },
        })
    }

    #[inline(always)]
    pub fn for_key(key: &Frame) -> Option<RoutingInfo> {
        if let Frame::BulkString(key) = key {
            let key = get_hashtag(key).unwrap_or(key);
            Some(RoutingInfo::Slot(
                crc16::State::<crc16::XMODEM>::calculate(key) % SLOT_SIZE as u16,
            ))
        } else {
            None
        }
    }
}

fn build_slot_to_server(
    frames: &[Frame],
    slot_entries: &mut Vec<(String, u16, u16)>,
    start: u16,
    end: u16,
) -> Result<()> {
    ensure!(start <= end, "invalid slot range: {}-{}", start, end);
    ensure!(frames.len() >= 2, "expected at least two fields");

    let ip = if let Frame::BulkString(ref ip) = frames[0] {
        String::from_utf8_lossy(ip.as_ref()).to_string()
    } else {
        bail!("unexpected type for ip");
    };

    if ip.is_empty() {
        warn!("Master IP unknown for slots {}-{}.", start, end);
        return Ok(());
    }

    let port = if let Frame::Integer(port) = frames[1] {
        port
    } else {
        bail!("unexpected type for port");
    };

    slot_entries.push((format!("{}:{}", ip, port), start, end));

    Ok(())
}

fn parse_slots(results: &[Frame]) -> Result<SlotMap> {
    let mut master_entries: Vec<(String, u16, u16)> = vec![];
    let mut replica_entries: Vec<(String, u16, u16)> = vec![];

    for result in results.iter() {
        match result {
            Frame::Array(result) => {
                let mut start: u16 = 0;
                let mut end: u16 = 0;

                for (index, item) in result.iter().enumerate() {
                    match (index, item) {
                        (0, Frame::Integer(i)) => start = *i as u16,
                        (1, Frame::Integer(i)) => end = *i as u16,
                        (2, Frame::Array(master)) => {
                            if let Err(e) =
                                build_slot_to_server(master, &mut master_entries, start, end)
                            {
                                bail!("Failed to decode master slots: {}", e,);
                            }
                        }
                        (_, Frame::Array(replica)) => {
                            if let Err(e) =
                                build_slot_to_server(replica, &mut replica_entries, start, end)
                            {
                                bail!("Failed to decode replica slots: {}", e,);
                            }
                        }
                        _ => bail!("unexpected value in slot map",),
                    }
                }
            }
            _ => bail!("unexpected value in slot map".to_string(),),
        }
    }

    // TODO: Only check masters?
    if master_entries.is_empty() {
        bail!("empty slot map!");
    }

    Ok(SlotMap::from_entries(master_entries, replica_entries))
}

async fn get_topology_from_node(
    sender: &UnboundedSender<Request>,
) -> Result<SlotMap, TransformError> {
    let return_chan_rx = send_frame_request(
        sender,
        Frame::Array(vec![
            Frame::BulkString(Bytes::from("CLUSTER")),
            Frame::BulkString(Bytes::from("SLOTS")),
        ]),
    )?;

    match receive_frame_response(return_chan_rx).await? {
        Frame::Array(results) => {
            parse_slots(&results).map_err(|e| TransformError::Protocol(e.to_string()))
        }
        Frame::Error(message) => Err(TransformError::Upstream(RedisError::from_message(
            message.as_str(),
        ))),
        frame => Err(TransformError::Protocol(format!(
            "unexpected response for cluster slots: {:?}",
            frame
        ))),
    }
}

#[inline(always)]
fn get_hashtag(key: &[u8]) -> Option<&[u8]> {
    if let Some(open) = key.iter().position(|v| *v == b'{') {
        if let Some(close) = key[open..].iter().position(|v| *v == b'}') {
            let rv = &key[open + 1..open + close];
            if !rv.is_empty() {
                return Some(rv);
            }
        }
    }
    None
}

#[inline(always)]
fn short_circuit(one_tx: oneshot::Sender<Response>) {
    warn!("Could not route request - short circuiting");
    if let Err(e) = send_error_response(one_tx, "ERR Could not route request") {
        trace!("short circuiting - couldn't send error - {:?}", e);
    }
}

#[inline(always)]
fn send_simple_response(one_tx: oneshot::Sender<Response>, message: &str) -> Result<()> {
    send_frame_response(one_tx, Frame::SimpleString(message.to_string()))
        .map_err(|_| anyhow!("failed to send simple: {}", message))
}

#[inline(always)]
fn send_error_response(one_tx: oneshot::Sender<Response>, message: &str) -> Result<()> {
    if let Err(e) = CONTEXT_CHAIN_NAME.try_with(|chain_name| {
        counter!("redis_cluster_failed_request", 1, "chain" => chain_name.to_string());
    }) {
        error!("failed to count failed request - missing chain name: {}", e);
    }
    send_frame_response(one_tx, Frame::Error(message.to_string()))
        .map_err(|_| anyhow!("failed to send error: {}", message))
}

#[inline(always)]
fn send_frame_request(
    sender: &UnboundedSender<Request>,
    frame: Frame,
) -> Result<oneshot::Receiver<(Message, Result<Messages>)>> {
    let (return_chan_tx, return_chan_rx) = oneshot::channel();

    sender.send(Request {
        messages: Message {
            details: MessageDetails::Unknown,
            modified: false,
            original: RawFrame::Redis(frame),
        },
        return_chan: Some(return_chan_tx),
        message_id: None,
    })?;

    Ok(return_chan_rx)
}

#[inline(always)]
async fn receive_frame_response(
    receiver: oneshot::Receiver<(Message, Result<Messages>)>,
) -> Result<Frame> {
    let (_, result) = receiver.await?;

    // Exactly one Redis response is guaranteed by the codec on success.
    let message = result?.messages.pop().unwrap().original;

    match message {
        RawFrame::Redis(frame) => Ok(frame),
        _ => unreachable!(),
    }
}

#[inline(always)]
fn send_frame_response(one_tx: oneshot::Sender<Response>, frame: Frame) -> Result<(), Response> {
    one_tx.send((
        Message::new_bypass(RawFrame::None),
        Ok(Messages::new_single_response(
            QueryResponse::empty(),
            false,
            RawFrame::Redis(frame),
        )),
    ))
}

fn immediate_responder() -> (
    oneshot::Sender<Response>,
    impl Future<Output = Result<Response>>,
) {
    let (one_tx, one_rx) = oneshot::channel::<Response>();
    (one_tx, async {
        one_rx.await.map_err(|_| {
            error!("unused responder");
            anyhow!("missing response")
        })
    })
}

#[async_trait]
impl Transform for RedisCluster {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        if self.rebuild_connections {
            match self.rebuild_connections().await {
                Ok(()) => self.rebuild_connections = false,
                Err(e) => {
                    if let TransformError::Upstream(RedisError::NotAuthenticated) = e {
                        self.connection_error = Some("NOAUTH Authentication required (cached)");
                        self.rebuild_connections = false;
                    }
                }
            }
        } else if self.rebuild_slots {
            self.rebuild_slot_map().await?;
            self.rebuild_slots = false;
        }

        let mut responses = FuturesOrdered::new();

        for message in message_wrapper.message {
            responses.push(match self.dispatch_message(message).await {
                Ok(response) => response,
                Err(e) => {
                    let (one_tx, one_rx) = immediate_responder();
                    send_error_response(one_tx, &format!("ERR {}", e))?;
                    Box::pin(one_rx)
                }
            });
        }

        trace!("Processing response");
        let mut response_buffer = vec![];

        while let Some(s) = responses.next().await {
            trace!("Got resp {:?}", s);
            let (original, response) = s.or_else(|_| -> Result<(_, _)> {
                Ok((
                    Message::new_bypass(RawFrame::None),
                    Ok(Messages::new_single_response(
                        QueryResponse::empty(),
                        false,
                        RawFrame::Redis(Frame::Error("ERR Could not route request".to_string())),
                    )),
                ))
            })?;
            let mut response = response?;
            assert_eq!(response.messages.len(), 1);
            let response_m = response.messages.remove(0);
            match response_m.original {
                RawFrame::Redis(Frame::Moved { slot, host, port }) => {
                    debug!("Got MOVE frame {} {} {}", slot, host, port);

                    // The destination of a MOVE should always be a master.
                    self.slots
                        .masters
                        .insert(slot, format!("{}:{}", &host, &port));

                    self.rebuild_slots = true;

                    let one_rx = self
                        .choose_and_send(&format!("{}:{}", host, port), original.clone())
                        .await?;

                    responses.prepend(Box::pin(
                        one_rx.map_err(|e| anyhow!("Error while retrying MOVE - {}", e)),
                    ));
                }
                RawFrame::Redis(Frame::Ask { slot, host, port }) => {
                    debug!("Got ASK frame {} {} {}", slot, host, port);

                    let one_rx = self
                        .choose_and_send(&format!("{}:{}", host, port), original.clone())
                        .await?;

                    responses.prepend(Box::pin(
                        one_rx.map_err(|e| anyhow!("Error while retrying ASK - {}", e)),
                    ));
                }
                _ => response_buffer.push(response_m),
            }
        }
        Ok(Messages {
            messages: response_buffer,
        })
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Derivative)]
#[derivative(Debug)]
pub struct UsernamePasswordToken {
    pub username: Option<String>,

    // Reduce risk of logging passwords.
    #[derivative(Debug = "ignore")]
    pub password: String,
}

#[derive(Clone)]
struct RedisAuthenticator {}

#[async_trait]
impl Authenticator<UsernamePasswordToken> for RedisAuthenticator {
    type Error = TransformError;

    async fn authenticate(
        &self,
        sender: &mut UnboundedSender<Request>,
        token: &UsernamePasswordToken,
    ) -> Result<(), TransformError> {
        let auth_frame = {
            let mut args = vec![Frame::BulkString(Bytes::from("AUTH"))];

            // Support non-ACL / username-less.
            if let Some(username) = &token.username {
                args.push(Frame::BulkString(Bytes::from(username.clone())));
            }

            args.push(Frame::BulkString(Bytes::from(token.password.clone())));

            Frame::Array(args)
        };

        let return_rx = send_frame_request(sender, auth_frame)?;

        match receive_frame_response(return_rx).await? {
            Frame::SimpleString(s) if s == "OK" => {
                trace!("authenticated upstream as user: {:?}", token.username);
                Ok(())
            }
            Frame::SimpleString(_) => {
                Err(TransformError::Protocol("bad response value".to_string()))
            }
            Frame::Error(e) => Err(TransformError::Upstream(RedisError::from_message(&e))),
            _ => Err(TransformError::Protocol("bad response type".to_string())),
        }
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use tokio_util::codec::Decoder;

    #[test]
    fn test_parse_slots() {
        // Wireshark capture from a Redis cluster with 3 masters and 3 replicas.
        let slots_pcap: &[u8] = b"*3\r\n*4\r\n:10923\r\n:16383\r\n*3\r\n$12\r\n192.168.80.6\r\n:6379\r\n$40\r\n3a7c357ed75d2aa01fca1e14ef3735a2b2b8ffac\r\n*3\r\n$12\r\n192.168.80.3\r\n:6379\r\n$40\r\n77c01b0ddd8668fff05e3f6a8aaf5f3ccd454a79\r\n*4\r\n:5461\r\n:10922\r\n*3\r\n$12\r\n192.168.80.5\r\n:6379\r\n$40\r\n969c6215d064e68593d384541ceeb57e9520dbed\r\n*3\r\n$12\r\n192.168.80.2\r\n:6379\r\n$40\r\n3929f69990a75be7b2d49594c57fe620862e6fd6\r\n*4\r\n:0\r\n:5460\r\n*3\r\n$12\r\n192.168.80.7\r\n:6379\r\n$40\r\n15d52a65d1fc7a53e34bf9193415aa39136882b2\r\n*3\r\n$12\r\n192.168.80.4\r\n:6379\r\n$40\r\ncd023916a3528fae7e606a10d8289a665d6c47b0\r\n";

        let mut codec = RedisCodec::new(true, 3);

        let slots_frames = if let RawFrame::Redis(Frame::Array(frames)) = codec
            .decode(&mut slots_pcap.into())
            .unwrap()
            .unwrap()
            .messages
            .pop()
            .unwrap()
            .original
        {
            frames
        } else {
            panic!("bad input or codec")
        };

        let slots = parse_slots(&slots_frames).unwrap();

        let nodes = vec![
            "192.168.80.2:6379",
            "192.168.80.3:6379",
            "192.168.80.4:6379",
            "192.168.80.5:6379",
            "192.168.80.6:6379",
            "192.168.80.7:6379",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let masters = vec![
            (5460u16, "192.168.80.7:6379".to_string()),
            (10922u16, "192.168.80.5:6379".to_string()),
            (16383u16, "192.168.80.6:6379".to_string()),
        ];

        let replicas = vec![
            (5460u16, "192.168.80.4:6379".to_string()),
            (10922u16, "192.168.80.2:6379".to_string()),
            (16383u16, "192.168.80.3:6379".to_string()),
        ];

        assert_eq!(slots.nodes, nodes);
        assert_eq!(slots.masters.into_iter().collect::<Vec<_>>(), masters);
        assert_eq!(slots.replicas.into_iter().collect::<Vec<_>>(), replicas);
    }
}
