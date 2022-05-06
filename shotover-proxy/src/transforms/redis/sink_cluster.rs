use crate::codec::redis::RedisCodec;
use crate::concurrency::FuturesOrdered;
use crate::error::ChainResponse;
use crate::frame::{Frame, RedisFrame};
use crate::message::Message;
use crate::tls::TlsConfig;
use crate::transforms::redis::RedisError;
use crate::transforms::redis::TransformError;
use crate::transforms::util::cluster_connection_pool::{Authenticator, ConnectionPool};
use crate::transforms::util::{Request, Response};
use crate::transforms::ResponseFuture;
use crate::transforms::CONTEXT_CHAIN_NAME;
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::{anyhow, bail, ensure, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use bytes_utils::string::Str;
use derivative::Derivative;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt, TryFutureExt};
use metrics::{counter, register_counter};
use rand::prelude::SmallRng;
use rand::SeedableRng;
use redis_protocol::types::Redirection;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio::time::Duration;
use tracing::{debug, error, info, trace, warn};

const SLOT_SIZE: usize = 16384;

type ChannelMap = HashMap<String, Vec<UnboundedSender<Request>>>;

#[derive(Deserialize, Debug, Clone)]
pub struct RedisSinkClusterConfig {
    pub first_contact_points: Vec<String>,
    pub tls: Option<TlsConfig>,
    connection_count: Option<usize>,
}

impl RedisSinkClusterConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<Transforms> {
        let mut cluster = RedisSinkCluster::new(
            self.first_contact_points.clone(),
            self.connection_count.unwrap_or(1),
            self.tls.clone(),
            chain_name,
        )?;

        match cluster.build_connections(None).await {
            Ok(()) => {
                info!("connected to upstream");
            }
            Err(TransformError::Upstream(RedisError::NotAuthenticated)) => {
                info!("upstream requires auth");
            }
            Err(e) => {
                return Err(anyhow!(e).context("failed to connect to upstream"));
            }
        }

        Ok(Transforms::RedisSinkCluster(cluster))
    }
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct RedisSinkCluster {
    pub slots: SlotMap,
    pub channels: ChannelMap,
    load_scores: HashMap<(String, usize), usize>,
    rng: SmallRng,
    connection_count: usize,
    connection_pool: ConnectionPool<RedisCodec, RedisAuthenticator, UsernamePasswordToken>,
    connection_error: Option<&'static str>,
    rebuild_connections: bool,
    first_contact_points: Vec<String>,
    token: Option<UsernamePasswordToken>,
}

impl RedisSinkCluster {
    pub fn new(
        first_contact_points: Vec<String>,
        connection_count: usize,
        tls: Option<TlsConfig>,
        chain_name: String,
    ) -> Result<Self> {
        let authenticator = RedisAuthenticator {};

        let connection_pool = ConnectionPool::new_with_auth(RedisCodec::new(), authenticator, tls)?;

        let sink_cluster = RedisSinkCluster {
            slots: SlotMap::new(),
            channels: ChannelMap::new(),
            load_scores: HashMap::new(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            first_contact_points,
            connection_count,
            connection_pool,
            connection_error: None,
            rebuild_connections: false,
            token: None,
        };

        register_counter!("failed_requests", "chain" => chain_name, "transform" => sink_cluster.get_name());

        Ok(sink_cluster)
    }

    fn get_name(&self) -> &'static str {
        "RedisSinkCluster"
    }

    #[inline]
    async fn dispatch_message(&mut self, mut message: Message) -> Result<ResponseFuture> {
        let command = match message.frame() {
            Some(Frame::Redis(RedisFrame::Array(ref command))) => command,
            None => bail!("Failed to parse redis frame"),
            message => bail!("syntax error: bad command: {message:?}"),
        };

        let routing_info = RoutingInfo::for_command_frame(command)?;
        let channels = match self.get_channels(routing_info) {
            ChannelResult::Channels(channels) => channels,
            ChannelResult::Other(Command::AUTH) => {
                return self.on_auth(command).await;
            }
            ChannelResult::ShortCircuit(frame) => {
                let (one_tx, one_rx) = immediate_responder();
                send_frame_response(one_tx, frame)
                    .map_err(|_| anyhow!("Failed to send short circuited redis frame"))?;
                return Ok(Box::pin(one_rx));
            }
        };

        Ok(match channels.len() {
            // Return an error as we cant send anything if there are no channels.
            0 => {
                let (one_tx, one_rx) = immediate_responder();
                match self.connection_error {
                    Some(message) => {
                        self.send_error_response(one_tx, message).ok();
                    }
                    None => self.short_circuit(one_tx),
                };
                Box::pin(one_rx)
            }
            // Send to the single channel and return its response.
            1 => {
                let channel = channels.get(0).unwrap();
                let one_rx = self.choose_and_send(channel, message).await?;
                Box::pin(one_rx.map_err(|_| anyhow!("no response from single channel")))
            }
            // Send to all senders.
            // If any of the responses were a failure then return that failure.
            // Otherwise return the first successful result
            _ => {
                let responses = FuturesUnordered::new();

                for channel in channels {
                    responses.push(self.choose_and_send(&channel, message.clone()).await?);
                }
                Box::pin(async move {
                    let response = responses
                        .fold(None, |acc, response| async move {
                            if let Some(RedisFrame::Error(_)) = acc {
                                acc
                            } else {
                                match response {
                                    Ok(Response {
                                        response: Ok(mut messages),
                                        ..
                                    }) => Some(messages.pop().map_or(
                                        RedisFrame::Null,
                                        |mut message| match message.frame().unwrap() {
                                            Frame::Redis(frame) => {
                                                let new_frame = frame.take();
                                                match acc {
                                                    Some(prev_frame) => routing_info
                                                        .response_join()
                                                        .join(prev_frame, new_frame),
                                                    None => new_frame,
                                                }
                                            }
                                            _ => unreachable!("direct response from a redis sink"),
                                        },
                                    )),
                                    Ok(Response {
                                        response: Err(e), ..
                                    }) => Some(RedisFrame::Error(e.to_string().into())),
                                    Err(e) => Some(RedisFrame::Error(e.to_string().into())),
                                }
                            }
                        })
                        .await;

                    Ok(Response {
                        original: message,
                        response: ChainResponse::Ok(vec![Message::from_frame(Frame::Redis(
                            response.unwrap(),
                        ))]),
                    })
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

    async fn fetch_slot_map(
        &mut self,
        token: &Option<UsernamePasswordToken>,
    ) -> Result<SlotMap, TransformError> {
        debug!("fetching slot map");

        let addresses = self.latest_contact_points();

        let mut errors = Vec::new();
        let mut results = FuturesUnordered::new();

        for address in &addresses {
            results.push(
                self.connection_pool
                    .new_unpooled_connection(address, token)
                    .map_err(move |err| {
                        trace!("error fetching slot map from {}: {}", address, err);
                        TransformError::from(err)
                    })
                    .and_then(get_topology_from_node)
                    .map_ok(move |slots| {
                        trace!("fetched slot map from {}: {:?}", address, slots);
                        slots
                    }),
            );
        }

        while let Some(result) = results.next().await {
            match result {
                Ok(slots) => return Ok(slots),
                Err(err) => errors.push(err),
            }
        }

        debug!("failed to fetch slot map from all hosts");
        Err(TransformError::choose_upstream_or_first(errors).unwrap())
    }

    async fn build_connections(
        &mut self,
        token: Option<UsernamePasswordToken>,
    ) -> Result<(), TransformError> {
        debug!("building connections");

        match self.build_connections_inner(&token).await {
            Ok((slots, channels)) => {
                debug!("connected to cluster: {:?}", channels.keys());
                self.token = token;
                self.slots = slots;
                self.channels = channels;

                self.connection_error = None;
                self.rebuild_connections = false;
                Ok(())
            }
            Err(err @ TransformError::Upstream(RedisError::NotAuthenticated)) => {
                // Assume retry is pointless if authentication is required.
                self.connection_error = Some("NOAUTH Authentication required (cached)");
                self.rebuild_connections = false;
                Err(err)
            }
            Err(err) => {
                warn!("failed to build connections: {}", err);
                Err(err)
            }
        }
    }

    async fn build_connections_inner(
        &mut self,
        token: &Option<UsernamePasswordToken>,
    ) -> Result<(SlotMap, ChannelMap), TransformError> {
        // NOTE: Fetch slot map uses unpooled connections to check token validity before reusing pooled connections.
        let slots = self.fetch_slot_map(token).await?;

        let mut channels = ChannelMap::new();
        let mut errors = Vec::new();
        for node in slots.masters.values().chain(slots.replicas.values()) {
            match self
                .connection_pool
                .get_connections(node, token, self.connection_count)
                .await
            {
                Ok(connections) => {
                    channels.insert(node.to_string(), connections);
                }
                Err(e) => {
                    // Intentional debug! Some errors should be silently passed through.
                    debug!("failed to connect to {}: {}", node, e);
                    errors.push(e.into());
                }
            }
        }

        if channels.is_empty() && !errors.is_empty() {
            Err(TransformError::choose_upstream_or_first(errors).unwrap())
        } else {
            Ok((slots, channels))
        }
    }

    #[inline]
    async fn choose_and_send(
        &mut self,
        host: &str,
        message: Message,
    ) -> Result<oneshot::Receiver<Response>> {
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

            match timeout(
                Duration::from_millis(40),
                self.connection_pool
                    .get_connections(host, &self.token, self.connection_count),
            )
            .await
            {
                Ok(Ok(connections)) => {
                    debug!("Found {} live connections for {}", connections.len(), host);
                    self.channels.insert(host.to_string(), connections);
                    self.channels.get_mut(host).unwrap().get_mut(0).unwrap()
                }
                Ok(Err(e)) => {
                    debug!("failed to connect to {}: {}", host, e);
                    self.rebuild_connections = true;
                    self.short_circuit(one_tx);
                    return Ok(one_rx);
                }
                Err(_) => {
                    debug!("timed out connecting to {}", host);
                    self.rebuild_connections = true;
                    self.short_circuit(one_tx);
                    return Ok(one_rx);
                }
            }
        };

        if let Err(e) = channel.send(Request {
            message,
            return_chan: Some(one_tx),
        }) {
            if let Some(error_return) = e.0.return_chan {
                self.rebuild_connections = true;
                self.short_circuit(error_return);
            }
            self.channels.remove(host);
        }

        Ok(one_rx)
    }

    #[inline(always)]
    fn get_channels(&mut self, routing_info: RoutingInfo) -> ChannelResult {
        match routing_info {
            RoutingInfo::Slot(slot) => ChannelResult::Channels(
                if let Some((_, lookup)) = self.slots.masters.range(&slot..).next() {
                    vec![lookup.clone()]
                } else {
                    vec![]
                },
            ),
            RoutingInfo::AllNodes(_) => {
                ChannelResult::Channels(self.slots.nodes.iter().cloned().collect())
            }
            RoutingInfo::AllMasters(_) => {
                ChannelResult::Channels(self.slots.masters.values().cloned().collect())
            }
            RoutingInfo::Random => ChannelResult::Channels(
                self.slots
                    .masters
                    .values()
                    .next()
                    .map(|key| vec![key.clone()])
                    .unwrap_or_default(),
            ),
            RoutingInfo::Other(name) => ChannelResult::Other(name),
            RoutingInfo::Unsupported => ChannelResult::Channels(vec![]),
            RoutingInfo::ShortCircuitNil => ChannelResult::ShortCircuit(RedisFrame::Null),
            RoutingInfo::ShortCircuitOk => {
                ChannelResult::ShortCircuit(RedisFrame::SimpleString(Bytes::from("OK")))
            }
        }
    }

    async fn on_auth(&mut self, command: &[RedisFrame]) -> Result<ResponseFuture> {
        let mut args = command.iter().skip(1).rev().map(|f| match f {
            RedisFrame::BulkString(s) => Ok(s),
            _ => bail!("syntax error: expected bulk string"),
        });

        let password = args
            .next()
            .ok_or_else(|| anyhow!("syntax error: expected password"))??
            .clone();

        let username = args.next().transpose()?.cloned();

        if args.next().is_some() {
            bail!("syntax error: too many args")
        }

        let token = UsernamePasswordToken { username, password };

        let (one_tx, one_rx) = immediate_responder();

        match self.build_connections(Some(token)).await {
            Ok(()) => {
                send_simple_response(one_tx, "OK")?;
            }
            Err(TransformError::Upstream(RedisError::BadCredentials)) => {
                self.send_error_response(one_tx, "WRONGPASS invalid username-password")?;
            }
            Err(TransformError::Upstream(RedisError::NotAuthorized)) => {
                self.send_error_response(one_tx, "NOPERM upstream user lacks required permission")?;
            }
            Err(TransformError::Upstream(e)) => {
                self.send_error_response(one_tx, e.to_string().as_str())?;
            }
            Err(e) => {
                return Err(anyhow!(e).context("authentication failed"));
            }
        }

        Ok(Box::pin(one_rx))
    }

    #[inline(always)]
    fn send_error_response(&self, one_tx: oneshot::Sender<Response>, message: &str) -> Result<()> {
        if let Err(e) = CONTEXT_CHAIN_NAME.try_with(|chain_name| {
        counter!("failed_requests", 1, "chain" => chain_name.to_string(), "transform" => self.get_name());
    }) {
        error!("failed to count failed request - missing chain name: {}", e);
    }
        send_frame_response(one_tx, RedisFrame::Error(message.to_string().into()))
            .map_err(|_| anyhow!("failed to send error: {}", message))
    }

    #[inline(always)]
    fn short_circuit(&self, one_tx: oneshot::Sender<Response>) {
        warn!("Could not route request - short circuiting");
        if let Err(e) = self.send_error_response(
            one_tx,
            "ERR Shotover RedisSinkCluster does not not support this command used in this way",
        ) {
            trace!("short circuiting - couldn't send error - {:?}", e);
        }
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct SlotMap {
    pub masters: BTreeMap<u16, String>,
    pub replicas: BTreeMap<u16, String>,

    // Hide redundant information.
    #[derivative(Debug = "ignore")]
    pub nodes: HashSet<String>,
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
    AllNodes(ResponseJoin),
    AllMasters(ResponseJoin),
    Random,
    Slot(u16),
    Other(Command),
    Unsupported,
    ShortCircuitOk,
    ShortCircuitNil,
}

#[derive(Debug, Clone, Copy)]
pub enum ResponseJoin {
    First,
    ArrayJoin,
    IntegerSum,
    IntegerMin,
}

#[derive(Debug, Clone, Copy)]
pub enum Command {
    AUTH,
}

enum ChannelResult {
    Channels(Vec<String>),
    Other(Command),
    ShortCircuit(RedisFrame),
}

impl RoutingInfo {
    #[inline(always)]
    pub fn for_command_frame(args: &[RedisFrame]) -> Result<RoutingInfo> {
        let command_name = match args.get(0) {
            Some(RedisFrame::BulkString(command_name)) => command_name.to_ascii_uppercase(),
            _ => bail!("syntax error: bad command name"),
        };

        Ok(match command_name.as_slice() {
            // These commands write data but need to touch every shard so we send it to every master
            b"FLUSHALL" | b"FLUSHDB" => RoutingInfo::AllMasters(ResponseJoin::First),
            // The total count of all keys can be obtained by summing the count of keys from every shard.
            b"DBSIZE" => RoutingInfo::AllMasters(ResponseJoin::IntegerSum),
            // A complete list of every key can be obtained by joining the key lists provided by every shard.
            //
            // With this implementation, running `KEYS *` in a large production environment could OoM shotover.
            // But I assume in such an environment the nodes would be configured to not allow running KEYS.
            // So each redis node would return an error and then we would return a single error to the client which would be fine.
            b"KEYS" => RoutingInfo::AllMasters(ResponseJoin::ArrayJoin),
            // The LASTSAVE command is needed to confirm that a previous BGSAVE command has succeed.
            // In order to maintain this use case we query every node and return the oldest save time.
            // This way the return value wont change until every node has completed their BGSAVE.
            b"LASTSAVE" => RoutingInfo::AllNodes(ResponseJoin::IntegerMin),
            // When a command that forces writing to disk occurs we want it to occur on every node.
            // Replica nodes receive updates from their master nodes and we want those to be written to disk too.
            b"BGSAVE" | b"SAVE" | b"BGREWRITEAOF" | b"ACL" => {
                RoutingInfo::AllNodes(ResponseJoin::First)
            }
            b"SCRIPT" => match args.get(1) {
                Some(RedisFrame::BulkString(a)) if a.to_ascii_uppercase() == b"KILL" => {
                    RoutingInfo::Unsupported
                }
                _ => RoutingInfo::AllMasters(ResponseJoin::First),
            },
            // * We cant reasonably support CLIENT SETNAME/GETNAME in shotover
            //     - Connections are pooled so we cant forward it to the redis node
            //     - We cant just implement the functionality shotover side because the connection names are supposed to be viewable from CLIENT LIST
            // * However we dont want to just fail them either as clients like jedis would break so:
            //     - We just pretend to accept CLIENT SETNAME returning an "OK" but without hitting any nodes
            //     - We just pretend to handle CLIENT GETNAME always returning nil without hitting any nodes
            b"CLIENT" => match args.get(1) {
                Some(RedisFrame::BulkString(sub_command)) => {
                    let sub_command = sub_command.to_ascii_uppercase();
                    match sub_command.as_slice() {
                        b"SETNAME" => RoutingInfo::ShortCircuitOk,
                        b"GETNAME" => RoutingInfo::ShortCircuitNil,
                        _ => RoutingInfo::Unsupported,
                    }
                }
                _ => RoutingInfo::Random,
            },
            // These commands can not reasonably be supported by shotover, so we just return an error to the client when they are used
            b"SCAN" | b"SHUTDOWN" | b"SLAVEOF" | b"REPLICAOF" | b"MOVE" | b"BITOP" | b"CONFIG"
            | b"SLOWLOG" | b"INFO" | b"TIME" => RoutingInfo::Unsupported,
            b"EVALSHA" | b"EVAL" => match args.get(2) {
                Some(RedisFrame::BulkString(key_count)) => {
                    if key_count.as_ref() == b"0" {
                        RoutingInfo::Random
                    } else {
                        // For an EVAL to succeed every key must be present on the same node.
                        // So we use the first key to find the correct destination node and if any of the
                        // remaining keys are not on the same node as the first key then the destination
                        // node will correctly handle this for us.
                        args.get(3)
                            .and_then(RoutingInfo::for_key)
                            .unwrap_or(RoutingInfo::Unsupported)
                    }
                }
                _ => RoutingInfo::Unsupported,
            },
            b"XGROUP" | b"XINFO" => args
                .get(2)
                .and_then(RoutingInfo::for_key)
                .unwrap_or(RoutingInfo::Unsupported),
            b"XREAD" | b"XREADGROUP" => args
                .iter()
                .position(|a| match a {
                    RedisFrame::BulkString(a) => a.to_ascii_uppercase() == b"STREAMS",
                    _ => false,
                })
                .and_then(|streams_position| {
                    args.get(streams_position + 1)
                        .and_then(RoutingInfo::for_key)
                })
                .unwrap_or(RoutingInfo::Unsupported),
            b"AUTH" => RoutingInfo::Other(Command::AUTH),
            // These are stateless commands that return a response.
            // We just need a single redis node to handle this for us so shotover can pretend to be a single node.
            // So we just pick a node at random.
            b"ECHO" | b"PING" => RoutingInfo::Random,
            _ => match args.get(1) {
                Some(key) => RoutingInfo::for_key(key).unwrap_or(RoutingInfo::Unsupported),
                None => RoutingInfo::Random,
            },
        })
    }

    #[inline(always)]
    pub fn for_key(key: &RedisFrame) -> Option<RoutingInfo> {
        if let RedisFrame::BulkString(key) = key {
            let key = get_hashtag(key).unwrap_or(key);
            Some(RoutingInfo::Slot(
                crc16::State::<crc16::XMODEM>::calculate(key) % SLOT_SIZE as u16,
            ))
        } else {
            None
        }
    }

    pub fn response_join(&self) -> ResponseJoin {
        match self {
            RoutingInfo::AllMasters(join) => *join,
            RoutingInfo::AllNodes(join) => *join,
            _ => ResponseJoin::First,
        }
    }
}

impl ResponseJoin {
    pub fn join(&self, prev_frame: RedisFrame, next_frame: RedisFrame) -> RedisFrame {
        match self {
            ResponseJoin::IntegerMin => match (prev_frame, next_frame) {
                (RedisFrame::Integer(prev), RedisFrame::Integer(next)) => {
                    RedisFrame::Integer(prev.min(next))
                }
                _ => RedisFrame::Error("One of the redis frames was not an integer".into()),
            },
            ResponseJoin::IntegerSum => match (prev_frame, next_frame) {
                (RedisFrame::Integer(prev), RedisFrame::Integer(next)) => {
                    RedisFrame::Integer(prev + next)
                }
                _ => RedisFrame::Error("One of the redis frames was not an integer".into()),
            },
            ResponseJoin::ArrayJoin => match (prev_frame, next_frame) {
                (RedisFrame::Array(mut prev), RedisFrame::Array(next)) => {
                    prev.extend(next);
                    RedisFrame::Array(prev)
                }
                _ => RedisFrame::Error("One of the redis frames was not an array".into()),
            },
            ResponseJoin::First => prev_frame,
        }
    }
}

fn build_slot_to_server(
    frames: &[RedisFrame],
    slot_entries: &mut Vec<(String, u16, u16)>,
    start: u16,
    end: u16,
) -> Result<()> {
    ensure!(start <= end, "invalid slot range: {}-{}", start, end);
    ensure!(frames.len() >= 2, "expected at least two fields");

    let ip = if let RedisFrame::BulkString(ref ip) = frames[0] {
        String::from_utf8_lossy(ip.as_ref()).to_string()
    } else {
        bail!("unexpected type for ip");
    };

    if ip.is_empty() {
        warn!("Master IP unknown for slots {}-{}.", start, end);
        return Ok(());
    }

    let port = if let RedisFrame::Integer(port) = frames[1] {
        port
    } else {
        bail!("unexpected type for port");
    };

    slot_entries.push((format!("{ip}:{port}"), start, end));

    Ok(())
}

pub fn parse_slots(results: &[RedisFrame]) -> Result<SlotMap> {
    let mut master_entries: Vec<(String, u16, u16)> = vec![];
    let mut replica_entries: Vec<(String, u16, u16)> = vec![];

    for result in results {
        match result {
            RedisFrame::Array(result) => {
                let mut start: u16 = 0;
                let mut end: u16 = 0;

                for (index, item) in result.iter().enumerate() {
                    match (index, item) {
                        (0, RedisFrame::Integer(i)) => start = *i as u16,
                        (1, RedisFrame::Integer(i)) => end = *i as u16,
                        (2, RedisFrame::Array(master)) => {
                            build_slot_to_server(master, &mut master_entries, start, end)
                                .context("failed to decode master slots")?
                        }
                        (_, RedisFrame::Array(replica)) => {
                            build_slot_to_server(replica, &mut replica_entries, start, end)
                                .context("failed to decode replica slots")?;
                        }
                        _ => bail!("unexpected value in slot map"),
                    }
                }
            }
            _ => bail!("unexpected value in slot map"),
        }
    }

    if master_entries.is_empty() {
        bail!("empty slot map!");
    }

    Ok(SlotMap::from_entries(master_entries, replica_entries))
}

async fn get_topology_from_node(
    sender: UnboundedSender<Request>,
) -> Result<SlotMap, TransformError> {
    let return_chan_rx = send_frame_request(
        &sender,
        RedisFrame::Array(vec![
            RedisFrame::BulkString("CLUSTER".into()),
            RedisFrame::BulkString("SLOTS".into()),
        ]),
    )?;

    match receive_frame_response(return_chan_rx).await? {
        RedisFrame::Array(results) => {
            parse_slots(&results).map_err(|e| TransformError::Protocol(e.to_string()))
        }
        RedisFrame::Error(message) => {
            Err(TransformError::Upstream(RedisError::from_message(&message)))
        }
        frame => Err(TransformError::Protocol(format!(
            "unexpected response for cluster slots: {frame:?}"
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
fn send_simple_response(one_tx: oneshot::Sender<Response>, message: &str) -> Result<()> {
    send_frame_response(one_tx, RedisFrame::SimpleString(message.to_string().into()))
        .map_err(|_| anyhow!("failed to send simple: {}", message))
}

#[inline(always)]
fn send_frame_request(
    sender: &UnboundedSender<Request>,
    frame: RedisFrame,
) -> Result<oneshot::Receiver<Response>> {
    let (return_chan_tx, return_chan_rx) = oneshot::channel();

    sender.send(Request {
        message: Message::from_frame(Frame::Redis(frame)),
        return_chan: Some(return_chan_tx),
    })?;

    Ok(return_chan_rx)
}

#[inline(always)]
async fn receive_frame_response(receiver: oneshot::Receiver<Response>) -> Result<RedisFrame> {
    let Response { response, .. } = receiver.await?;

    // Exactly one Redis response is guaranteed by the codec on success.
    let mut message = response?.pop().unwrap();

    match message.frame() {
        Some(Frame::Redis(frame)) => Ok(frame.take()),
        None => Err(anyhow!("Failed to parse redis frame")),
        response => Err(anyhow!("Unexpected redis response: {response:?}")),
    }
}

#[inline(always)]
fn send_frame_response(
    one_tx: oneshot::Sender<Response>,
    frame: RedisFrame,
) -> Result<(), Response> {
    one_tx.send(Response {
        original: Message::from_frame(Frame::None),
        response: Ok(vec![Message::from_frame(Frame::Redis(frame))]),
    })
}

fn immediate_responder() -> (
    oneshot::Sender<Response>,
    impl Future<Output = Result<Response>>,
) {
    let (one_tx, one_rx) = oneshot::channel::<Response>();
    (one_tx, async {
        one_rx
            .await
            .map_err(|_| panic!("immediate responder must be used"))
    })
}

#[async_trait]
impl Transform for RedisSinkCluster {
    fn is_terminating(&self) -> bool {
        true
    }

    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        if self.rebuild_connections {
            self.build_connections(self.token.clone()).await.ok();
        }

        let mut responses = FuturesOrdered::new();

        for message in message_wrapper.messages {
            responses.push(match self.dispatch_message(message).await {
                Ok(response) => response,
                Err(e) => {
                    let (one_tx, one_rx) = immediate_responder();
                    self.send_error_response(one_tx, &format!("ERR {e}"))?;
                    Box::pin(one_rx)
                }
            });
        }

        trace!("Processing response");
        let mut response_buffer = vec![];

        while let Some(s) = responses.next().await {
            trace!("Got resp {:?}", s);
            let Response { original, response } = s.or_else(|_| -> Result<Response> {
                Ok(Response {
                    original: Message::from_frame(Frame::None),
                    response: Ok(vec![Message::from_frame(Frame::Redis(RedisFrame::Error(
                        Str::from_inner(Bytes::from_static(b"ERR Could not route request"))
                            .unwrap(),
                    )))]),
                })
            })?;

            let mut response = response?;
            assert_eq!(response.len(), 1);
            let mut response_m = response.remove(0);
            match response_m.frame() {
                Some(Frame::Redis(frame)) => {
                    match frame.to_redirection() {
                        Some(Redirection::Moved { slot, server }) => {
                            debug!("Got MOVE {} {}", slot, server);

                            // The destination of a MOVE should always be a master.
                            self.slots.masters.insert(slot, server.clone());

                            self.rebuild_connections = true;

                            let one_rx = self.choose_and_send(&server, original).await?;

                            responses.prepend(Box::pin(
                                one_rx.map_err(|e| anyhow!("Error while retrying MOVE - {}", e)),
                            ));
                        }
                        Some(Redirection::Ask { slot, server }) => {
                            debug!("Got ASK {} {}", slot, server);

                            let one_rx = self.choose_and_send(&server, original).await?;

                            responses.prepend(Box::pin(
                                one_rx.map_err(|e| anyhow!("Error while retrying ASK - {}", e)),
                            ));
                        }
                        None => response_buffer.push(response_m),
                    }
                }
                _ => response_buffer.push(response_m),
            }
        }
        Ok(response_buffer)
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Derivative)]
#[derivative(Debug)]
pub struct UsernamePasswordToken {
    pub username: Option<Bytes>,

    // Reduce risk of logging passwords.
    #[derivative(Debug = "ignore")]
    pub password: Bytes,
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
        let mut auth_args = vec![RedisFrame::BulkString(Bytes::from_static(b"AUTH"))];

        // Support non-ACL / username-less.
        if let Some(username) = &token.username {
            auth_args.push(RedisFrame::BulkString(username.clone()));
        }

        auth_args.push(RedisFrame::BulkString(token.password.clone()));

        let return_rx = send_frame_request(sender, RedisFrame::Array(auth_args))?;

        match receive_frame_response(return_rx).await? {
            RedisFrame::SimpleString(s) if s == "OK" => {
                trace!("authenticated upstream as user: {:?}", token.username);
                Ok(())
            }
            RedisFrame::SimpleString(s) => Err(TransformError::Protocol(format!(
                "expected OK but got: {s:?}"
            ))),
            RedisFrame::Error(e) => Err(TransformError::Upstream(RedisError::from_message(&e))),
            f => Err(TransformError::Protocol(format!(
                "unexpected response type: {f:?}"
            ))),
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

        let mut codec = RedisCodec::new();

        let mut message = codec
            .decode(&mut slots_pcap.into())
            .unwrap()
            .unwrap()
            .pop()
            .unwrap();

        let slots_frames = match message.frame().unwrap() {
            Frame::Redis(RedisFrame::Array(frames)) => frames,
            frame => panic!("bad input: {frame:?}"),
        };

        let slots = parse_slots(slots_frames).unwrap();

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
