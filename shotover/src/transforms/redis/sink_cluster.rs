use crate::codec::redis::RedisCodecBuilder;
use crate::codec::{CodecBuilder, Direction};
use crate::frame::{Frame, MessageType, RedisFrame};
use crate::message::{Message, Messages};
use crate::tls::TlsConnectorConfig;
use crate::transforms::redis::RedisError;
use crate::transforms::redis::TransformError;
use crate::transforms::util::cluster_connection_pool::{Authenticator, ConnectionPool};
use crate::transforms::util::{Request, Response};
use crate::transforms::{
    DownChainProtocol, ResponseFuture, Transform, TransformBuilder, TransformConfig,
    TransformContextBuilder, TransformContextConfig, UpChainProtocol, Wrapper,
};
use anyhow::{anyhow, bail, ensure, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use derivative::Derivative;
use futures::stream::FuturesOrdered;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryFutureExt};
use itertools::Itertools;
use metrics::{counter, Counter};
use rand::rngs::SmallRng;
use rand::seq::IteratorRandom;
use rand::SeedableRng;
use redis_protocol::bytes_utils::string::Str;
use redis_protocol::resp2::types::Resp2Frame;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{oneshot, RwLock};
use tokio::time::{timeout, Duration};
use tracing::{debug, trace, warn};

const SLOT_SIZE: usize = 16384;

type ChannelMap = HashMap<String, Vec<UnboundedSender<Request>>>;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct RedisSinkClusterConfig {
    pub first_contact_points: Vec<String>,
    pub direct_destination: Option<String>,
    pub tls: Option<TlsConnectorConfig>,
    pub connection_count: Option<usize>,
    pub connect_timeout_ms: u64,
}

const NAME: &str = "RedisSinkCluster";
#[typetag::serde(name = "RedisSinkCluster")]
#[async_trait(?Send)]
impl TransformConfig for RedisSinkClusterConfig {
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        let connection_pool = ConnectionPool::new_with_auth(
            Duration::from_millis(self.connect_timeout_ms),
            RedisCodecBuilder::new(Direction::Sink, "RedisSinkCluster".to_owned()),
            RedisAuthenticator {},
            self.tls.clone(),
        )?;
        Ok(Box::new(RedisSinkClusterBuilder::new(
            self.first_contact_points.clone(),
            self.direct_destination.clone(),
            self.connection_count.unwrap_or(1),
            connection_pool,
            transform_context.chain_name,
            Arc::new(RwLock::new(Topology::new())),
        )))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::MustBeOneOf(vec![MessageType::Redis])
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::Terminating
    }
}

pub struct RedisSinkClusterBuilder {
    first_contact_points: Vec<String>,
    direct_destination: Option<String>,
    connection_count: usize,
    connection_pool: ConnectionPool<RedisCodecBuilder, RedisAuthenticator, UsernamePasswordToken>,
    shared_topology: Arc<RwLock<Topology>>,
    failed_requests: Counter,
}

impl RedisSinkClusterBuilder {
    fn new(
        first_contact_points: Vec<String>,
        direct_destination: Option<String>,
        connection_count: usize,
        connection_pool: ConnectionPool<
            RedisCodecBuilder,
            RedisAuthenticator,
            UsernamePasswordToken,
        >,
        chain_name: String,
        shared_topology: Arc<RwLock<Topology>>,
    ) -> Self {
        let failed_requests =
            counter!("shotover_failed_requests_count", "chain" => chain_name, "transform" => NAME);

        RedisSinkClusterBuilder {
            first_contact_points,
            direct_destination,
            connection_count,
            connection_pool,
            shared_topology,
            failed_requests,
        }
    }
}

impl TransformBuilder for RedisSinkClusterBuilder {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(RedisSinkCluster::new(
            self.first_contact_points.clone(),
            self.direct_destination.clone(),
            self.connection_count,
            self.shared_topology.clone(),
            self.connection_pool.clone(),
            self.failed_requests.clone(),
        ))
    }

    fn get_name(&self) -> &'static str {
        NAME
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
struct Topology {
    slots: SlotMap,
    channels: ChannelMap,
}

impl Topology {
    fn new() -> Self {
        Topology {
            slots: SlotMap::new(),
            channels: ChannelMap::new(),
        }
    }
}

pub struct RedisSinkCluster {
    has_run_init: bool,
    topology: Topology,
    shared_topology: Arc<RwLock<Topology>>,
    direct_connection: Option<UnboundedSender<Request>>,
    load_scores: HashMap<(String, usize), usize>,
    rng: SmallRng,
    connection_count: usize,
    connection_pool: ConnectionPool<RedisCodecBuilder, RedisAuthenticator, UsernamePasswordToken>,
    reason_for_no_nodes: Option<&'static str>,
    rebuild_connections: bool,
    first_contact_points: Vec<String>,
    direct_destination: Option<String>,
    token: Option<UsernamePasswordToken>,
    failed_requests: Counter,
}

impl RedisSinkCluster {
    fn new(
        first_contact_points: Vec<String>,
        direct_destination: Option<String>,
        connection_count: usize,
        shared_topology: Arc<RwLock<Topology>>,
        connection_pool: ConnectionPool<
            RedisCodecBuilder,
            RedisAuthenticator,
            UsernamePasswordToken,
        >,
        failed_requests: Counter,
    ) -> Self {
        RedisSinkCluster {
            has_run_init: false,
            first_contact_points,
            direct_destination,
            topology: Topology::new(),
            shared_topology,
            direct_connection: None,
            load_scores: HashMap::new(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            connection_count,
            connection_pool,
            reason_for_no_nodes: None,
            rebuild_connections: true,
            token: None,
            failed_requests,
        }
    }

    async fn direct_connection(&mut self) -> Result<&UnboundedSender<Request>> {
        if self.direct_connection.is_none() {
            match &self.direct_destination {
                Some(address) => {
                    self.direct_connection = Some(
                        self.connection_pool
                            .get_connections(address, &self.token, 1)
                            .await?
                            .remove(0),
                    );
                }
                None => {
                    bail!("Cannot call direct_connection when direct_destination is configured as None")
                }
            }
        }
        Ok(self.direct_connection.as_ref().unwrap())
    }

    #[inline]
    async fn dispatch_message(&mut self, mut message: Message) -> Result<ResponseFuture> {
        let command = match message.frame() {
            Some(Frame::Redis(RedisFrame::Array(ref command))) => command,
            None => bail!("Failed to parse redis frame"),
            message => bail!("syntax error: bad command: {message:?}"),
        };

        let routing_info = RoutingInfo::for_command_frame(command)?;
        match self.direct_destination {
            Some(_) => self.dispatch_message_handling(routing_info, message).await,
            None => self.dispatch_message_hiding(routing_info, message).await,
        }
    }

    async fn send_message_to_slot(
        &mut self,
        slot: u16,
        message: Message,
    ) -> Result<ResponseFuture> {
        if let Some((_, lookup)) = self.topology.slots.masters.range(&slot..).next() {
            let lookup = lookup.to_string();
            let one_rx = self.choose_and_send(&lookup, message).await?;
            Ok(Box::pin(
                one_rx.map_err(|_| anyhow!("no response from single channel")),
            ))
        } else {
            self.send_error_response(
                self.reason_for_no_nodes
                    .unwrap_or("ERR Shotover RedisSinkCluster does not know of a node containing the required slot")
            )
        }
    }

    async fn send_message_to_channels(
        &mut self,
        channels: &[String],
        message: Message,
        routing_info: RoutingInfo,
    ) -> Result<ResponseFuture> {
        match channels.len() {
            // Return an error as we cant send anything if there are no channels.
            0 => self.send_error_response(
                self.reason_for_no_nodes
                    .unwrap_or("ERR Shotover RedisSinkCluster does not know of any nodes"),
            ),
            // Send to the single channel and return its response.
            1 => Ok(Box::pin(
                self.choose_and_send(&channels[0], message)
                    .await?
                    .map_err(|_| anyhow!("no response from single channel")),
            )),
            // Send to all senders.
            // If any of the responses were a failure then return that failure.
            // Otherwise collate results according to routing_info.
            _ => {
                let responses = FuturesUnordered::new();

                for channel in channels {
                    responses.push(self.choose_and_send(channel, message.clone()).await?);
                }
                Ok(Box::pin(async move {
                    let response = responses
                        .fold(None, |acc, response| async move {
                            if let Some((_, RedisFrame::Error(_))) = acc {
                                acc
                            } else {
                                match response {
                                    Ok(Response {
                                        response: Ok(mut message),
                                        ..
                                    }) => Some((
                                        message.received_from_source_or_sink_at,
                                        match message.frame().unwrap() {
                                            Frame::Redis(frame) => {
                                                let new_frame = frame.take();
                                                match acc {
                                                    Some((_, prev_frame)) => routing_info
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
                                    }) => Some((None, RedisFrame::Error(e.to_string().into()))),
                                    Err(e) => Some((None, RedisFrame::Error(e.to_string().into()))),
                                }
                            }
                        })
                        .await;

                    let (received_at, response) = response.unwrap();
                    Ok(Response {
                        response: Ok(Message::from_frame_at_instant(
                            Frame::Redis(response),
                            received_at,
                        )),
                    })
                }))
            }
        }
    }

    fn latest_contact_points(&self) -> Vec<&str> {
        if !self.topology.slots.nodes.is_empty() {
            // Use latest node addresses as contact points.
            self.topology
                .slots
                .nodes
                .iter()
                .map(|x| x.as_str())
                .collect()
        } else {
            // Fallback to initial contact points.
            self.first_contact_points
                .iter()
                .map(|x| x.as_str())
                .collect()
        }
    }

    async fn fetch_slot_map(
        &mut self,
        token: &Option<UsernamePasswordToken>,
    ) -> Result<SlotMap, TransformError> {
        debug!("fetching slot map");

        let addresses = self.latest_contact_points();

        let mut results = FuturesUnordered::new();
        for address in &addresses {
            results.push(
                self.connection_pool
                    .new_unpooled_connection(address, token)
                    .map_err(move |err| {
                        trace!("error fetching slot map from {}: {:?}", address, err);
                        TransformError::from(err)
                    })
                    .and_then(get_topology_from_node)
                    .map_ok(move |slots| {
                        trace!("fetched slot map from {}: {:?}", address, slots);
                        slots
                    }),
            );
        }

        let mut errors = Vec::new();
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
                self.topology = Topology { slots, channels };
                if token.is_none() {
                    // when authentication isnt used we can share topology between connections
                    *self.shared_topology.write().await = self.topology.clone();
                }
                self.token = token;
                self.reason_for_no_nodes = None;
                self.rebuild_connections = false;
                Ok(())
            }
            Err(err @ TransformError::Upstream(RedisError::NotAuthenticated)) => {
                // Assume retry is pointless if authentication is required.
                self.reason_for_no_nodes = Some("NOAUTH Authentication required (cached)");
                self.rebuild_connections = false;
                Err(err)
            }
            Err(err) => Err(err),
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
                    debug!("failed to connect to {}: {:?}", node, e);
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
    async fn choose_and_send(&mut self, host: &str, message: Message) -> Result<ResponseFuture> {
        let channel = match self.topology.channels.get_mut(host) {
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
                    self.topology.channels.insert(host.to_string(), connections);
                    self.topology
                        .channels
                        .get_mut(host)
                        .unwrap()
                        .get_mut(0)
                        .unwrap()
                }
                Ok(Err(e)) => {
                    debug!("failed to connect to {}: {}", host, e);
                    self.rebuild_connections = true;
                    return self.short_circuit_with_error();
                }
                Err(_) => {
                    debug!("timed out connecting to {}", host);
                    self.rebuild_connections = true;
                    return self.short_circuit_with_error();
                }
            }
        };

        let (one_tx, one_rx) = oneshot::channel::<Response>();
        if channel
            .send(Request {
                message,
                return_chan: Some(one_tx),
            })
            .is_err()
        {
            self.rebuild_connections = true;
            self.topology.channels.remove(host);
            return self.short_circuit_with_error();
        }

        Ok(Box::pin(one_rx.map_err(|e| anyhow!(e))))
    }

    async fn dispatch_message_hiding(
        &mut self,
        routing_info: RoutingInfo,
        message: Message,
    ) -> Result<ResponseFuture> {
        match routing_info {
            RoutingInfo::Slot(slot) => self.send_message_to_slot(slot, message).await,
            RoutingInfo::AllNodes(_) => {
                self.send_message_to_channels(
                    &self.topology.slots.nodes.iter().cloned().collect_vec(),
                    message,
                    routing_info,
                )
                .await
            }
            RoutingInfo::AllMasters(_) => {
                self.send_message_to_channels(
                    &self.topology.slots.masters.values().cloned().collect_vec(),
                    message,
                    routing_info,
                )
                .await
            }
            RoutingInfo::Random => {
                let lookup = self
                    .topology
                    .slots
                    .masters
                    .values()
                    .choose(&mut self.rng)
                    .cloned()
                    .unwrap_or_default();
                self.choose_and_send(&lookup, message).await
            }
            RoutingInfo::Auth => self.on_auth(message).await,
            RoutingInfo::Unsupported => {
                short_circuit(RedisFrame::Error(
                    Str::from_inner(Bytes::from_static(b"ERR unknown command - Shotover RedisSinkCluster does not not support this command")).unwrap(),
                ))
            }
            RoutingInfo::ShortCircuitNil => short_circuit(RedisFrame::Null),
            RoutingInfo::ShortCircuitOk => {
                short_circuit(RedisFrame::SimpleString(Bytes::from_static(b"OK")))
            }
        }
    }

    async fn dispatch_message_handling(
        &mut self,
        routing_info: RoutingInfo,
        message: Message,
    ) -> Result<ResponseFuture> {
        match routing_info {
            RoutingInfo::Slot(slot) => self.send_message_to_slot(slot, message).await,
            RoutingInfo::AllNodes(_)
            | RoutingInfo::AllMasters(_)
            | RoutingInfo::Random
            | RoutingInfo::Unsupported
            | RoutingInfo::ShortCircuitNil
            | RoutingInfo::ShortCircuitOk => {
                let connection = self.direct_connection().await?;
                Ok(Box::pin(
                    send_message_request(connection, message)?
                        .map_err(|_| anyhow!("no response from direct connection")),
                ))
            }
            RoutingInfo::Auth => self.on_auth(message).await,
        }
    }

    async fn on_auth(&mut self, mut message: Message) -> Result<ResponseFuture> {
        let command = match message.frame() {
            Some(Frame::Redis(RedisFrame::Array(ref command))) => command,
            None => bail!("Failed to parse redis frame"),
            message => bail!("syntax error: bad command: {message:?}"),
        };

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

        match self.build_connections(Some(token)).await {
            Ok(()) => short_circuit(RedisFrame::SimpleString("OK".into())),
            Err(TransformError::Upstream(RedisError::BadCredentials)) => {
                self.send_error_response("WRONGPASS invalid username-password")
            }
            Err(TransformError::Upstream(RedisError::NotAuthorized)) => {
                self.send_error_response("NOPERM upstream user lacks required permission")
            }
            Err(TransformError::Upstream(e)) => self.send_error_response(e.to_string().as_str()),
            Err(e) => Err(anyhow!(e).context("authentication failed")),
        }
    }

    #[inline(always)]
    fn send_error_response(&self, message: &str) -> Result<ResponseFuture> {
        self.failed_requests.increment(1);
        short_circuit(RedisFrame::Error(message.into()))
    }

    // TODO: calls to this function should be completely replaced with calls to short_circuit that provide more specific error messages
    fn short_circuit_with_error(&self) -> Result<ResponseFuture> {
        warn!("Could not route request - short circuiting");
        short_circuit(RedisFrame::Error(
            "ERR Shotover RedisSinkCluster does not not support this command used in this way"
                .into(),
        ))
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
    Slot(u16),
    Auth,
    /// In handling mode falls back to sending to the destination address
    AllNodes(ResponseJoin),
    /// In handling mode falls back to sending to the destination address
    AllMasters(ResponseJoin),
    /// In handling mode falls back to sending to the destination address
    Random,
    /// In handling mode falls back to sending to the destination address
    Unsupported,
    /// In handling mode falls back to sending to the destination address
    ShortCircuitOk,
    /// In handling mode falls back to sending to the destination address
    ShortCircuitNil,
}

#[derive(Debug, Clone, Copy)]
pub enum ResponseJoin {
    First,
    ArrayJoin,
    IntegerSum,
    IntegerMin,
}

impl RoutingInfo {
    #[inline(always)]
    pub fn for_command_frame(args: &[RedisFrame]) -> Result<RoutingInfo> {
        let command_name = match args.first() {
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
                Some(RedisFrame::BulkString(a)) if a.eq_ignore_ascii_case(b"KILL") => {
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
                    match sub_command.to_ascii_uppercase().as_slice() {
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
                    RedisFrame::BulkString(a) => a.eq_ignore_ascii_case(b"STREAMS"),
                    _ => false,
                })
                .and_then(|streams_position| {
                    args.get(streams_position + 1)
                        .and_then(RoutingInfo::for_key)
                })
                .unwrap_or(RoutingInfo::Unsupported),
            b"AUTH" => RoutingInfo::Auth,
            // These are stateless commands that return a response.
            // We just need a single redis node to handle this for us so shotover can pretend to be a single node.
            // So we just pick a node at random.
            b"ECHO" | b"PING" => RoutingInfo::Random,
            b"HELLO" => RoutingInfo::Unsupported,
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
        std::str::from_utf8(ip.as_ref()).context("Failed to parse IP address as utf8")?
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
    let return_chan_rx = send_message_request(
        &sender,
        Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
            RedisFrame::BulkString("CLUSTER".into()),
            RedisFrame::BulkString("SLOTS".into()),
        ]))),
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
fn send_message_request(
    sender: &UnboundedSender<Request>,
    message: Message,
) -> Result<oneshot::Receiver<Response>> {
    let (return_chan_tx, return_chan_rx) = oneshot::channel();

    sender.send(Request {
        message,
        return_chan: Some(return_chan_tx),
    })?;

    Ok(return_chan_rx)
}

#[inline(always)]
async fn receive_frame_response(receiver: oneshot::Receiver<Response>) -> Result<RedisFrame> {
    let Response { response, .. } = receiver.await?;

    match response?.frame() {
        Some(Frame::Redis(frame)) => Ok(frame.take()),
        None => Err(anyhow!("Failed to parse redis frame")),
        response => Err(anyhow!("Unexpected redis response: {response:?}")),
    }
}

fn short_circuit(frame: RedisFrame) -> Result<ResponseFuture> {
    let (one_tx, one_rx) = oneshot::channel::<Response>();

    one_tx
        .send(Response {
            response: Ok(Message::from_frame(Frame::Redis(frame))),
        })
        .map_err(|_| anyhow!("Failed to send short circuited redis frame"))?;

    Ok(Box::pin(async {
        one_rx
            .await
            .map_err(|_| panic!("immediate responder must be used"))
    }))
}

#[async_trait]
impl Transform for RedisSinkCluster {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        if !self.has_run_init {
            self.topology = (*self.shared_topology.read().await).clone();
            if self.topology.channels.is_empty() {
                // The code paths for authenticated and unauthenticated redis are quite different.
                // * For unauthenticated redis this initial build_connections should succeed.
                //    + This is required to process the messages we are about to receive.
                //    + We also share the results to skip having to run build_connections again for new connection
                // * For authenticated redis this initial build_connections always fails
                //    + The first message to come through should be an AUTH command which will give us the credentials required for us to run build_connections.
                //      As soon as we receive it we will rerun build_connections so we can process other message types afterwards.
                //    + It is important we do not share the results of the successful build_connections as that would leak authenticated shotover<->redis connections to other client<->shotover connections.
                if let Err(err) = self.build_connections(self.token.clone()).await {
                    match err {
                        TransformError::Upstream(RedisError::NotAuthenticated) => {
                            // Build_connections sent an internal `CLUSTER SLOTS` command to redis and redis refused to respond because it is enforcing authentication.
                            // When the client sends an AUTH message we will rerun build_connections.
                        }
                        _ => tracing::warn!("Error when building connections: {err:?}"),
                    }
                }
            }
            self.has_run_init = true;
        }

        if self.rebuild_connections {
            if let Err(err) = self.build_connections(self.token.clone()).await {
                tracing::warn!("Error when rebuilding connections: {err:?}");
            }
        }

        let mut responses = FuturesOrdered::new();

        let mut requests = requests_wrapper.requests.clone();
        requests.reverse();
        for message in requests_wrapper.requests {
            responses.push_back(match self.dispatch_message(message).await {
                Ok(response) => response,
                Err(e) => short_circuit(RedisFrame::Error(format!("ERR {e}").into())).unwrap(),
            })
        }

        trace!("Processing response");
        let mut response_buffer = vec![];

        while let Some(s) = responses.next().await {
            let original = requests.pop().unwrap();

            trace!("Got resp {:?}", s);
            let Response { response } = s.or_else(|e| -> Result<Response> {
                Ok(Response {
                    response: Ok(Message::from_frame(Frame::Redis(RedisFrame::Error(
                        format!("ERR Could not route request - {e}").into(),
                    )))),
                })
            })?;

            let mut response = response?;
            match response.frame() {
                Some(Frame::Redis(frame)) => {
                    match Redirection::parse(frame) {
                        Some(Redirection::Moved { slot, server }) => {
                            debug!("Got MOVE {} {}", slot, server);

                            // The destination of a MOVE should always be a master.
                            self.topology.slots.masters.insert(slot, server.clone());

                            self.rebuild_connections = true;

                            responses.push_front(Box::pin(
                                self.choose_and_send(&server, original)
                                    .await?
                                    .map_err(|e| e.context("Error while retrying MOVE")),
                            ));
                        }
                        Some(Redirection::Ask { slot, server }) => {
                            debug!("Got ASK {} {}", slot, server);

                            responses.push_front(Box::pin(
                                self.choose_and_send(&server, original)
                                    .await?
                                    .map_err(|e| e.context("Error while retrying ASK")),
                            ));
                        }
                        None => response_buffer.push(response),
                    }
                }
                _ => response_buffer.push(response),
            }
        }
        Ok(response_buffer)
    }
}

enum Redirection {
    Moved { slot: u16, server: String },
    Ask { slot: u16, server: String },
}

impl Redirection {
    fn parse(frame: &RedisFrame) -> Option<Redirection> {
        match frame {
            RedisFrame::Error(err) => {
                let mut tokens = err.split(' ');
                match tokens.next()? {
                    "MOVED" => Some(Redirection::Moved {
                        slot: tokens.next()?.parse().ok()?,
                        server: tokens.next()?.to_owned(),
                    }),
                    "ASK" => Some(Redirection::Ask {
                        slot: tokens.next()?.parse().ok()?,
                        server: tokens.next()?.to_owned(),
                    }),
                    _ => None,
                }
            }
            _ => None,
        }
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

        let return_rx = send_message_request(
            sender,
            Message::from_frame(Frame::Redis(RedisFrame::Array(auth_args))),
        )?;

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
    use crate::codec::redis::RedisDecoder;
    use crate::codec::Direction;
    use pretty_assertions::assert_eq;
    use tokio_util::codec::Decoder;

    #[test]
    fn test_parse_slots() {
        // Wireshark capture from a Redis cluster with 3 masters and 3 replicas.
        let slots_pcap: &[u8] = b"*3\r\n*4\r\n:10923\r\n:16383\r\n*3\r\n$12\r\n192.168.80.6\r\n:6379\r\n$40\r\n3a7c357ed75d2aa01fca1e14ef3735a2b2b8ffac\r\n*3\r\n$12\r\n192.168.80.3\r\n:6379\r\n$40\r\n77c01b0ddd8668fff05e3f6a8aaf5f3ccd454a79\r\n*4\r\n:5461\r\n:10922\r\n*3\r\n$12\r\n192.168.80.5\r\n:6379\r\n$40\r\n969c6215d064e68593d384541ceeb57e9520dbed\r\n*3\r\n$12\r\n192.168.80.2\r\n:6379\r\n$40\r\n3929f69990a75be7b2d49594c57fe620862e6fd6\r\n*4\r\n:0\r\n:5460\r\n*3\r\n$12\r\n192.168.80.7\r\n:6379\r\n$40\r\n15d52a65d1fc7a53e34bf9193415aa39136882b2\r\n*3\r\n$12\r\n192.168.80.4\r\n:6379\r\n$40\r\ncd023916a3528fae7e606a10d8289a665d6c47b0\r\n";

        let mut codec = RedisDecoder::new(None, Direction::Sink);

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
