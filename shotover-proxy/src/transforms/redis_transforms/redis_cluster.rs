use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::*;

use anyhow::{anyhow, bail, ensure, Context, Result};
use async_trait::async_trait;
use derivative::Derivative;
use futures::stream::FuturesUnordered;
use futures::{Future, SinkExt, StreamExt, TryFutureExt};
use hyper::body::Bytes;
use itertools::Itertools;
use metrics::counter;
use rand::prelude::SmallRng;
use rand::SeedableRng;
use redis_protocol::types::Frame;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio::time::Duration;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, trace, warn};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Message, MessageDetails, Messages, QueryResponse};
use crate::protocols::redis_codec::RedisCodec;
use crate::protocols::RawFrame;
use crate::transforms::util::cluster_connection_pool::ConnectionPool;
use crate::transforms::util::{Request, Response};
use crate::transforms::CONTEXT_CHAIN_NAME;
use crate::transforms::{ResponseFuture, ResponseFuturesOrdered};
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};

const SLOT_SIZE: usize = 16384;

type ChannelMap = HashMap<String, Vec<UnboundedSender<Request>>>;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisClusterConfig {
    pub first_contact_points: Vec<String>,
    pub strict_close_mode: Option<bool>,
    connection_count: Option<i32>,
}

#[async_trait]
impl TransformsFromConfig for RedisClusterConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        let slots = get_topology(&self.first_contact_points).await?;
        debug!("Detected cluster: {:?}", slots);

        let connection_pool = ConnectionPool::new(
            slots.nodes.iter().cloned().collect_vec(),
            RedisCodec::new(true, 3),
        );

        let mut connection_map: ChannelMap = ChannelMap::new();

        for node in slots.masters.values() {
            match connection_pool
                .get_connections(&node, self.connection_count.unwrap_or(1))
                .await
            {
                Ok(conn) => {
                    connection_map.insert(node.clone(), conn);
                }
                Err(e) => {
                    info!("Could not create connection to {} - {}", node, e);
                }
            }
        }

        debug!("Channels: {:?}", connection_map);

        Ok(Transforms::RedisCluster(RedisCluster {
            name: "RedisCluster",
            slots,
            channels: connection_map,
            load_scores: HashMap::new(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            connection_count: self.connection_count.unwrap_or(1),
            connection_pool,
            rebuild_slots: true,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct RedisCluster {
    name: &'static str,
    pub slots: SlotMap,
    pub channels: ChannelMap,
    load_scores: HashMap<(String, usize), usize>,
    rng: SmallRng,
    connection_count: i32,
    connection_pool: ConnectionPool<RedisCodec>,
    rebuild_slots: bool,
}

impl RedisCluster {
    #[inline]
    async fn dispatch_message(&mut self, message: Message) -> Result<ResponseFuture> {
        let command = match message.original {
            RawFrame::Redis(Frame::Array(ref command)) => command,
            _ => bail!("syntax error: bad command"),
        };

        let channels = self.get_channels(command).await?;

        Ok(match channels.len() {
            0 => {
                let (one_tx, one_rx) = immediate_responder();
                short_circuit(one_tx);
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

    async fn rebuild_slot_map(&mut self) -> Result<()> {
        let contact_points = self.channels.keys().cloned().collect_vec();
        self.slots = get_topology(&contact_points).await?;
        debug!("successfully updated map {:#?}", self.slots);
        Ok(())
    }

    #[inline]
    async fn choose_and_send(
        &mut self,
        host: &String,
        message: Message,
    ) -> Result<oneshot::Receiver<(Message, ChainResponse)>> {
        let (one_tx, one_rx) = oneshot::channel::<Response>();

        let channel = match self.channels.get_mut(host) {
            Some(channels) if channels.len() == 1 => channels.get_mut(0).unwrap(),
            Some(channels) if channels.len() > 1 => {
                let candidates = rand::seq::index::sample(&mut self.rng, channels.len(), 2);
                let aidx = candidates.index(0);
                let bidx = candidates.index(1);

                // TODO: Actually update or remove these "load balancing" scores.
                let aload = *self.load_scores.entry((host.clone(), aidx)).or_insert(0);
                let bload = *self.load_scores.entry((host.clone(), bidx)).or_insert(0);

                channels
                    .get_mut(if aload <= bload { aidx } else { bidx })
                    .unwrap()
            }
            _ => {
                debug!("connection {} doesn't exist trying to connect", host);
                if let Ok(result) = timeout(
                    Duration::from_millis(40),
                    self.connection_pool
                        .get_connections(host, self.connection_count),
                )
                .await
                {
                    if let Ok(connections) = result {
                        debug!("Found {} live connections for {}", connections.len(), host);
                        self.channels.insert(host.to_string(), connections);
                        self.channels.get_mut(host).unwrap().get_mut(0).unwrap()
                    } else {
                        debug!("failed to connect to {}", host);
                        self.rebuild_slots = true;
                        short_circuit(one_tx);
                        return Ok(one_rx);
                    }
                } else {
                    debug!("timed out connecting to {}", host);
                    self.rebuild_slots = true;
                    short_circuit(one_tx);
                    return Ok(one_rx);
                }
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
    async fn get_channels(&mut self, command: &Vec<Frame>) -> Result<Vec<String>> {
        Ok(match RoutingInfo::for_command_frame(command)? {
            Some(RoutingInfo::Slot(slot)) => {
                if let Some((_, lookup)) = self.slots.masters.range(&slot..).next() {
                    // let idx = self.choose(lookup);
                    vec![lookup.clone()]
                } else {
                    vec![]
                }
            }
            Some(RoutingInfo::AllNodes) | Some(RoutingInfo::AllMasters) => {
                self.channels.keys().cloned().collect()
            }
            Some(RoutingInfo::Random) => {
                let key = self
                    .channels
                    .keys()
                    .next()
                    .unwrap_or(&"nothing".to_string())
                    .clone();
                vec![key]
            }
            None => vec![],
        })
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
            | b"KEYS" | b"AUTH" => Some(RoutingInfo::AllNodes),
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
            _ => match args.get(1) {
                Some(key) => RoutingInfo::for_key(key),
                None => Some(RoutingInfo::Random),
            },
        })
    }

    #[inline(always)]
    pub fn for_key(key: &Frame) -> Option<RoutingInfo> {
        if let Frame::BulkString(key) = key {
            let key = get_hashtag(&key).unwrap_or(&key);
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

fn parse_slots(response: Frame) -> Result<SlotMap> {
    let mut master_entries: Vec<(String, u16, u16)> = vec![];
    let mut replica_entries: Vec<(String, u16, u16)> = vec![];

    match response {
        Frame::Array(results) => {
            for result in results.into_iter() {
                match result {
                    Frame::Array(result) => {
                        let mut start: u16 = 0;
                        let mut end: u16 = 0;

                        for (index, item) in result.into_iter().enumerate() {
                            match (index, item) {
                                (0, Frame::Integer(i)) => start = i as u16,
                                (1, Frame::Integer(i)) => end = i as u16,
                                (2, Frame::Array(master)) => {
                                    build_slot_to_server(&master, &mut master_entries, start, end)
                                        .context("Failed to decode master slots")?
                                }
                                (_, Frame::Array(replica)) => {
                                    build_slot_to_server(&replica, &mut replica_entries, start, end)
                                        .context("Failed to decode replica slots")?
                                }
                                _ => bail!("Unexpected value in slot map"),
                            }
                        }
                    }
                    _ => bail!("Unexpected value in slot map"),
                }
            }
        }
        Frame::Error(err) => {
            bail!("Frame error: {}", err);
        }
        _ => {}
    }

    if master_entries.is_empty() {
        Err(anyhow!("Empty slot map!"))
    } else {
        Ok(SlotMap::from_entries(master_entries, replica_entries))
    }
}

async fn get_topology_from_node(stream: TcpStream) -> Result<SlotMap> {
    let mut outbound_framed_codec = Framed::new(stream, RedisCodec::new(true, 1));
    outbound_framed_codec
        .send(Messages::new_from_message(Message {
            details: MessageDetails::Unknown,
            modified: false,
            original: RawFrame::Redis(Frame::Array(vec![
                Frame::BulkString(Bytes::from("CLUSTER")),
                Frame::BulkString(Bytes::from("SLOTS")),
            ])),
        }))
        .await?;

    if let Some(messages) = outbound_framed_codec.next().await {
        if let RawFrame::Redis(response) = messages?.messages.pop().unwrap().original {
            parse_slots(response).map_err(|e| anyhow!("couldn't decode map: {}", e))
        } else {
            Err(anyhow!("Redis did not respond with a redis message"))
        }
    } else {
        Err(anyhow!("Redis did not respond with a message"))
    }
}

async fn get_topology(first_contact_points: &[String]) -> Result<SlotMap> {
    let mut results = FuturesUnordered::new();

    for contact in first_contact_points {
        let query_map = TcpStream::connect(contact.clone())
            .map_err(|e| anyhow!("couldn't connect: {}", e))
            .and_then(get_topology_from_node);
        results.push(query_map);
    }

    while let Some(response) = results.next().await {
        match response {
            Ok(map) => return Ok(map),
            Err(e) => warn!("failed to fetch map from one host: {}", e),
        }
    }

    Err(anyhow!("Couldn't get slot map from redis"))
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
fn send_error_response(one_tx: oneshot::Sender<Response>, message: &str) -> Result<()> {
    if let Err(e) = CONTEXT_CHAIN_NAME.try_with(|chain_name| {
        counter!("redis_cluster_failed_request", 1, "chain" => chain_name.clone());
    }) {
        error!("failed to count failed request - missing chain name: {}", e)
    };
    send_frame_response(one_tx, Frame::Error(message.to_string()))
        .map_err(|_| anyhow!("failed to send error: {}", message))
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
        if self.rebuild_slots {
            self.rebuild_slot_map().await?;
            self.rebuild_slots = false;
        }

        let mut responses = ResponseFuturesOrdered::new();

        for message in message_wrapper.message {
            responses.push(match self.dispatch_message(message).await {
                Ok(response) => response,
                Err(e) => {
                    let (one_tx, one_rx) = immediate_responder();
                    send_error_response(one_tx, &format!("ERR transform error: {}", e))?;
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

                    self.slots
                        .masters
                        .insert(slot, format!("{}:{}", &host, &port));

                    self.rebuild_slots = true;

                    let one_rx = self
                        .choose_and_send(&format!("{}:{}", &host, port), original.clone())
                        .await?;

                    responses.prepend(Box::pin(
                        one_rx.map_err(|e| anyhow!("Error while retrying MOVE - {}", e)),
                    ));
                }
                RawFrame::Redis(Frame::Ask { slot, host, port }) => {
                    debug!("Got ASK frame {} {} {}", slot, host, port);

                    let one_rx = self
                        .choose_and_send(&format!("{}:{}", &host, port), original.clone())
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
