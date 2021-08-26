use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::*;

use anyhow::{anyhow, bail, ensure, Context, Result};
use async_trait::async_trait;
use derivative::Derivative;
use futures::stream::FuturesUnordered;
use futures::{SinkExt, StreamExt, TryFutureExt};
use hyper::body::Bytes;
use itertools::Itertools;
use metrics::counter;
use rand::prelude::SmallRng;
use rand::SeedableRng;
use redis_protocol::types::Frame;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
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
use crate::transforms::ResponseFuturesOrdered;
use crate::transforms::CONTEXT_CHAIN_NAME;
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
                .get_connection(&node, self.connection_count.unwrap_or(1))
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
    ) -> Result<tokio::sync::oneshot::Receiver<(Message, ChainResponse)>> {
        let (one_tx, one_rx) = tokio::sync::oneshot::channel::<Response>();

        // let mut retry = true;

        // TODO: this is hard to read and may be bug prone
        let chan = match self.channels.get_mut(host) {
            Some(chans) if chans.len() == 1 => chans.get_mut(0).unwrap(),
            Some(chans) if chans.len() > 1 => {
                let candidates = rand::seq::index::sample(&mut self.rng, chans.len(), 2);
                let aidx = candidates.index(0);
                let bidx = candidates.index(1);

                let aload = *self.load_scores.entry((host.clone(), aidx)).or_insert(0);
                let bload = *self.load_scores.entry((host.clone(), bidx)).or_insert(0);

                chans
                    .get_mut(if aload <= bload { aidx } else { bidx })
                    .ok_or_else(|| anyhow!("Couldn't find host {}", host))?
            }
            _ => {
                debug!("connection {} doesn't exist trying to connect", host);
                if let Ok(res) = timeout(
                    Duration::from_millis(40),
                    self.connection_pool
                        .get_connection(host, self.connection_count),
                )
                .await
                {
                    if let Ok(conn) = res {
                        debug!("Found {} live connections for {}", conn.len(), host);
                        self.channels.insert(host.clone(), conn);
                        self.channels
                            .get_mut(host)
                            .unwrap()
                            .get_mut(0)
                            .ok_or_else(|| anyhow!("Couldn't find host {}", host))?
                    } else {
                        debug!(
                            "couldn't connect to {} - updating slot map from upstream cluster",
                            host
                        );
                        self.rebuild_slots = true;
                        short_circuit(one_tx);
                        return Ok(one_rx);
                    }
                } else {
                    debug!(
                        "couldn't connect to {} - updating slot map from upstream cluster",
                        host
                    );
                    self.rebuild_slots = true;
                    short_circuit(one_tx);
                    return Ok(one_rx);
                }
            }
        };

        if let Err(e) = chan.send(Request {
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
    async fn get_channels(&mut self, redis_frame: &RawFrame) -> Vec<String> {
        match &redis_frame {
            RawFrame::Redis(Frame::Array(ref commands)) => {
                match RoutingInfo::for_command_frame(&commands) {
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
                }
            }
            _ => vec![],
        }
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
    pub fn for_command_frame(args: &[Frame]) -> Option<RoutingInfo> {
        if let Some(Frame::BulkString(command_arg)) = args.get(0) {
            match command_arg.as_ref() {
                b"FLUSHALL" | b"FLUSHDB" | b"SCRIPT" | b"ACL" => Some(RoutingInfo::AllMasters),
                b"ECHO" | b"CONFIG" | b"CLIENT" | b"SLOWLOG" | b"DBSIZE" | b"LASTSAVE"
                | b"PING" | b"INFO" | b"BGREWRITEAOF" | b"BGSAVE" | b"CLIENT LIST" | b"SAVE"
                | b"TIME" | b"KEYS" | b"AUTH" => Some(RoutingInfo::AllNodes),
                b"SCAN" | b"CLIENT SETNAME" | b"SHUTDOWN" | b"SLAVEOF" | b"REPLICAOF"
                | b"SCRIPT KILL" | b"MOVE" | b"BITOP" => None,
                b"EVALSHA" | b"EVAL" => {
                    //TODO: Appears the the codec is not decoding integers correctly
                    let key_count = match args.get(2) {
                        Some(Frame::Integer(key_count)) => Some(*key_count),
                        Some(Frame::BulkString(key_count)) => String::from_utf8(key_count.to_vec())
                            .unwrap_or_else(|_| "0".to_string())
                            .parse::<i64>()
                            .ok(),
                        _ => None,
                    };

                    let route_info = if let Some(key_count) = key_count {
                        if key_count == 0 {
                            Some(RoutingInfo::Random)
                        } else {
                            args.get(3).and_then(RoutingInfo::for_key)
                        }
                    } else {
                        None
                    };
                    route_info
                }
                b"XGROUP" | b"XINFO" => args.get(2).and_then(RoutingInfo::for_key),
                b"XREAD" | b"XREADGROUP" => {
                    let streams_position = args.iter().position(|a| match a {
                        Frame::BulkString(a) => a.as_ref() == b"STREAMS",
                        _ => false,
                    })?;
                    args.get(streams_position + 1)
                        .and_then(RoutingInfo::for_key)
                }
                _ => match args.get(1) {
                    Some(key) => RoutingInfo::for_key(key),
                    None => Some(RoutingInfo::Random),
                },
            }
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn for_key(key: &Frame) -> Option<RoutingInfo> {
        if let Frame::BulkString(key) = key {
            let key = match get_hashtag(&key) {
                Some(tag) => tag,
                None => &key,
            };
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
    if outbound_framed_codec
        .send(Messages::new_from_message(Message {
            details: MessageDetails::Unknown,
            modified: false,
            original: RawFrame::Redis(Frame::Array(vec![
                Frame::BulkString(Bytes::from("CLUSTER")),
                Frame::BulkString(Bytes::from("SLOTS")),
            ])),
        }))
        .await
        .is_ok()
    {
        if let Some(Ok(mut o)) = outbound_framed_codec.next().await {
            if let RawFrame::Redis(response) = o.messages.pop().unwrap().original {
                parse_slots(response).map_err(|e| anyhow!("couldn't decode map: {}", e))
            } else {
                Err(anyhow!("couldn't decode map"))
            }
        } else {
            Err(anyhow!("couldn't connect"))
        }
    } else {
        Err(anyhow!("couldn't decode map"))
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
fn short_circuit(one_tx: tokio::sync::oneshot::Sender<Response>) {
    warn!("Could not route request - short circuiting");
    if let Err(e) = CONTEXT_CHAIN_NAME.try_with(|chain_name| {
        counter!("redis_cluster_failed_request", 1, "chain" => chain_name.to_string());
    }) {
        error!("failed to count failed request - missing chain name: {}", e);
    };

    if let Err(e) = one_tx.send((
        Message::new_bypass(RawFrame::None),
        Ok(Messages::new_single_response(
            QueryResponse::empty(),
            false,
            RawFrame::Redis(Frame::Error("ERR Could not route request".to_string())),
        )),
    )) {
        trace!("short circtuiting - couldn't send error - {:?}", e);
    }
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
            let sender = self.get_channels(&message.original).await;

            responses.push(match sender.len() {
                0 => {
                    let (one_tx, one_rx) = tokio::sync::oneshot::channel::<Response>();
                    short_circuit(one_tx);
                    Box::pin(one_rx.map_err(|e| {
                        anyhow!("0 Couldn't get short circtuited for no channels - {}", e)
                    }))
                }
                1 => {
                    let one_rx = self
                        .choose_and_send(sender.get(0).unwrap(), message.clone())
                        .await?;
                    Box::pin(one_rx.map_err(|e| anyhow!("1 {}", e)))
                }
                _ => {
                    let futures: FuturesUnordered<
                        tokio::sync::oneshot::Receiver<(Message, ChainResponse)>,
                    > = FuturesUnordered::new();
                    for chan in sender {
                        let one_rx = self.choose_and_send(&chan, message.clone()).await?;
                        futures.push(one_rx);
                    }
                    Box::pin(async move {
                        let (response, orig) = futures
                            .fold((vec![], message), |(mut acc, last), m| async move {
                                match m {
                                    Ok((_orig, response)) => match response {
                                        Ok(mut m) => {
                                            acc.push(m.messages.pop().map_or(Frame::Null, |m| {
                                                if let RawFrame::Redis(f) = m.original {
                                                    f
                                                } else {
                                                    Frame::Error(
                                                        "Non-redis frame detected".to_string(),
                                                    )
                                                }
                                            }))
                                        }
                                        Err(e) => acc.push(Frame::Error(e.to_string())),
                                    },
                                    Err(e) => acc.push(Frame::Error(e.to_string())),
                                }
                                (acc, last)
                            })
                            .await;

                        std::result::Result::Ok((
                            orig,
                            ChainResponse::Ok(Messages::new_from_message(Message {
                                details: MessageDetails::Unknown,
                                modified: false,
                                original: RawFrame::Redis(Frame::Array(response)),
                            })),
                        ))
                    })
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
