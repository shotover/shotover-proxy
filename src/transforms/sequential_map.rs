use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Message, MessageDetails, Messages, QueryResponse, Value};
use std::iter::*;

use crate::concurrency::FuturesOrdered;
use crate::protocols::redis_codec::RedisCodec;
use crate::protocols::RawFrame;
use crate::transforms::chain::TransformChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, TransformsFromConfig, Wrapper,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use hyper::body::Bytes;
use itertools::Itertools;
use redis_protocol::types::Frame;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio::io::Error;
use tokio::net::TcpStream;

use futures::{Future, TryFuture, TryFutureExt};
use std::pin::Pin;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{Framed, FramedRead, FramedWrite};
use tracing::debug;

const SLOT_SIZE: usize = 16384;

#[derive(Debug, Clone)]
pub struct SequentialMap {
    name: &'static str,
    // chain: TransformChain,
    pub slots: SlotMap,
    pub channels: ChannelMap,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SequentialMapConfig {
    pub first_contact_points: Vec<String>,
    pub strict_close_mode: Option<bool>,
}

fn build_slot_to_server(
    frames: &mut Vec<Frame>,
    nodes: &mut HashSet<String>,
    slots: &mut Vec<(String, u16, u16)>,
    start: u16,
    end: u16,
) {
    if frames.len() < 2 {
        return;
    }

    let ip = if let Frame::BulkString(ref ip) = frames[0] {
        String::from_utf8_lossy(ip.as_ref()).to_string()
    } else {
        return;
    };

    let port = if let Frame::Integer(port) = frames[1] {
        port
    } else {
        return;
    };
    if start == end {
        return;
    }
    nodes.insert(format!("{}:{}", ip, port));
    slots.push((format!("{}:{}", ip, port), start, end));
}

pub struct SlotsMapping {
    pub masters: Vec<(String, u16, u16)>,
    pub followers: Vec<(String, u16, u16)>,
    pub nodes: HashSet<String>,
}

fn parse_slots(contacts_raw: Frame) -> Result<SlotsMapping> {
    let mut slots: Vec<(String, u16, u16)> = vec![];
    let mut replica_slots: Vec<(String, u16, u16)> = vec![];
    let mut nodes: HashSet<String> = HashSet::new();

    if let Frame::Array(response) = contacts_raw {
        let mut response_iter = response.into_iter();
        while let Some(Frame::Array(item)) = response_iter.next() {
            let mut enumerator = item.into_iter().enumerate();

            let mut start: u16 = 0;
            let mut end: u16 = 0;

            while let Some((index, item)) = enumerator.next() {
                match (index, item) {
                    (0, Frame::Integer(i)) => start = i as u16,
                    (1, Frame::Integer(i)) => end = i as u16,
                    (2, Frame::Array(mut master)) => {
                        build_slot_to_server(&mut master, &mut nodes, &mut slots, start, end)
                    }
                    (n, Frame::Array(mut follow)) if n > 2 => build_slot_to_server(
                        &mut follow,
                        &mut nodes,
                        &mut replica_slots,
                        start,
                        end,
                    ),
                    _ => return Err(anyhow!("Unexpected value in slot map")),
                }
            }
        }
    }

    return if slots.is_empty() {
        Err(anyhow!("Empty slot map!"))
    } else {
        Ok(SlotsMapping {
            masters: slots,
            followers: replica_slots,
            nodes,
        })
    };
}

async fn get_topology(first_contact_points: &Vec<String>) -> Result<SlotsMapping> {
    for contact in first_contact_points {
        match TcpStream::connect(contact.clone()).await {
            Ok(stream) => {
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
                    .is_err()
                {
                    continue;
                }
                if let Some(Ok(mut o)) = outbound_framed_codec.next().await {
                    if let RawFrame::Redis(contacts_raw) = o.messages.pop().unwrap().original {
                        if let Ok(slotmaps) = parse_slots(contacts_raw) {
                            return Ok(slotmaps);
                        } else {
                            continue;
                        }
                    }
                } else {
                    continue;
                }
            }
            Err(e) => continue,
        }
    }
    Err(anyhow!("Couldn't get slot map from redis"))
}

#[async_trait]
impl TransformsFromConfig for SequentialMapConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        let slots = get_topology(&self.first_contact_points).await?;
        let mut connection_map: ChannelMap = ChannelMap::new();

        for node in slots.nodes {
            connection_map.insert(node.clone(), connect(&node).await?);
        }

        let slot_map: SlotMap = slots
            .masters
            .iter()
            .map(|(host, start, end)| (*end, connection_map.get(host).unwrap().clone()))
            .collect();

        Ok(Transforms::SequentialMap(SequentialMap {
            name: "SequentialMap",
            slots: slot_map,
            channels: connection_map,
        }))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RoutingInfo {
    AllNodes,
    AllMasters,
    Random,
    Slot(u16),
}

fn get_hashtag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|v| *v == b'{');
    let open = match open {
        Some(open) => open,
        None => return None,
    };

    let close = key[open..].iter().position(|v| *v == b'}');
    let close = match close {
        Some(close) => close,
        None => return None,
    };

    let rv = &key[open + 1..open + close];
    if rv.is_empty() {
        None
    } else {
        Some(rv)
    }
}

impl RoutingInfo {
    #[inline(always)]
    pub fn for_command_frame(args: &Vec<Frame>) -> Option<RoutingInfo> {
        if let Some(Frame::BulkString(command_arg)) = args.get(0) {
            return match command_arg.as_ref() {
                b"FLUSHALL" | b"FLUSHDB" | b"SCRIPT" | b"ACL" => Some(RoutingInfo::AllMasters),
                b"ECHO" | b"CONFIG" | b"CLIENT" | b"SLOWLOG" | b"DBSIZE" | b"LASTSAVE"
                | b"PING" | b"INFO" | b"BGREWRITEAOF" | b"BGSAVE" | b"CLIENT LIST" | b"SAVE"
                | b"TIME" | b"KEYS" | b"AUTH" => Some(RoutingInfo::AllNodes),
                b"SCAN" | b"CLIENT SETNAME" | b"SHUTDOWN" | b"SLAVEOF" | b"REPLICAOF"
                | b"SCRIPT KILL" | b"MOVE" | b"BITOP" => None,
                b"EVALSHA" | b"EVAL" => {
                    let foo = if let Some(Frame::Integer(key_count)) = args.get(2) {
                        if *key_count == 0 {
                            Some(RoutingInfo::Random)
                        } else {
                            args.get(3).and_then(RoutingInfo::for_key)
                        }
                    } else {
                        None
                    };
                    foo
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
            };
        }
        None
    }

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

type SlotMap = BTreeMap<u16, UnboundedSender<Request>>;
type ChannelMap = HashMap<String, UnboundedSender<Request>>;
type Response = (Message, ChainResponse);

#[derive(Debug)]
pub struct Request {
    pub messages: Message,
    pub return_chan: Option<tokio::sync::oneshot::Sender<Response>>,
}

fn short_circuit(one_tx: tokio::sync::oneshot::Sender<Response>) {
    one_tx.send((
        Message::new_bypass(RawFrame::NONE),
        Ok(Messages::new_single_response(
            QueryResponse::empty_with_error(Some(Value::Strings(
                "ERR Could not route request".to_string(),
            ))),
            true,
            RawFrame::NONE,
        )),
    ));
}

pub async fn connect(host: &String) -> Result<UnboundedSender<Request>> {
    let socket: TcpStream = TcpStream::connect(host).await?;
    let (read, write) = socket.into_split();
    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
    let (return_tx, mut return_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();

    tokio::spawn(async move {
        let codec = RedisCodec::new(true, 4);
        let mut in_w = FramedWrite::new(write, codec.clone());
        let rx_stream = UnboundedReceiverStream::new(out_rx).map(|x| {
            let ret = Ok(Messages {
                messages: vec![x.messages.clone()],
            });
            return_tx.send(x);
            ret
        });
        rx_stream.forward(in_w).await;
    });

    tokio::spawn(async move {
        let codec = RedisCodec::new(true, 1);
        let mut in_r = FramedRead::new(read, codec.clone());

        while let Some(Ok(req)) = in_r.next().await {
            for m in req {
                if let Some(Request {
                    messages,
                    return_chan: Some(ret),
                }) = return_rx.recv().await
                {
                    //TODO convert codec to single messages
                    ret.send((messages, Ok(Messages { messages: vec![m] })));
                }
            }
        }
    });

    Ok(out_tx)
}

#[async_trait]
impl Transform for SequentialMap {
    async fn transform<'a>(&'a mut self, mut qd: Wrapper<'a>) -> ChainResponse {
        let mut responses: FuturesOrdered<
            Pin<
                Box<
                    dyn Future<Output = Result<(Message, Result<Messages, anyhow::Error>)>>
                        + std::marker::Send,
                >,
            >,
        > = FuturesOrdered::new();
        // let message = qd.message.messages.pop().unwrap();
        for message in qd.message {
            let mut sender = match &message.original {
                RawFrame::Redis(Frame::Array(ref commands)) => {
                    match RoutingInfo::for_command_frame(&commands) {
                        Some(RoutingInfo::Slot(slot)) => {
                            if let Some((_, sender)) = self.slots.range_mut(&slot..).next() {
                                vec![sender]
                            } else {
                                vec![]
                            }
                        }
                        Some(RoutingInfo::AllNodes) | Some(RoutingInfo::AllMasters) => {
                            self.channels.iter_mut().map(|(_, chan)| chan).collect_vec()
                        }
                        Some(RoutingInfo::Random) => {
                            let key = self
                                .channels
                                .keys()
                                .next()
                                .unwrap_or(&"nothing".to_string())
                                .clone();
                            self.channels.get_mut(&key).into_iter().collect_vec()
                        }
                        None => {
                            vec![]
                        }
                    }
                }
                _ => {
                    vec![]
                }
            };

            if sender.is_empty() {
                let (one_tx, one_rx) = tokio::sync::oneshot::channel::<Response>();
                short_circuit(one_tx);
                responses.push(Box::pin(async move {
                    one_rx.await.map_err(|e| anyhow!("{}", e))
                }))
            } else {
                if sender.len() == 1 {
                    let (one_tx, one_rx) = tokio::sync::oneshot::channel::<Response>();
                    let chan = sender.pop().unwrap();
                    chan.send(Request {
                        messages: message.clone(),
                        return_chan: Some(one_tx),
                    })?;
                    responses.push(Box::pin(async move {
                        one_rx.await.map_err(|e| anyhow!("{}", e))
                    }))
                } else {
                    let futures = sender
                        .into_iter()
                        .map(|chan| {
                            let (one_tx, one_rx) = tokio::sync::oneshot::channel::<Response>();
                            chan.send(Request {
                                messages: message.clone(),
                                return_chan: Some(one_tx),
                            });
                            Box::pin(async move { one_rx.await })
                        })
                        .fold(vec![], |mut acc, f| {
                            acc.push(f);
                            acc
                        });
                    let results = futures::future::join_all(futures).await;
                    let result_future = Box::pin(async move {
                        let (response, orig) = results.into_iter().fold(
                            (vec![], Message::new_bypass(RawFrame::NONE)),
                            |(mut acc, mut last), mut m| {
                                match m {
                                    Ok((orig, response)) => match response {
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
                            },
                        );

                        std::result::Result::Ok((
                            orig.clone(),
                            ChainResponse::Ok(Messages::new_from_message(Message {
                                details: MessageDetails::Unknown,
                                modified: false,
                                original: RawFrame::Redis(Frame::Array(response)),
                            })),
                        ))
                    });
                    responses.push(result_future)
                }
            }
        }

        let mut response_buffer = vec![];
        loop {
            match responses.next().await {
                Some(s) => {
                    let (original, response) = s?;
                    let response_m = response?.messages.remove(0);
                    match response_m.original {
                        RawFrame::Redis(Frame::Moved { slot, host, port }) => {
                            debug!("Got Moved frame {} {} {}", slot, host, port);
                            let chan = match self.channels.get_mut(&*format!("{}:{}", host, port)) {
                                None => {
                                    //here we create a new connection if there isn't one, here we update the slot map
                                    //TODO: Connection error will break everything

                                    //TODO: change connections to slots for more multiple connections
                                    let chan = connect(&format!("{}:{}", &host, port)).await?;
                                    self.slots.insert(slot, chan.clone());
                                    self.channels.insert(format!("{}:{}", &host, &port), chan);
                                    self.channels
                                        .get_mut(&*format!("{}:{}", &host, &port))
                                        .unwrap()
                                }
                                Some(chan) => chan,
                            };

                            let (one_tx, one_rx) = tokio::sync::oneshot::channel::<Response>();
                            chan.send(Request {
                                messages: original.clone(),
                                return_chan: Some(one_tx),
                            })?;
                            responses.prepend(Box::pin(async move {
                                one_rx.await.map_err(|e| anyhow!("{}", e))
                            }));

                            //update slots
                            //handle request
                        }
                        RawFrame::Redis(Frame::Ask { slot, host, port }) => {
                            // see redis-protocol.rs for redirection struct so we dont need to parse the string again
                            debug!("Got ASK frame {} {} {}", slot, host, port);
                            let chan = match self.channels.get_mut(&*format!("{}:{}", host, port)) {
                                None => {
                                    //here we create a new connection if there isn't one, however we don't update the slot map
                                    // TODO this will break on any connection error
                                    let chan = connect(&format!("{}:{}", &host, port)).await?;
                                    self.channels
                                        .insert(format!("{}:{}", &host, &port), chan.clone());
                                    self.channels
                                        .get_mut(&*format!("{}:{}", &host, &port))
                                        .unwrap()
                                }
                                Some(chan) => chan,
                            };

                            let (one_tx, one_rx) = tokio::sync::oneshot::channel::<Response>();
                            chan.send(Request {
                                messages: original.clone(),
                                return_chan: Some(one_tx),
                            })?;
                            responses.prepend(Box::pin(async move {
                                one_rx.await.map_err(|e| anyhow!("{}", e))
                            }));
                        }
                        _ => response_buffer.push(response_m),
                    }
                }
                None => break,
            }
        }
        return Ok(Messages {
            messages: response_buffer,
        });
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
