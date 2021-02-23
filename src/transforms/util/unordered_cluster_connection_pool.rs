use crate::error::ChainResponse;
use crate::message::{Message, Messages};
use crate::protocols::RawFrame;
use crate::transforms::util::Request;
use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::iter::FromIterator;
use std::sync::Arc;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use tracing::{debug, info};

#[derive(Clone)]
pub struct UnorderedConnectionPool<C>
where
    C: Decoder<Item = Messages> + Encoder<Messages, Error = anyhow::Error> + Clone + Send,
{
    host_set: Arc<Mutex<HashSet<String>>>,
    queue_map: Arc<Mutex<HashMap<String, Vec<UnboundedSender<Request>>>>>,
    codec: C,
    auth_func: fn(&UnorderedConnectionPool<C>, &mut UnboundedSender<Request>) -> Result<()>,
}

impl<C> fmt::Debug for UnorderedConnectionPool<C>
where
    C: Decoder<Item = Messages> + Encoder<Messages, Error = anyhow::Error> + Clone + Send,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("host_set", &self.host_set)
            .field("queue_map", &self.queue_map)
            .finish()
    }
}

impl<C: 'static> UnorderedConnectionPool<C>
where
    C: Decoder<Item = Messages> + Encoder<Messages, Error = anyhow::Error> + Clone + Send,
    <C as Decoder>::Error: std::fmt::Debug + Send,
{
    pub fn new(hosts: Vec<String>, codec: C) -> Self {
        UnorderedConnectionPool {
            host_set: Arc::new(Mutex::new(HashSet::from_iter(hosts.into_iter()))),
            queue_map: Arc::new(Mutex::new(HashMap::new())),
            codec,
            auth_func: |_, _| Ok(()),
        }
    }

    pub fn new_with_auth(
        hosts: Vec<String>,
        codec: C,
        auth_func: fn(&UnorderedConnectionPool<C>, &mut UnboundedSender<Request>) -> Result<()>,
    ) -> Self {
        UnorderedConnectionPool {
            host_set: Arc::new(Mutex::new(HashSet::from_iter(hosts.into_iter()))),
            queue_map: Arc::new(Mutex::new(HashMap::new())),
            codec,
            auth_func,
        }
    }

    /// Try and grab an existing connection, if it's closed (e.g. the listener on the other side
    /// has closed due to a TCP error), we'll try to reconnect and return the new connection while
    /// updating the connection map. Errors are returned when a connection can't be established.
    pub async fn get_connection(
        &self,
        host: &String,
        connection_count: i32,
    ) -> Result<Vec<UnboundedSender<Request>>> {
        let mut queue_map = self.queue_map.lock().await;
        if let Some(x) = queue_map.get(host) {
            if x.iter().all(|x| !x.is_closed()) {
                return Ok(x.clone());
            }
        }
        let connection = self.connect(&host, connection_count).await?;
        queue_map.insert(host.clone(), connection.clone());
        return Ok(connection);
    }

    pub async fn connect(
        &self,
        host: &String,
        connection_count: i32,
    ) -> Result<Vec<UnboundedSender<Request>>>
    where
        <C as Decoder>::Error: std::marker::Send,
    {
        let mut connection_pool: Vec<UnboundedSender<Request>> = Vec::new();

        for _i in 0..connection_count {
            let socket: TcpStream = TcpStream::connect(host).await?;
            let (read, write) = socket.into_split();
            let (mut out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
            let (return_tx, return_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();

            tokio::spawn(tx_process(write, out_rx, return_tx, self.codec.clone()));

            tokio::spawn(rx_process(read, return_rx, self.codec.clone()));
            match (self.auth_func)(&self, &mut out_tx) {
                Ok(_) => {
                    connection_pool.push(out_tx);
                }
                Err(e) => {
                    info!("Could not authenticate to upstream TCP service - {}", e);
                }
            }
        }

        if connection_pool.len() == 0 {
            return Err(anyhow!("Couldn't connect to upstream TCP service"));
        }

        Ok(connection_pool)
    }
}

async fn tx_process<C>(
    write: OwnedWriteHalf,
    out_rx: UnboundedReceiver<Request>,
    return_tx: UnboundedSender<Request>,
    codec: C,
) -> Result<()>
where
    C: Encoder<Messages, Error = anyhow::Error> + Clone + Send + 'static,
{
    let codec = codec.clone();
    let in_w = FramedWrite::new(write, codec);
    let rx_stream = UnboundedReceiverStream::new(out_rx).map(|x| {
        let ret = Ok(Messages {
            messages: vec![x.messages.clone()],
        });
        return_tx.send(x)?;
        ret
    });
    rx_stream.forward(in_w).await?;
    Ok(())
}

async fn rx_process<C>(
    read: OwnedReadHalf,
    mut return_rx: UnboundedReceiver<Request>,
    codec: C,
) -> Result<()>
where
    C: Decoder<Item = Messages> + Clone + Send + 'static,
    <C as Decoder>::Error: std::fmt::Debug + Send,
{
    let codec = codec.clone();
    let mut in_r = FramedRead::new(read, codec);
    let mut message_map: HashMap<u16, Request> = HashMap::new();
    loop {
        tokio::select! {
        Some(maybe_req) = in_r.next() => {
            match maybe_req {
                Ok(req) => {
                    for m in req {
                        if let RawFrame::CASSANDRA(frame) = &m.original {
                            loop {
                                match message_map.remove(&frame.stream) {
                                    None => tokio::task::yield_now().await,
                                    Some(Request {
                                        messages,
                                        return_chan: Some(ret),
                                        message_id,
                                    }) => {
                                        ret.send((messages, Ok(Messages { messages: vec![m] })));
                                        break;
                                    }
                                    Some(Request {
                                        messages: _,
                                        return_chan: None,
                                        message_id: _,
                                    }) => {
                                        info!("No return channel found for Cassandra response");
                                        break;
                                    }
                                };
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("Couldn't decode message from upstream host {:?}", e);
                    return Err(anyhow!(
                        "Couldn't decode message from upstream host {:?}",
                        e
                    ));
                }
            }
        },
        Some(original_request) = return_rx.recv() => {
            if let RawFrame::CASSANDRA(frame) = &original_request.messages.original {
                message_map.insert(frame.stream, original_request);
            } else {
                panic!("Couldn't get valid cassandra stream id");
            }
        },
        else => break
        }
    }
    Ok(())
}
