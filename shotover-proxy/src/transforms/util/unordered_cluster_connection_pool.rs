use crate::protocols::Frame;
use crate::server::CodecReadHalf;
use crate::server::CodecWriteHalf;
use crate::transforms::util::{Request, Response};
use crate::{message::Message, server::Codec};

use anyhow::{anyhow, Result};
use futures::StreamExt;
use multimap::MultiMap;
use std::fmt;
use std::fmt::Formatter;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{info, Instrument};

#[derive(Clone)]
pub struct OwnedUnorderedConnectionPool<C: Codec> {
    host: String,
    pub connections: Vec<UnboundedSender<Request>>,
    codec: C,
    auth_func: fn(&OwnedUnorderedConnectionPool<C>, &mut UnboundedSender<Request>) -> Result<()>,
}

impl<C: Codec> fmt::Debug for OwnedUnorderedConnectionPool<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("host_set", &self.host)
            .field("queue_map", &self.connections)
            .finish()
    }
}

impl<C: Codec + 'static> OwnedUnorderedConnectionPool<C> {
    pub fn new(host: String, codec: C) -> Self {
        OwnedUnorderedConnectionPool {
            host,
            connections: Vec::new(),
            codec,
            auth_func: |_, _| Ok(()),
        }
    }

    pub fn new_with_auth(
        host: String,
        codec: C,
        auth_func: fn(
            &OwnedUnorderedConnectionPool<C>,
            &mut UnboundedSender<Request>,
        ) -> Result<()>,
    ) -> Self {
        OwnedUnorderedConnectionPool {
            host,
            connections: Vec::new(),
            codec,
            auth_func,
        }
    }

    pub async fn connect(&mut self, connection_count: i32) -> Result<()> {
        let mut connection_pool: Vec<UnboundedSender<Request>> = Vec::new();

        for _i in 0..connection_count {
            let socket: TcpStream = TcpStream::connect(self.host.clone()).await?;
            let (read, write) = socket.into_split();
            let (mut out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
            let (return_tx, return_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();

            tokio::spawn(
                tx_process(write, out_rx, return_tx, self.codec.clone()).in_current_span(),
            );

            tokio::spawn(rx_process(read, return_rx, self.codec.clone()).in_current_span());
            match (self.auth_func)(self, &mut out_tx) {
                Ok(_) => {
                    connection_pool.push(out_tx);
                }
                Err(e) => {
                    info!("Could not authenticate to upstream TCP service - {}", e);
                }
            }
        }

        if connection_pool.is_empty() {
            Err(anyhow!("Couldn't connect to upstream TCP service"))
        } else {
            self.connections = connection_pool;
            Ok(())
        }
    }
}

async fn tx_process<C: CodecWriteHalf>(
    write: OwnedWriteHalf,
    out_rx: UnboundedReceiver<Request>,
    return_tx: UnboundedSender<Request>,
    codec: C,
) -> Result<()> {
    let in_w = FramedWrite::new(write, codec);
    let rx_stream = UnboundedReceiverStream::new(out_rx).map(|x| {
        let ret = Ok(vec![x.messages.clone()]);
        return_tx.send(x)?;
        ret
    });
    rx_stream.forward(in_w).await?;
    Ok(())
}

async fn rx_process<C: CodecReadHalf>(
    read: OwnedReadHalf,
    mut return_rx: UnboundedReceiver<Request>,
    codec: C,
) -> Result<()> {
    let mut in_r = FramedRead::new(read, codec);
    let mut return_channel_map: MultiMap<i16, (tokio::sync::oneshot::Sender<Response>, Message)> =
        MultiMap::new();

    let mut return_message_map: MultiMap<i16, Message> = MultiMap::new();

    loop {
        tokio::select! {
            Some(maybe_req) = in_r.next() => {
                match maybe_req {
                    Ok(req) => {
                        for m in req {
                            if let Frame::Cassandra(frame) = &m.original {
                                match return_channel_map.remove_first(&frame.stream_id) {
                                    None => {
                                        return_message_map.insert(frame.stream_id, m);
                                    },
                                    Some((ret, orig)) => {
                                        ret.send((orig, Ok(vec![m] ))).map_err(|_| anyhow!("couldn't send message"))?;
                                    }
                                };
                            }
                        }
                    }
                    Err(e) => {
                        info!("Couldn't decode message from upstream host {:?}", e);
                        return Err(anyhow!("Couldn't decode message from upstream host {:?}", e));
                    }
                }
            },
            Some(original_request) = return_rx.recv() => {
                if let Request { messages: orig , return_chan: Some(chan), message_id: Some(id) } = original_request {
                    match return_message_map.remove_first(&id) {
                        None => {
                            return_channel_map.insert(id, (chan, orig));
                        }
                        Some(m) => {
                            chan.send((orig, Ok(vec![m])))
                                .map_err(|_| anyhow!("couldn't send message"))?;
                        }
                    };
                } else {
                   panic!("Couldn't get valid cassandra stream id");
                }
            },
            else => {
                break
            }
        }
    }
    Ok(())
}
