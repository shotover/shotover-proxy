// use crate::message::{Message, Messages};
// use crate::protocols::RawFrame;
use crate::util::{Request, Response};
use crate::RawFrame;
use crate::{Message, Messages};
use anyhow::{anyhow, Result};
use futures::StreamExt;
use halfbrown::HashMap;
use std::fmt;
use std::fmt::Formatter;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use tracing::info;

#[derive(Clone)]
pub struct OwnedUnorderedConnectionPool<C>
where
    C: Decoder<Item = Messages> + Encoder<Messages, Error = anyhow::Error> + Clone + Send,
{
    host: String,
    pub connections: Vec<UnboundedSender<Request>>,
    codec: C,
    auth_func: fn(&OwnedUnorderedConnectionPool<C>, &mut UnboundedSender<Request>) -> Result<()>,
}

impl<C> fmt::Debug for OwnedUnorderedConnectionPool<C>
where
    C: Decoder<Item = Messages> + Encoder<Messages, Error = anyhow::Error> + Clone + Send,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("host_set", &self.host)
            .field("queue_map", &self.connections)
            .finish()
    }
}

impl<C: 'static> OwnedUnorderedConnectionPool<C>
where
    C: Decoder<Item = Messages> + Encoder<Messages, Error = anyhow::Error> + Clone + Send,
    <C as Decoder>::Error: std::fmt::Debug + Send,
{
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

    pub async fn connect(&mut self, connection_count: i32) -> Result<()>
    where
        <C as Decoder>::Error: std::marker::Send,
    {
        let mut connection_pool: Vec<UnboundedSender<Request>> = Vec::new();

        for _i in 0..connection_count {
            let socket: TcpStream = TcpStream::connect(self.host.clone()).await?;
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

        self.connections = connection_pool;

        Ok(())
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
    let mut return_channel_map: HashMap<u16, (tokio::sync::oneshot::Sender<Response>, Message)> =
        HashMap::new();

    let mut return_message_map: HashMap<u16, Message> = HashMap::new();

    // let foo = timeout(Duration::from_millis(10), )

    loop {
        tokio::select! {
            Some(maybe_req) = in_r.next() => {

                        if return_message_map.len() > 1 || return_channel_map.len() > 1 {
                info!("message map {:?}", return_message_map);
                info!("channel map {:?}", return_channel_map);
            }
                match maybe_req {
                    Ok(req) => {
                        for m in req {
                            if let RawFrame::CASSANDRA(frame) = &m.original {
                                match return_channel_map.remove(&frame.stream) {
                                    None => {
                                        return_message_map.insert(frame.stream, m);
                                    },
                                    Some((ret, orig)) => {
                                        ret.send((orig, Ok(Messages { messages: vec![m] }))).map_err(|_| anyhow!("couldn't send message"))?;
                                    }
                                };
                            }
                        }
                    }
                    Err(e) => {
                        info!("Couldn't decode message from upstream host {:?}", e);
                        return Err(anyhow!(
                                "Couldn't decode message from upstream host {:?}",
                                e
                            ));
                    }
                }
            },
            Some(original_request) = return_rx.recv() => {
            if return_message_map.len() > 1 || return_channel_map.len() > 1 {
                info!("message map {:?}", return_message_map);
                info!("channel map {:?}", return_channel_map);
            }
                if let Request { messages: orig , return_chan: Some(chan), message_id: Some(id) } = original_request {
                    match return_message_map.remove(&id) {
                        None => {
                            return_channel_map.insert(id, (chan, orig));
                        }
                        Some(m) => {
                            chan.send((orig, Ok(Messages { messages: vec![m] })))
                                .map_err(|_| anyhow!("couldn't send message"))?;
                        }
                    };
                } else {
                   panic!("Couldn't get valid cassandra stream id");
                }
            },
            else => {
                // info!("tjos happened");
                break
            }
        }
    }
    Ok(())
}
