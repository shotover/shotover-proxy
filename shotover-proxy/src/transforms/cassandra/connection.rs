use crate::protocols::Frame;
use crate::server::CodecReadHalf;
use crate::server::CodecWriteHalf;
use crate::transforms::util::Response;
use crate::{message::Message, server::Codec};

use anyhow::{anyhow, Result};
use derivative::Derivative;
use futures::StreamExt;
use halfbrown::HashMap;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{info, Instrument};

/// Represents a `Request` to a `CassandraConnection`
#[derive(Debug)]
struct Request {
    message: Message,
    return_chan: oneshot::Sender<Response>,
    message_id: i16,
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct CassandraConnection<C: Codec> {
    host: String,
    connection: Option<mpsc::UnboundedSender<Request>>,
    #[derivative(Debug = "ignore")]
    codec: C,
}

impl<C: Codec + 'static> CassandraConnection<C> {
    pub fn new(host: String, codec: C) -> Self {
        CassandraConnection {
            host,
            connection: None,
            codec,
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        let socket: TcpStream = TcpStream::connect(self.host.clone()).await?;
        let (read, write) = socket.into_split();
        let (out_tx, out_rx) = mpsc::unbounded_channel::<Request>();
        let (return_tx, return_rx) = mpsc::unbounded_channel::<Request>();

        tokio::spawn(tx_process(write, out_rx, return_tx, self.codec.clone()).in_current_span());

        tokio::spawn(rx_process(read, return_rx, self.codec.clone()).in_current_span());
        self.connection = Some(out_tx);
        Ok(())
    }

    /// Send a `Message` to this `CassandraConnection` and expect a response on `return_chan`
    pub fn send(&self, message: Message, return_chan: oneshot::Sender<Response>) -> Result<()> {
        let message_id = if let Frame::Cassandra(frame) = &message.original {
            frame.stream_id
        } else {
            return Err(anyhow!("no cassandra frame found"));
        };

        let connection = self.connection.as_ref().expect("No connection found");

        // Convert the message to `Request` and send upstream
        Ok(connection.send(Request {
            message,
            return_chan,
            message_id,
        })?)
    }
}

async fn tx_process<C: CodecWriteHalf>(
    write: OwnedWriteHalf,
    out_rx: mpsc::UnboundedReceiver<Request>,
    return_tx: mpsc::UnboundedSender<Request>,
    codec: C,
) -> Result<()> {
    let in_w = FramedWrite::new(write, codec);
    let rx_stream = UnboundedReceiverStream::new(out_rx).map(|x| {
        let ret = Ok(vec![x.message.clone()]);
        return_tx.send(x)?;
        ret
    });
    rx_stream.forward(in_w).await?;
    Ok(())
}

async fn rx_process<C: CodecReadHalf>(
    read: OwnedReadHalf,
    mut return_rx: mpsc::UnboundedReceiver<Request>,
    codec: C,
) -> Result<()> {
    let mut in_r = FramedRead::new(read, codec);
    let mut return_channel_map: HashMap<i16, (oneshot::Sender<Response>, Message)> = HashMap::new();

    let mut return_message_map: HashMap<i16, Message> = HashMap::new();

    loop {
        tokio::select! {
            Some(maybe_req) = in_r.next() => {
                match maybe_req {
                    Ok(req) => {
                        for m in req {
                            if let Frame::Cassandra(frame) = &m.original {
                                match return_channel_map.remove(&frame.stream_id) {
                                    None => {
                                        return_message_map.insert(frame.stream_id, m);
                                    },
                                    Some((return_tx, original)) => {
                                        return_tx.send(Response {original, response: Ok(vec![m]) }).map_err(|_| anyhow!("couldn't send message"))?;
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
                let Request { message, return_chan, message_id} = original_request;
                match return_message_map.remove(&message_id) {
                    None => {
                        return_channel_map.insert(message_id, (return_chan, message));
                    }
                    Some(m) => {
                        return_chan.send(Response{ original: message, response: Ok(vec![m]) }).map_err(|_| anyhow!("couldn't send message"))?;
                    }
                };
            },
            else => {
                break
            }
        }
    }
    Ok(())
}
