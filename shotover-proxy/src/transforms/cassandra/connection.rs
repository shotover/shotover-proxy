use crate::message::Message;
use crate::server::Codec;
use crate::server::CodecReadHalf;
use crate::server::CodecWriteHalf;
use crate::tls::TlsConnector;
use crate::transforms::util::Response;
use anyhow::{anyhow, Result};
use derivative::Derivative;
use futures::StreamExt;
use halfbrown::HashMap;
use tokio::io::{split, AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
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
pub struct CassandraConnection {
    host: String,
    connection: mpsc::UnboundedSender<Request>,
}

impl CassandraConnection {
    pub async fn new<C: Codec + 'static>(
        host: String,
        codec: C,
        mut tls: Option<TlsConnector>,
    ) -> Result<Self> {
        let tcp_stream: TcpStream = TcpStream::connect(&host).await?;

        let (out_tx, out_rx) = mpsc::unbounded_channel::<Request>();
        let (return_tx, return_rx) = mpsc::unbounded_channel::<Request>();

        if let Some(tls) = tls.as_mut() {
            let tls_stream = tls.connect(tcp_stream).await?;
            let (read, write) = split(tls_stream);
            tokio::spawn(tx_process(write, out_rx, return_tx, codec.clone()).in_current_span());
            tokio::spawn(rx_process(read, return_rx, codec.clone()).in_current_span());
        } else {
            let (read, write) = split(tcp_stream);
            tokio::spawn(tx_process(write, out_rx, return_tx, codec.clone()).in_current_span());
            tokio::spawn(rx_process(read, return_rx, codec.clone()).in_current_span());
        };

        Ok(CassandraConnection {
            host,
            connection: out_tx,
        })
    }

    /// Send a `Message` to this `CassandraConnection` and expect a response on `return_chan`
    pub fn send(&self, message: Message, return_chan: oneshot::Sender<Response>) -> Result<()> {
        // Convert the message to `Request` and send upstream
        if let Some(message_id) = message.stream_id() {
            self.connection
                .send(Request {
                    message,
                    return_chan,
                    message_id,
                })
                .map_err(|x| x.into())
        } else {
            Err(anyhow!("no cassandra frame found"))
        }
    }
}

async fn tx_process<C: CodecWriteHalf, T: AsyncWrite>(
    write: WriteHalf<T>,
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

async fn rx_process<C: CodecReadHalf, T: AsyncRead>(
    read: ReadHalf<T>,
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
                            if let Some(stream_id) = m.stream_id() {
                                match return_channel_map.remove(&stream_id) {
                                    None => {
                                        return_message_map.insert(stream_id, m);
                                    },
                                    Some((return_tx, original)) => {
                                        return_tx.send(Response {original, response: Ok(vec![m]) })
                                        .map_err(|_| anyhow!("couldn't send message"))?;
                                    }
                                }
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
                        return_chan.send(Response { original: message, response: Ok(vec![m]) })
                            .map_err(|_| anyhow!("couldn't send message"))?;
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
