use crate::codec::cassandra::CassandraCodec;
use crate::frame::cassandra::CassandraMetadata;
use crate::message::{Message, Metadata};
use crate::tls::TlsConnector;
use crate::transforms::util::Response;
use crate::transforms::Messages;
use anyhow::{anyhow, Result};
use cassandra_protocol::frame::Opcode;
use derivative::Derivative;
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use halfbrown::HashMap;
use std::time::Duration;
use tokio::io::{split, AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{error, Instrument};

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
    connection: mpsc::UnboundedSender<Request>,
}

impl CassandraConnection {
    pub async fn new<A: ToSocketAddrs + std::fmt::Debug>(
        host: A,
        codec: CassandraCodec,
        mut tls: Option<TlsConnector>,
        pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    ) -> Result<Self> {
        let tcp_stream = timeout(Duration::from_secs(3), TcpStream::connect(&host))
            .await
            .map_err(|_| {
                anyhow!(
                    "Cassandra node at {:?} did not respond to connection attempt within 3 seconds",
                    host
                )
            })?
            .map_err(|e| {
                anyhow::Error::new(e)
                    .context(format!("Failed to connect to cassandra node: {:?}", host))
            })?;

        let (out_tx, out_rx) = mpsc::unbounded_channel::<Request>();
        let (return_tx, return_rx) = mpsc::unbounded_channel::<Request>();

        if let Some(tls) = tls.as_mut() {
            let tls_stream = tls.connect(tcp_stream).await?;
            let (read, write) = split(tls_stream);
            tokio::spawn(tx_process(write, out_rx, return_tx, codec.clone()).in_current_span());
            tokio::spawn(
                rx_process(read, return_rx, codec.clone(), pushed_messages_tx).in_current_span(),
            );
        } else {
            let (read, write) = split(tcp_stream);
            tokio::spawn(tx_process(write, out_rx, return_tx, codec.clone()).in_current_span());
            tokio::spawn(
                rx_process(read, return_rx, codec.clone(), pushed_messages_tx).in_current_span(),
            );
        };

        Ok(CassandraConnection { connection: out_tx })
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

async fn tx_process<T: AsyncWrite>(
    write: WriteHalf<T>,
    out_rx: mpsc::UnboundedReceiver<Request>,
    return_tx: mpsc::UnboundedSender<Request>,
    codec: CassandraCodec,
) {
    if let Err(err) = tx_process_fallible(write, out_rx, return_tx, codec).await {
        error!("{:?}", err.context("tx_process task terminated"));
    }
}

async fn tx_process_fallible<T: AsyncWrite>(
    write: WriteHalf<T>,
    out_rx: mpsc::UnboundedReceiver<Request>,
    return_tx: mpsc::UnboundedSender<Request>,
    codec: CassandraCodec,
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

async fn rx_process<T: AsyncRead>(
    read: ReadHalf<T>,
    return_rx: mpsc::UnboundedReceiver<Request>,
    codec: CassandraCodec,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
) {
    if let Err(err) = rx_process_fallible(read, return_rx, codec, pushed_messages_tx).await {
        error!("{:?}", err.context("rx_process task terminated"));
    }
}

async fn rx_process_fallible<T: AsyncRead>(
    read: ReadHalf<T>,
    mut return_rx: mpsc::UnboundedReceiver<Request>,
    codec: CassandraCodec,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
) -> Result<()> {
    let mut reader = FramedRead::new(read, codec);
    let mut return_channel_map: HashMap<i16, (oneshot::Sender<Response>, Message)> = HashMap::new();

    let mut return_message_map: HashMap<i16, Message> = HashMap::new();

    loop {
        tokio::select! {
            Some(response) = reader.next() => {
                match response {
                    Ok(response) => {
                        for m in response {
                            if let Ok(Metadata::Cassandra(CassandraMetadata { opcode: Opcode::Event, .. })) = m.metadata() {
                                if let Some(ref pushed_messages_tx) = pushed_messages_tx {
                                    pushed_messages_tx.send(vec![m]).unwrap();
                                }
                            } else if let Some(stream_id) = m.stream_id() {
                                match return_channel_map.remove(&stream_id) {
                                    None => {
                                        return_message_map.insert(stream_id, m);
                                    },
                                    Some((return_tx, original)) => {
                                        return_tx.send(Response { original, response: Ok(m) })
                                            .map_err(|_| anyhow!("couldn't send message"))?;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        return Err(anyhow!("{:?}", e).context("Encountered error while communicating with destination cassandra node"));
                    }
                }
            },
            Some(original_request) = return_rx.recv() => {
                let Request { message, return_chan, message_id } = original_request;
                match return_message_map.remove(&message_id) {
                    None => {
                        return_channel_map.insert(message_id, (return_chan, message));
                    }
                    Some(m) => {
                        return_chan.send(Response { original: message, response: Ok(m) })
                            .map_err(|_| anyhow!("couldn't send message"))?;
                    }
                };
            },
            else => {
                return Ok(())
            }
        }
    }
}

pub async fn receive(
    timeout_duration: Option<Duration>,
    failed_requests: &metrics::Counter,
    mut results: FuturesOrdered<oneshot::Receiver<Response>>,
) -> Result<Messages> {
    let expected_size = results.len();
    let mut responses = Vec::with_capacity(expected_size);
    while responses.len() < expected_size {
        if let Some(timeout_duration) = timeout_duration {
            match timeout(
                timeout_duration,
                receive_message(failed_requests, &mut results),
            )
            .await
            {
                Ok(response) => {
                    responses.push(response?);
                }
                Err(_) => {
                    return Err(anyhow!(
                        "timed out waiting for responses, received {:?} responses but expected {:?} responses",
                        responses.len(),
                        expected_size
                    ));
                }
            }
        } else {
            responses.push(receive_message(failed_requests, &mut results).await?);
        }
    }
    Ok(responses)
}

pub async fn receive_message(
    failed_requests: &metrics::Counter,
    results: &mut FuturesOrdered<oneshot::Receiver<Response>>,
) -> Result<Message> {
    match results.next().await {
        Some(result) => match result? {
            Response {
                response: Ok(message),
                ..
            } => {
                if let Ok(Metadata::Cassandra(CassandraMetadata {
                    opcode: Opcode::Error,
                    ..
                })) = message.metadata()
                {
                    failed_requests.increment(1);
                }
                Ok(message)
            }
            Response {
                mut original,
                response: Err(err),
            } => {
                original.set_error(err.to_string());
                Ok(original)
            }
        },
        None => unreachable!("Ran out of responses"),
    }
}
