use crate::codec::cassandra::CassandraCodec;
use crate::frame::cassandra::CassandraMetadata;
use crate::message::{Message, Metadata};
use crate::server::CodecReadError;
use crate::tcp;
use crate::tls::{TlsConnector, ToHostname};
use crate::transforms::util::Response;
use crate::transforms::Messages;
use anyhow::{anyhow, Result};
use cassandra_protocol::frame::Opcode;
use derivative::Derivative;
use futures::stream::FuturesOrdered;
use futures::{SinkExt, StreamExt};
use halfbrown::HashMap;
use std::time::Duration;
use tokio::io::{split, AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::net::ToSocketAddrs;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
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
    pub async fn new<A: ToSocketAddrs + ToHostname + std::fmt::Debug>(
        connect_timeout: Duration,
        host: A,
        codec: CassandraCodec,
        mut tls: Option<TlsConnector>,
        pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    ) -> Result<Self> {
        let (out_tx, out_rx) = mpsc::unbounded_channel::<Request>();
        let (return_tx, return_rx) = mpsc::unbounded_channel::<Request>();
        let (rx_process_has_shutdown_tx, rx_process_has_shutdown_rx) = oneshot::channel::<()>();

        if let Some(tls) = tls.as_mut() {
            let tls_stream = tls.connect(connect_timeout, host).await?;
            let (read, write) = split(tls_stream);
            tokio::spawn(
                tx_process(
                    write,
                    out_rx,
                    return_tx,
                    codec.clone(),
                    rx_process_has_shutdown_rx,
                )
                .in_current_span(),
            );
            tokio::spawn(
                rx_process(
                    read,
                    return_rx,
                    codec.clone(),
                    pushed_messages_tx,
                    rx_process_has_shutdown_tx,
                )
                .in_current_span(),
            );
        } else {
            let tcp_stream = tcp::tcp_stream(connect_timeout, host).await?;
            let (read, write) = split(tcp_stream);
            tokio::spawn(
                tx_process(
                    write,
                    out_rx,
                    return_tx,
                    codec.clone(),
                    rx_process_has_shutdown_rx,
                )
                .in_current_span(),
            );
            tokio::spawn(
                rx_process(
                    read,
                    return_rx,
                    codec.clone(),
                    pushed_messages_tx,
                    rx_process_has_shutdown_tx,
                )
                .in_current_span(),
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
    rx_process_has_shutdown_rx: oneshot::Receiver<()>,
) {
    if let Err(err) =
        tx_process_fallible(write, out_rx, return_tx, codec, rx_process_has_shutdown_rx).await
    {
        error!("{:?}", err.context("tx_process task terminated"));
    }
}

async fn tx_process_fallible<T: AsyncWrite>(
    write: WriteHalf<T>,
    mut out_rx: mpsc::UnboundedReceiver<Request>,
    return_tx: mpsc::UnboundedSender<Request>,
    codec: CassandraCodec,
    rx_process_has_shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    let mut in_w = FramedWrite::new(write, codec);
    loop {
        if let Some(request) = out_rx.recv().await {
            in_w.send(vec![request.message.clone()]).await?;
            return_tx.send(request)?;
        } else {
            // transform is shutting down, time to cleanly shutdown both tx_process and rx_process.
            // We need to ensure that the rx_process task has shutdown before closing the write half of the tcpstream
            // If we dont do this, rx_process may attempt to read from the tcp stream after the write half has closed.
            // Closing the write half will send a TCP FIN ACK to the server.
            // The server may then respond with a TCP RST, after which any reads from the read half would return a ConnectionReset error

            // first we drop return_tx which will instruct rx_process to shutdown
            std::mem::drop(return_tx);

            // wait for rx_process to shutdown
            rx_process_has_shutdown_rx.await.ok();

            // Now that rx_process is shutdown we can safely drop the write half of the
            // tcp stream without the read half hitting errors due to the connection being closed or reset.
            std::mem::drop(in_w);
            return Ok(());
        }
    }
}

async fn rx_process<T: AsyncRead>(
    read: ReadHalf<T>,
    return_rx: mpsc::UnboundedReceiver<Request>,
    codec: CassandraCodec,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    rx_process_has_shutdown_tx: oneshot::Sender<()>,
) {
    if let Err(err) = rx_process_fallible(read, return_rx, codec, pushed_messages_tx).await {
        error!("{:?}", err.context("rx_process task terminated"));
    }

    // Just dropping this is enough to notify of shutdown
    std::mem::drop(rx_process_has_shutdown_tx);
}

async fn rx_process_fallible<T: AsyncRead>(
    read: ReadHalf<T>,
    mut return_rx: mpsc::UnboundedReceiver<Request>,
    codec: CassandraCodec,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
) -> Result<()> {
    let mut reader = FramedRead::new(read, codec);

    // Invariants:
    // * client must not reuse a stream_id until the client has received a response with that stream_id (not required by the protocol)
    // * every response from the server must match the stream_id of the request it is responding to (required by the protocol)
    //     + events are not responses and so dont follow this invariant
    //
    // Implementation:
    // To process a message we need to receive things from two different sources:
    // 1. the response from the cassandra server
    // 2. the oneshot::Sender and original message from the tx_process task
    //
    // We can receive these in any order.
    // In order to handle that we have two seperate maps.
    //
    // We store the sender + original message here if we receive from the tx_process task first
    let mut from_tx_process: HashMap<i16, (oneshot::Sender<Response>, Message)> = HashMap::new();

    // We store the response message here if we receive from the server first.
    let mut from_server: HashMap<i16, Message> = HashMap::new();

    loop {
        tokio::select! {
            response = reader.next() => {
                match response {
                    Some(Ok(response)) => {
                        for m in response {
                            if let Ok(Metadata::Cassandra(CassandraMetadata { opcode: Opcode::Event, .. })) = m.metadata() {
                                if let Some(pushed_messages_tx) = pushed_messages_tx.as_ref() {
                                    pushed_messages_tx.send(vec![m]).unwrap();
                                }
                            } else if let Some(stream_id) = m.stream_id() {
                                match from_tx_process.remove(&stream_id) {
                                    None => {
                                        from_server.insert(stream_id, m);
                                    },
                                    Some((return_tx, original)) => {
                                        return_tx.send(Response { original, response: Ok(m) })
                                            .map_err(|_| anyhow!("couldn't send message"))?;
                                    }
                                }
                            }
                        }
                    }
                    Some(Err(CodecReadError::Io(err))) => {
                        return Err(anyhow!(err)
                            .context("Encountered IO error while communicating with destination cassandra node"))
                    }
                    Some(Err(err)) => {
                        return Err(anyhow!("{:?}", err).context("Encountered error while communicating with destination cassandra node"));
                    }
                    None => return Ok(())
                }
            },
            original_request = return_rx.recv() => {
                if let Some(Request { message, return_chan, message_id }) = original_request {
                    match from_server.remove(&message_id) {
                        None => {
                            from_tx_process.insert(message_id, (return_chan, message));
                        }
                        Some(m) => {
                            return_chan.send(Response { original: message, response: Ok(m) })
                                .map_err(|_| anyhow!("couldn't send message"))?;
                        }
                    }
                } else {
                    return Ok(())
                }
            },
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
