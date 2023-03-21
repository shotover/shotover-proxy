use crate::codec::cassandra::{CassandraCodecBuilder, CassandraDecoder, CassandraEncoder};
use crate::codec::{CodecBuilder, CodecReadError};
use crate::frame::cassandra::CassandraMetadata;
use crate::frame::{CassandraFrame, Frame};
use crate::message::{Message, Metadata};
use crate::tcp;
use crate::tls::{TlsConnector, ToHostname};
use crate::transforms::Messages;
use anyhow::{anyhow, Result};
use cassandra_protocol::frame::{Opcode, Version};
use derivative::Derivative;
use futures::stream::FuturesOrdered;
use futures::{SinkExt, StreamExt};
use halfbrown::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{split, AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::net::ToSocketAddrs;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::Instrument;

#[derive(Debug)]
struct Request {
<<<<<<< v5-compression
    messages: Messages,
    return_chans_tx: Vec<oneshot::Sender<Response>>,
    stream_ids: Vec<i16>,
||||||| ancestor
    message: Message,
    return_chan: oneshot::Sender<Response>,
    stream_id: i16,
=======
    messages: Messages,
    return_chans_tx: Vec<oneshot::Sender<Response>>,
    stream_ids: Vec<i16>,
>>>>>>> cassandra connection pass multiple messages to codec
}

pub type Response = Result<Message, ResponseError>;

#[derive(Debug, thiserror::Error)]
#[error("Connection to destination cassandra node {destination} was closed: {cause:?}")]
pub struct ResponseError {
    #[source]
    pub cause: anyhow::Error,
    pub destination: SocketAddr,
    pub stream_id: i16,
}

impl ResponseError {
    pub fn to_response(&self, version: Version) -> Message {
        Message::from_frame(Frame::Cassandra(CassandraFrame::shotover_error(
            self.stream_id,
            version,
            &format!("{}", self),
        )))
    }
}

#[derive(Debug)]
struct ReturnChannel {
    return_chans: Vec<oneshot::Sender<Response>>,
    stream_ids: Vec<i16>,
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct CassandraConnection {
    connection: mpsc::UnboundedSender<Request>,
}

impl CassandraConnection {
    /// If any cassandra events are received they are sent on the pushed_messages_tx field.
    /// If the Receiver corresponding to pushed_messages_tx is dropped CassandraConnection will stop sending events but will otherwise function normally.
    pub async fn new<A: ToSocketAddrs + ToHostname + std::fmt::Debug>(
        connect_timeout: Duration,
        host: A,
        codec: CassandraCodecBuilder,
        mut tls: Option<TlsConnector>,
        pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    ) -> Result<Self> {
        let (out_tx, out_rx) = mpsc::unbounded_channel::<Request>();
        let (return_tx, return_rx) = mpsc::unbounded_channel::<ReturnChannel>();
        let (rx_process_has_shutdown_tx, rx_process_has_shutdown_rx) = oneshot::channel::<String>();

        let destination = tokio::net::lookup_host(&host).await?.next().unwrap();

        let (decoder, encoder) = codec.build();
        if let Some(tls) = tls.as_mut() {
            let tls_stream = tls.connect(connect_timeout, host).await?;
            let (read, write) = split(tls_stream);
            tokio::spawn(
                tx_process(
                    write,
                    out_rx,
                    return_tx,
                    encoder,
                    rx_process_has_shutdown_rx,
                    destination,
                )
                .in_current_span(),
            );
            tokio::spawn(
                rx_process(
                    read,
                    return_rx,
                    decoder,
                    pushed_messages_tx,
                    rx_process_has_shutdown_tx,
                    destination,
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
                    encoder,
                    rx_process_has_shutdown_rx,
                    destination,
                )
                .in_current_span(),
            );
            tokio::spawn(
                rx_process(
                    read,
                    return_rx,
                    decoder,
                    pushed_messages_tx,
                    rx_process_has_shutdown_tx,
                    destination,
                )
                .in_current_span(),
            );
        };

        Ok(CassandraConnection { connection: out_tx })
    }

    /// Send a `Message` to this `CassandraConnection` and expect a response on `return_chan`
    ///
    /// The return_chan will never be closed without first sending a response.
    /// However there is no internal timeout so the user will likely want to add their own timeout.
    ///
    /// The user is allowed to drop the receive half of return_chan.
    /// In that case the request may or may not succeed but the user will receive no indication of this.
    ///
    /// If an IO error occurs the Response will contain an Err.
    ///
    /// If an internal invariant is broken the internal tasks may panic and external invariants will no longer be upheld.
    /// But this indicates a bug within CassandraConnection and should be fixed here.
<<<<<<< v5-compression
    pub fn send_multiple(
        &self,
        messages: Messages,
        // return_chans: Vec<oneshot::Sender<Response>>,
    ) -> Result<Vec<Result<oneshot::Receiver<Response>>>> {
        let mut return_chans_tx = Vec::<oneshot::Sender<Response>>::new();
        let mut return_chans_rx = Vec::<Result<oneshot::Receiver<Response>>>::new();

        for _ in 0..messages.len() {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            return_chans_tx.push(return_chan_tx);
            return_chans_rx.push(Ok(return_chan_rx));
        }

        let stream_ids = messages
            .iter()
            .map(|message| message.stream_id().unwrap())
            .collect();

        self.connection.send(Request {
            messages,
            return_chans_tx,
            stream_ids,
        })?;

        Ok(return_chans_rx)
    }

||||||| ancestor
=======
    pub fn send_multiple(
        &self,
        messages: Messages,
        // return_chans: Vec<oneshot::Sender<Response>>,
    ) -> Result<Vec<Result<oneshot::Receiver<Response>>>> {
        let mut return_chans_tx = Vec::<oneshot::Sender<Response>>::new();
        let mut return_chans_rx = Vec::<Result<oneshot::Receiver<Response>>>::new();

        for _ in 0..messages.len() {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            return_chans_tx.push(return_chan_tx);
            return_chans_rx.push(Ok(return_chan_rx));
        }

        let stream_ids = messages
            .iter()
            .map(|message| message.stream_id().unwrap())
            .collect();

        self.connection.send(Request {
            messages,
            return_chans_tx,
            stream_ids,
        })?;

        Ok(return_chans_rx)
    }

>>>>>>> cassandra connection pass multiple messages to codec
    pub fn send(&self, message: Message) -> Result<oneshot::Receiver<Response>> {
        let (return_chan_tx, return_chan_rx) = oneshot::channel();
        // Convert the message to `Request` and send upstream
        if let Some(stream_id) = message.stream_id() {
            self.connection
                .send(Request {
<<<<<<< v5-compression
                    messages: vec![message],
                    return_chans_tx: vec![return_chan_tx],
                    stream_ids: vec![stream_id],
||||||| ancestor
                    message,
                    return_chan: return_chan_tx,
                    stream_id,
=======
                    messages: vec![message],
                    return_chans_tx: vec![return_chan_tx],
                    stream_ids: vec![stream_id],
>>>>>>> cassandra connection pass multiple messages to codec
                })
                .map(|_| return_chan_rx)
                .map_err(|x| x.into())
        } else {
            Err(anyhow!(""))
        }
    }
}
// tx and rx task lifetimes:
// * tx task will only shutdown when the user requests it
// * rx task will only shutdown when the tx task requests it or when the rx task hits an IO error
// * tx task will always outlive rx task

async fn tx_process<T: AsyncWrite>(
    write: WriteHalf<T>,
    mut out_rx: mpsc::UnboundedReceiver<Request>,
    return_tx: mpsc::UnboundedSender<ReturnChannel>,
    codec: CassandraEncoder,
    mut rx_process_has_shutdown_rx: oneshot::Receiver<String>,
    // Only used for error reporting
    destination: SocketAddr,
) {
    let mut in_w = FramedWrite::new(write, codec);

    // Continue responding to requests for as long as the CassandraConnection is kept alive
    // If we encounter an IO error the connection is now dead but we must keep the task running so that each request gets a response.
    // Any requests received while the connection is dead will immediately be responded with the error that put the connection into a dead state.
    let mut connection_dead_error: Option<String> = None;
    loop {
        if let Some(request) = out_rx.recv().await {
            if let Some(error) = &connection_dead_error {
<<<<<<< v5-compression
                send_error_to_requests(
                    request.return_chans_tx,
                    request.stream_ids,
                    destination,
                    error,
                );
            } else if let Err(error) = in_w.send(request.messages).await {
||||||| ancestor
                send_error_to_request(request.return_chan, request.stream_id, destination, error);
            } else if let Err(error) = in_w.send(vec![request.message]).await {
=======
                send_error_to_requests(
                    request.return_chans_tx,
                    request.stream_ids,
                    destination,
                    error,
                );
            } else if let Err(error) = in_w.send(request.messages).await {
>>>>>>> cassandra connection pass multiple messages to codec
                let error = format!("{:?}", error);
<<<<<<< v5-compression
                send_error_to_requests(
                    request.return_chans_tx,
                    request.stream_ids,
                    destination,
                    &error,
                );
||||||| ancestor
                send_error_to_request(request.return_chan, request.stream_id, destination, &error);
=======
                send_error_to_requests(
                    request.return_chans_tx,
                    request.stream_ids,
                    destination,
                    &error,
                );
>>>>>>> cassandra connection pass multiple messages to codec
                connection_dead_error = Some(error.clone());
            } else if let Err(mpsc::error::SendError(return_chan)) = return_tx.send(ReturnChannel {
<<<<<<< v5-compression
<<<<<<< v5-compression
                return_chans: request.return_chans_tx,
                stream_ids: request.stream_ids,
||||||| ancestor
                return_chan: request.return_chan,
                stream_id: request.stream_id,
=======
                return_chans: request.return_chans,
||||||| ancestor
                return_chans: request.return_chans,
=======
                return_chans: request.return_chans_tx,
>>>>>>> rebase
                stream_ids: request.stream_ids,
>>>>>>> cassandra connection pass multiple messages to codec
            }) {
                let error = rx_process_has_shutdown_rx
                    .try_recv()
                    .expect("Rx task must send this before closing return_tx");
                send_error_to_requests(
                    return_chan.return_chans,
                    return_chan.stream_ids,
                    destination,
                    &error,
                );
                connection_dead_error = Some(error.clone());
            }
        }
        // CassandraConnection has been dropped, time to cleanly shutdown both tx_process and rx_process.
        // We need to ensure that the rx_process task has shutdown before closing the write half of the tcpstream
        // If we dont do this, rx_process may attempt to read from the tcp stream after the write half has closed.
        // Closing the write half will send a TCP FIN ACK to the server.
        // The server may then respond with a TCP RST, after which any reads from the read half would return a ConnectionReset error
        //
        // If the connection is already dead then we cant cleanly shutdown because rx_process_has_shutdown_rx has already completed.
        // But its fine to skip clean shutdown in this case because once rx_process_has_shutdown_rx has been sent the rx task will never read from the connection again.
        else if connection_dead_error.is_none() {
            // first we drop return_tx which will instruct rx_process to shutdown
            std::mem::drop(return_tx);

            // wait for rx_process to shutdown
            rx_process_has_shutdown_rx.await.ok();

            // Now that rx_process is shutdown we can safely drop the write half of the
            // tcp stream without the read half hitting errors due to the connection being closed or reset.
            std::mem::drop(in_w);
            return;
        }
    }
}

fn send_error_to_requests(
    return_chans: Vec<oneshot::Sender<Response>>,
    stream_ids: Vec<i16>,
    destination: SocketAddr,
    error: &str,
) {
    for (i, return_chan) in return_chans.into_iter().enumerate() {
        return_chan
            .send(Err(ResponseError {
                cause: anyhow!(error.to_owned()),
                destination,
                stream_id: stream_ids[i],
            }))
            .ok();
    }
}

async fn rx_process<T: AsyncRead>(
    read: ReadHalf<T>,
    mut return_rx: mpsc::UnboundedReceiver<ReturnChannel>,
    codec: CassandraDecoder,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    rx_process_has_shutdown_tx: oneshot::Sender<String>,
    // Only used for error reporting
    destination: SocketAddr,
) {
    let mut reader = FramedRead::new(read, codec);

    // Invariants:
    // * client must not reuse a stream_id until the client has received a response with that stream_id (not required by the protocol)
    // * every response from the server must match the stream_id of the request it is responding to (required by the protocol)
    //     + events are not responses and so dont follow this invariant
    //
    // Implementation:
    // To process a message we need to receive things from two different sources:
    // 1. the response from the cassandra server
    // 2. the oneshot::Sender from the tx_process task
    //
    // We can receive these in any order.
    // In order to handle that we have two seperate maps.
    //
    // We store the sender here if we receive from the tx_process task first
    let mut from_tx_process: HashMap<i16, oneshot::Sender<Response>> = HashMap::new();

    // We store the response message here if we receive from the server first.
    let mut from_server: HashMap<i16, Message> = HashMap::new();

    loop {
        tokio::select! {
            response = reader.next() => {
                match response {
                    Some(Ok(response)) => {
                        for m in response {
                            let meta = m.metadata();
                            if let Ok(Metadata::Cassandra(CassandraMetadata { opcode: Opcode::Event, .. })) = meta {
                                if let Some(pushed_messages_tx) = pushed_messages_tx.as_ref() {
                                    pushed_messages_tx.send(vec![m]).ok();
                                }
                            } else if let Some(stream_id) = m.stream_id() {
                                match from_tx_process.remove(&stream_id) {
                                    None => {
                                        from_server.insert(stream_id, m);
                                    },
                                    Some(return_tx) => {
                                        return_tx.send(Ok(m)).ok();
                                    }
                                }
                            }
                        }
                    }
                    Some(Err(CodecReadError::Io(err))) => {
                        // Manually handle Io errors so they can use the nicer Display formatting
                        let error_message = format!("IO error: {err}");
                        send_errors_and_shutdown(return_rx, from_tx_process, rx_process_has_shutdown_tx, destination, &error_message).await;
                        return;
                    }
                    Some(Err(err)) => {
                        // Anyhow errors should be formatted with Debug
                        let error_message = format!("{err:?}");
                        send_errors_and_shutdown(return_rx, from_tx_process, rx_process_has_shutdown_tx, destination, &error_message).await;
                        return;
                    }
                    None => {
                        // We know the connection wasnt closed by the tx task dropping its writer because the tx task must outlive the rx task
                        send_errors_and_shutdown(return_rx, from_tx_process, rx_process_has_shutdown_tx, destination, "The destination cassandra node closed the conection").await;
                        return;
                    }
                }
            },
            original_request = return_rx.recv() => {
                if let Some(ReturnChannel { mut return_chans, stream_ids }) = original_request {
                    for stream_id in stream_ids.iter().rev() {
                    match from_server.remove(stream_id) {
                        None => {
                            from_tx_process.insert(*stream_id, return_chans.pop().unwrap());
                        }
                        Some(m) => {
                            return_chans.pop().unwrap().send(Ok(m)).ok();
                        }
                    }
                    }
                } else {
                    // tx task has requested we shutdown cleanly

                    // confirm we are shutting down immediately by dropping this
                    std::mem::drop(rx_process_has_shutdown_tx);
                    return;
                }
            },
        }
    }
}

async fn send_errors_and_shutdown(
    mut return_rx: mpsc::UnboundedReceiver<ReturnChannel>,
    mut waiting: HashMap<i16, oneshot::Sender<Response>>,
    rx_process_has_shutdown_tx: oneshot::Sender<String>,
    destination: SocketAddr,
    message: &str,
) {
    // Ensure we send this before closing return_rx.
    // This means that when the tx task finds return_rx is closed, it can rely on rx_process_has_shutdown_tx being already sent
    rx_process_has_shutdown_tx
        // Dont send the full message here because the tx task is responsible for that.
        .send(message.to_owned())
        .expect("Tx task must outlive rx task");

    return_rx.close();

    for (stream_id, return_tx) in waiting.drain() {
        return_tx
            .send(Err(ResponseError {
                cause: anyhow!(message.to_owned()),
                destination,
                stream_id,
            }))
            .ok();
    }

    // return_rx is already closed so by looping over all remaning values we ensure there are no dropped unused return_chan's
    while let Some(ReturnChannel {
        return_chans,
        stream_ids,
    }) = return_rx.recv().await
    {
        for (i, return_chan) in return_chans.into_iter().enumerate() {
            return_chan
                .send(Err(ResponseError {
                    cause: anyhow!(message.to_owned()),
                    destination,
                    stream_id: stream_ids[i],
                }))
                .ok();
        }
    }
}

pub async fn receive(
    timeout_duration: Option<Duration>,
    failed_requests: &metrics::Counter,
    mut results: FuturesOrdered<oneshot::Receiver<Response>>,
) -> Result<Vec<Result<Message, ResponseError>>> {
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
                    responses.push(response);
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
            responses.push(receive_message(failed_requests, &mut results).await);
        }
    }
    Ok(responses)
}

async fn receive_message(
    failed_requests: &metrics::Counter,
    results: &mut FuturesOrdered<oneshot::Receiver<Response>>,
) -> Result<Message, ResponseError> {
    match results.next().await {
        Some(result) => match result.expect("The tx_process task must always return a value") {
            Ok(message) => {
                if let Ok(Metadata::Cassandra(CassandraMetadata {
                    opcode: Opcode::Error,
                    ..
                })) = message.metadata()
                {
                    failed_requests.increment(1);
                }
                Ok(message)
            }
            err => err,
        },
        None => unreachable!("Ran out of responses"),
    }
}
