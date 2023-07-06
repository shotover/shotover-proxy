use crate::codec::{CodecBuilder, CodecReadError, CodecWriteError};
use crate::message::Messages;
use crate::sources::Transport;
use crate::tls::{AcceptError, TlsAcceptor};
use crate::transforms::chain::{TransformChain, TransformChainBuilder};
use crate::transforms::Wrapper;
use anyhow::{anyhow, Context, Result};
use bytes::BytesMut;
use futures::future::join_all;
use futures::{SinkExt, StreamExt};
use metrics::{register_gauge, Gauge};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::timeout;
use tokio::time::Duration;
use tokio_tungstenite::{
    tungstenite::{
        handshake::server::{Request, Response},
        protocol::Message as WsMessage,
    },
    WebSocketStream,
};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use tracing::Instrument;
use tracing::{debug, error, warn};

pub struct TcpCodecListener<C: CodecBuilder> {
    chain_builder: TransformChainBuilder,
    source_name: String,

    /// TCP listener supplied by the `run` caller.
    listener: Option<TcpListener>,
    listen_addr: String,
    hard_connection_limit: bool,

    codec: C,

    /// Limit the max number of connections.
    ///
    /// A `Semaphore` is used to limit the max number of connections. Before
    /// attempting to accept a new connection, a permit is acquired from the
    /// semaphore. If none are available, the listener waits for one.
    ///
    /// When handlers complete processing a connection, the permit is returned
    /// to the semaphore.
    limit_connections: Arc<Semaphore>,

    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shutdown is initiated, a `true` value is sent via
    /// the watch::Sender. Each active connection receives it, reaches a
    /// safe terminal state, and completes the task.
    trigger_shutdown_rx: watch::Receiver<bool>,

    tls: Option<TlsAcceptor>,

    /// Keep track of how many connections we have received so we can use it as a request id.
    connection_count: u64,

    available_connections_gauge: Gauge,

    /// Timeout in seconds after which to kill an idle connection. No timeout means connections will never be timed out.
    timeout: Option<u64>,

    connection_handles: Vec<JoinHandle<()>>,

    transport: Transport,
}

impl<C: CodecBuilder + 'static> TcpCodecListener<C> {
    #![allow(clippy::too_many_arguments)]
    pub async fn new(
        chain_builder: TransformChainBuilder,
        source_name: String,
        listen_addr: String,
        hard_connection_limit: bool,
        codec: C,
        limit_connections: Arc<Semaphore>,
        trigger_shutdown_rx: watch::Receiver<bool>,
        tls: Option<TlsAcceptor>,
        timeout: Option<u64>,
        transport: Transport,
    ) -> Result<Self> {
        let available_connections_gauge =
            register_gauge!("shotover_available_connections", "source" => source_name.clone());
        available_connections_gauge.set(limit_connections.available_permits() as f64);

        let listener = Some(create_listener(&listen_addr).await?);

        Ok(TcpCodecListener {
            chain_builder,
            source_name,
            listener,
            listen_addr,
            hard_connection_limit,
            codec,
            limit_connections,
            trigger_shutdown_rx,
            tls,
            connection_count: 0,
            available_connections_gauge,
            timeout,
            connection_handles: vec![],
            transport,
        })
    }

    /// Run the server
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` if accepting returns an error. This can happen for a
    /// number of reasons that resolve over time. For example, if the underlying
    /// operating system has reached an internal limit for max number of
    /// sockets, accept will fail.
    ///
    /// The process is not able to detect when a transient error resolves
    /// itself. One strategy for handling this is to implement a back off
    /// strategy, which is what we do here.
    pub async fn run(&mut self) -> Result<()> {
        loop {
            // Wait for a permit to become available
            let permit = if self.hard_connection_limit {
                match self.limit_connections.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_e) => {
                        //close the socket too full!
                        self.listener = None;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            } else {
                self.limit_connections.clone().acquire_owned().await?
            };
            if self.listener.is_none() {
                self.listener = Some(create_listener(&self.listen_addr).await?);
            }

            self.connection_count = self.connection_count.wrapping_add(1);
            let span = tracing::error_span!(
                "connection",
                id = self.connection_count,
                source = self.source_name.as_str(),
            );
            let transport = self.transport;
            async {
                // Accept a new socket. This will attempt to perform error handling.
                // The `accept` method internally attempts to recover errors, so an
                // error here is non-recoverable.
                let stream = self.accept().await?;

                debug!("got socket");
                self.available_connections_gauge
                    .set(self.limit_connections.available_permits() as f64);

                let (pushed_messages_tx, pushed_messages_rx) =
                    tokio::sync::mpsc::unbounded_channel::<Messages>();

                let handler = Handler {
                    chain: self
                        .chain_builder
                        .build_with_pushed_messages(pushed_messages_tx),
                    codec: self.codec.clone(),
                    shutdown: Shutdown::new(self.trigger_shutdown_rx.clone()),
                    tls: self.tls.clone(),
                    timeout: self.timeout,
                    pushed_messages_rx,
                    _permit: permit,
                };

                // Spawn a new task to process the connections.
                self.connection_handles.push(tokio::spawn(
                    async move {
                        // Process the connection. If an error is encountered, log it.
                        if let Err(err) = handler.run(stream, transport).await {
                            error!(
                                "{:?}",
                                err.context("connection was unexpectedly terminated")
                            );
                        }
                    }
                    .in_current_span(),
                ));
                // Only prune the list every so often
                // theres no point in doing it every iteration because most likely none of the handles will have completed
                if self.connection_count % 1000 == 0 {
                    self.connection_handles.retain(|x| !x.is_finished());
                }
                Ok::<(), anyhow::Error>(())
            }
            .instrument(span)
            .await?;
        }
    }

    pub async fn shutdown(&mut self) {
        join_all(&mut self.connection_handles).await;
    }

    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> Result<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.as_mut().unwrap().accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

async fn create_listener(listen_addr: &str) -> Result<TcpListener> {
    TcpListener::bind(listen_addr)
        .await
        .map_err(|e| anyhow!("{} address={}", e, listen_addr))
}

pub struct Handler<C: CodecBuilder> {
    chain: TransformChain,
    codec: C,
    tls: Option<TlsAcceptor>,
    /// Listen for shutdown notifications.
    ///
    /// A wrapper around the `broadcast::Receiver` paired with the sender in
    /// `Listener`. The connection handler processes requests from the
    /// connection until the peer disconnects **or** a shutdown notification is
    /// received from `shutdown`. In the latter case, any in-flight work being
    /// processed for the peer is continued until it reaches a safe state, at
    /// which point the connection is terminated.
    shutdown: Shutdown,
    /// Timeout in seconds after which to kill an idle connection. No timeout means connections will never be timed out.
    timeout: Option<u64>,
    pushed_messages_rx: UnboundedReceiver<Messages>,
    _permit: OwnedSemaphorePermit,
}

fn spawn_websocket_read_write_tasks<C: CodecBuilder + 'static>(
    codec: C,
    ws_stream: WebSocketStream<TcpStream>,
    in_tx: UnboundedSender<Messages>,
    mut out_rx: UnboundedReceiver<Messages>,
    out_tx: UnboundedSender<Messages>,
) {
    let (mut writer, mut reader) = ws_stream.split();
    let (mut decoder, mut encoder) = codec.build();

    // read task
    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = reader.next() => {
                    if let Some(ws_message) = result {
                        match ws_message {
                            Ok(WsMessage::Binary(ws_message_data)) => {
                                // Entire message is reallocated and copied here due to incompatibility between tokio codecs and tungstenite.
                                let message = decoder.decode(&mut BytesMut::from(ws_message_data.as_slice()));
                                match message {
                                    Ok(Some(message)) => {
                                        if in_tx.send(message).is_err() {
                                            // main task has shutdown, this task is no longer needed
                                            return;
                                        }
                                    }
                                    Ok(None) => {
                                        // websocket client has closed the connection
                                        return;
                                    }
                                    Err(CodecReadError::RespondAndThenCloseConnection(messages)) => {
                                        if let Err(err) = out_tx.send(messages) {
                                            // TODO we need to send a close message to the client
                                            error!("Failed to send RespondAndThenCloseConnection message: {:?}", err);
                                        }
                                        return;
                                    }
                                    Err(CodecReadError::Parser(err)) => {
                                        // TODO we need to send a close message to the client, protocol error
                                        warn!("failed to decode message: {:?}", err);
                                        return;
                                    }
                                    Err(CodecReadError::Io(_err)) => {
                                        unreachable!("CodecReadError::Io should not occur because we are reading from a newly created BytesMut")
                                    }
                                }
                            }
                            Ok(_ws_message) => {
                                // TODO we need to tell the client about a protocol error
                                todo!();
                            }
                            Err(err) => {
                                // TODO
                                error!("{err}");
                                return;
                            }
                        }
                    } else {
                        return;
                    }
                }
                _ = in_tx.closed() => {
                    // main task has shutdown, this task is no longer needed
                    return;
                }
            }
        }
    }
    .in_current_span(),
    );

    // write task
    tokio::spawn(
        async move {
            loop {
                if let Some(message) = out_rx.recv().await {
                    let mut bytes = BytesMut::new();
                    match encoder.encode(message, &mut bytes) {
                        Err(err) => {
                            error!("failed to encode message destined for client: {err:?}");
                            return;
                        }
                        Ok(_) => {
                            let message = WsMessage::binary(bytes);
                            match writer.send(message).await {
                                Ok(_) => {}
                                Err(err) => {
                                    // TODO
                                    error!("{err}");
                                    return;
                                }
                            }
                        }
                    }
                } else {
                    // Main task has ended.
                    // First flush out any remaining messages.
                    // Then end the task thus closing the connection by dropping the write half
                    while let Ok(message) = out_rx.try_recv() {
                        let mut bytes = BytesMut::new();
                        match encoder.encode(message, &mut bytes) {
                            Err(err) => {
                                error!("failed to encode message destined for client: {err:?}")
                            }
                            Ok(_) => {
                                let message = WsMessage::binary(bytes);
                                match writer.send(message).await {
                                    Ok(_) => {}
                                    Err(err) => {
                                        panic!("{err}"); // TODO
                                    }
                                }
                            }
                        }
                    }
                    break;
                }
            }
            match writer.send(WsMessage::Close(None)).await {
                Ok(_) => {}
                Err(err) => {
                    panic!("{err}"); // TODO
                }
            }
        }
        .in_current_span(),
    );
}

fn spawn_read_write_tasks<
    C: CodecBuilder + 'static,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
>(
    codec: C,
    rx: R,
    tx: W,
    in_tx: UnboundedSender<Messages>,
    mut out_rx: UnboundedReceiver<Messages>,
    out_tx: UnboundedSender<Messages>,
) {
    let (decoder, encoder) = codec.build();
    let mut reader = FramedRead::new(rx, decoder);
    let mut writer = FramedWrite::new(tx, encoder);

    // Shutdown flows
    //
    // main task shuts down due to transform error or shotover shutting down:
    // 1. The main task terminates, dropping in_rx and the first out_tx
    // 2. The reader task detects that in_rx has dropped and terminates, the last out_tx instance is dropped
    // 3. The writer task detects that the last out_tx is dropped by out_rx returning None and terminates
    //
    // client closes connection:
    // 1. The reader task detects that the client has closed the connection via reader returning None and terminates,
    // dropping in_tx and the first out_tx
    // 2. The main task detects that in_tx is dropped by in_rx returning None and terminates, dropping the last out_tx
    // 3. The writer task detects that the last out_tx is dropped by out_rx returning None and terminates
    // The writer task could also close early by detecting that the client has closed the connection via writer returning BrokenPipe

    // reader task
    tokio::spawn(
        async move {
            loop {
                tokio::select! {
                    result = reader.next() => {
                        if let Some(message) = result {
                            match message {
                                Ok(messages) => {
                                    if in_tx.send(messages).is_err() {
                                        // main task has shutdown, this task is no longer needed
                                        return;
                                    }
                                }
                                Err(CodecReadError::RespondAndThenCloseConnection(messages)) => {
                                    if let Err(err) = out_tx.send(messages) {
                                        error!("Failed to send RespondAndThenCloseConnection message: {:?}", err);
                                    }
                                    return;
                                }
                                Err(CodecReadError::Parser(err)) => {
                                    warn!("failed to decode message: {:?}", err);
                                    return;
                                }
                                Err(CodecReadError::Io(err)) => {
                                    // I suspect (but have not confirmed) that UnexpectedEof occurs here when the ssl client
                                    // does not send "close notify" before terminating the connection.
                                    // We shouldnt report that as a warning because its common for clients to do
                                    // that for performance reasons.
                                    if !matches!(err.kind(), ErrorKind::UnexpectedEof) {
                                        warn!("failed to receive message on tcp stream: {:?}", err);
                                    }
                                    return;
                                }
                            }
                        } else {
                            debug!("client has closed the connection");
                            return;
                        }
                    }
                    _ = in_tx.closed() => {
                        // main task has shutdown, this task is no longer needed
                        return;
                    }
                }
            }
        }
        .in_current_span(),
    );

    // sender task
    tokio::spawn(
        async move {
            loop {
                if let Some(message) = out_rx.recv().await {
                    match writer.send(message).await {
                        Err(CodecWriteError::Encoder(err)) => {
                            error!("failed to encode message destined for client: {err:?}")
                        }
                        Err(CodecWriteError::Io(err)) => {
                            if matches!(err.kind(), ErrorKind::BrokenPipe | ErrorKind::ConnectionReset) {
                                debug!("client disconnected before it could receive a response");
                                return;
                            } else {
                                error!("failed to send message to client: {err:?}");
                            }
                        }
                        Ok(_) => {}
                    }
                } else {
                    // Main task has ended.
                    // First flush out any remaining messages.
                    // Then end the task thus closing the connection by dropping the write half
                    while let Ok(message) = out_rx.try_recv() {
                        match writer.send(message).await {
                            Err(CodecWriteError::Encoder(err)) => {
                                error!("while flushing messages: failed to encode message destined for client: {err:?}")
                            }
                            Err(CodecWriteError::Io(err)) => {
                                if matches!(err.kind(), ErrorKind::BrokenPipe | ErrorKind::ConnectionReset) {
                                    debug!("while flushing messages: client disconnected before it could receive a response");
                                    return;
                                } else {
                                    error!(
                                        "while flushing messages: failed to send message to client: {err:?}"
                                    );
                                }
                            }
                            Ok(_) => {}
                        }
                    }
                    break;
                }
            }
        }
        .in_current_span(),
    );
}

impl<C: CodecBuilder + 'static> Handler<C> {
    /// Process a single connection.
    ///
    /// Request frames are read from the socket and processed. Responses are
    /// written back to the socket.
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    pub async fn run(mut self, stream: TcpStream, transport: Transport) -> Result<()> {
        stream.set_nodelay(true)?;

        let client_details = stream
            .peer_addr()
            .map(|p| p.ip().to_string())
            .unwrap_or_else(|_| "Unknown peer".to_string());
        tracing::debug!("New connection from {}", client_details);

        let (in_tx, in_rx) = mpsc::unbounded_channel::<Messages>();
        let (out_tx, out_rx) = mpsc::unbounded_channel::<Messages>();

        let local_addr = stream.local_addr()?;

        let codec_builder = self.codec.clone();

        match transport {
            Transport::WebSocket => {
                let websocket_subprotocol = codec_builder.websocket_subprotocol();

                let callback = |_request: &Request, mut response: Response| {
                    let response_headers = response.headers_mut();

                    response_headers.append(
                        "Sec-WebSocket-Protocol",
                        websocket_subprotocol.parse().unwrap(),
                    );

                    Ok(response)
                };

                let ws_stream = tokio_tungstenite::accept_hdr_async(stream, callback)
                    .await
                    .expect("Error during the websocket handshake occurred");

                spawn_websocket_read_write_tasks(
                    codec_builder,
                    ws_stream,
                    in_tx,
                    out_rx,
                    out_tx.clone(),
                );
            }
            Transport::Tcp => {
                if let Some(tls) = &self.tls {
                    let tls_stream = match tls.accept(stream).await {
                        Ok(x) => x,
                        Err(AcceptError::Disconnected) => return Ok(()),
                        Err(AcceptError::Failure(err)) => return Err(err),
                    };
                    let (rx, tx) = tokio::io::split(tls_stream);
                    spawn_read_write_tasks(
                        self.codec.clone(),
                        rx,
                        tx,
                        in_tx,
                        out_rx,
                        out_tx.clone(),
                    );
                } else {
                    let (rx, tx) = stream.into_split();
                    spawn_read_write_tasks(
                        self.codec.clone(),
                        rx,
                        tx,
                        in_tx,
                        out_rx,
                        out_tx.clone(),
                    );
                };
            }
        };

        let result = self
            .process_messages(&client_details, local_addr, in_rx, out_tx)
            .await;

        // Flush messages regardless of if we are shutting down due to a failure or due to application shutdown
        match self
            .chain
            .process_request(
                Wrapper::flush_with_chain_name(self.chain.name.clone()),
                client_details,
            )
            .await
        {
            Ok(_) => {}
            Err(e) => error!(
                "{:?}",
                e.context(format!(
                    "encountered an error when flushing the chain {} for shutdown",
                    self.chain.name,
                ))
            ),
        }

        result
    }

    async fn process_messages(
        &mut self,
        client_details: &str,
        local_addr: SocketAddr,
        mut in_rx: mpsc::UnboundedReceiver<Messages>,
        out_tx: mpsc::UnboundedSender<Messages>,
    ) -> Result<()> {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        let mut idle_time_seconds: u64 = 1;

        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown signal
            debug!("Waiting for message {client_details}");
            let mut reverse_chain = false;

            let messages = tokio::select! {
                res = timeout(Duration::from_secs(idle_time_seconds), in_rx.recv()) => {
                    match res {
                        Ok(maybe_message) => {
                            idle_time_seconds = 1;
                            match maybe_message {
                                Some(m) => m,
                                None => return Ok(())
                            }
                        },
                        Err(_) => {
                            if let Some(timeout) =  self.timeout {
                                if idle_time_seconds < timeout {
                                    debug!("Connection Idle for more than {} seconds {}", timeout, client_details);
                                } else {
                                    debug!("Dropping. Connection Idle for more than {} seconds {}", timeout, client_details);
                                    return Ok(());
                                }
                            }
                            idle_time_seconds *= 2;
                            continue
                        }
                    }
                },
                Some(res) = self.pushed_messages_rx.recv() => {
                    reverse_chain = true;
                    res
                },
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            debug!("Received raw messages {:?}", messages);

            let mut error_report_messages = if reverse_chain {
                // Avoid allocating for reverse chains as we dont make use of this value in that case
                vec![]
            } else {
                // This clone should be cheap as cloning a Message that has never had `.frame()`
                // called should result in no new allocations.
                messages.clone()
            };

            let wrapper = Wrapper::new_with_client_details(
                messages,
                client_details.to_owned(),
                self.chain.name.clone(),
                local_addr,
            );

            let modified_messages = if reverse_chain {
                self.chain
                    .process_request_rev(wrapper, client_details.to_owned())
                    .await
                    .context("Chain failed to receive pushed messages/events, the connection will now be closed.")?
            } else {
                match self
                    .chain
                    .process_request(wrapper, client_details.to_owned())
                    .await
                    .context("Chain failed to send and/or receive messages, the connection will now be closed.")
                {
                    Ok(x) => x,
                    Err(err) => {
                        // An internal error occured and we need to terminate the connection because we can no
                        // longer make any gaurantees about the state its in.
                        // However before we do that we need to return errors for all the messages in this batch for two reasons:
                        // * Poorly programmed clients may hang forever waiting for a response
                        // * We want to give the user a hint as to what went wrong
                        //     + they might not know to check the shotover logs
                        //     + they may not be able to correlate which error in the shotover logs corresponds to their failed message
                        for m in &mut error_report_messages {
                            #[allow(clippy::single_match)]
                            match m.to_error_response(format!("Internal shotover (or custom transform) bug: {err:?}")) {
                                Ok(new_m) => *m = new_m,
                                Err(_) => {
                                    // If we cant produce an error then nothing we can do, just continue on and close the connection.
                                }
                            }
                        }
                        out_tx.send(error_report_messages)?;
                        return Err(err);
                    }
                }
            };

            debug!("sending message: {:?}", modified_messages);
            // send the result of the process up stream
            if out_tx.send(modified_messages).is_err() {
                // the client has disconnected so we should terminate this connection
                return Ok(());
            }
        }

        Ok(())
    }
}

/// Listens for the server shutdown signal.
///
/// Shutdown is signaled using a `broadcast::Receiver`. Only a single value is
/// ever sent. Once a value has been sent via the broadcast channel, the server
/// should shutdown.
///
/// The `Shutdown` struct listens for the signal and tracks that the signal has
/// been received. Callers may query for whether the shutdown signal has been
/// received or not.
#[derive(Debug)]
pub struct Shutdown {
    /// `true` if the shutdown signal has been received
    shutdown: bool,

    /// The receive half of the channel used to listen for shutdown.
    notify: watch::Receiver<bool>,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub(crate) fn new(notify: watch::Receiver<bool>) -> Shutdown {
        Shutdown {
            shutdown: false,
            notify,
        }
    }

    /// Returns `true` if the shutdown signal has been received.
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    /// Receive the shutdown notice, waiting if necessary.
    pub(crate) async fn recv(&mut self) {
        // If the shutdown signal has already been received, then return
        // immediately.
        if self.shutdown {
            return;
        }

        // check we didn't receive a shutdown message before the receiver was created
        if !*self.notify.borrow() {
            // Await the shutdown message
            self.notify.changed().await.unwrap();
        }

        // Remember that the signal has been received.
        self.shutdown = true;
    }
}
