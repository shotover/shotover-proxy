use crate::codec::{CodecBuilder, CodecReadError, CodecWriteError};
use crate::config::chain::TransformChainConfig;
use crate::frame::MessageType;
use crate::hot_reload::protocol::{
    GradualShutdownRequest, HotReloadListenerRequest, HotReloadListenerResponse,
};
use crate::message::{Message, MessageIdMap, Messages, Metadata};
use crate::sources::Transport;
use crate::tls::{AcceptError, TlsAcceptor};
use crate::transforms::chain::{TransformChain, TransformChainBuilder};
use crate::transforms::{ChainState, TransformContextBuilder, TransformContextConfig};
use anyhow::{Result, anyhow};
use bytes::BytesMut;
use futures::future::join_all;
use futures::{SinkExt, StreamExt};
use metrics::{Counter, Gauge, counter, gauge};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::{
    handshake::server::{Request, Response},
    protocol::Message as WsMessage,
};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use tracing::{Instrument, info, trace};
use tracing::{debug, error, warn};

/// Represents a tracked connection with an abort handle
struct TrackedConnection {
    handle: JoinHandle<()>,
    abort_handle: tokio::task::AbortHandle,
}

impl TrackedConnection {
    fn new(handle: JoinHandle<()>) -> Self {
        let abort_handle = handle.abort_handle();
        Self {
            handle,
            abort_handle,
        }
    }

    fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

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

    connections_opened: Counter,
    available_connections_gauge: Gauge,

    /// Timeout after which to kill an idle connection. No timeout means connections will never be timed out.
    timeout: Option<Duration>,

    connection_handles: Arc<tokio::sync::Mutex<Vec<TrackedConnection>>>,

    transport: Transport,

    /// Receiver for hot reload requests to extract listening socket file descriptor
    hot_reload_rx: tokio::sync::mpsc::UnboundedReceiver<HotReloadListenerRequest>,

    /// Receiver for gradual shutdown requests
    gradual_shutdown_rx: tokio::sync::mpsc::UnboundedReceiver<GradualShutdownRequest>,

    port: u16,

    /// Flag indicating whether the socket has been handed off during hot reload
    /// When true, prevents attempting to recreate the listener
    socket_handed_off: bool,

    /// Receiver for notification when gradual shutdown draining is complete
    draining_complete_rx: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl<C: CodecBuilder + 'static> TcpCodecListener<C> {
    #![allow(clippy::too_many_arguments)]
    pub async fn new(
        chain_config: &TransformChainConfig,
        source_name: String,
        listen_addr: String,
        hard_connection_limit: bool,
        codec: C,
        limit_connections: Arc<Semaphore>,
        trigger_shutdown_rx: watch::Receiver<bool>,
        tls: Option<TlsAcceptor>,
        timeout: Option<Duration>,
        transport: Transport,
        hot_reload_rx: tokio::sync::mpsc::UnboundedReceiver<HotReloadListenerRequest>,
        gradual_shutdown_rx: tokio::sync::mpsc::UnboundedReceiver<GradualShutdownRequest>,
        hot_reload_listeners: &mut HashMap<u16, TcpListener>,
    ) -> Result<Self, Vec<String>> {
        let available_connections_gauge =
            gauge!("shotover_available_connections_count", "source" => source_name.clone());
        let connections_opened = counter!("connections_opened", "source" => source_name.clone());
        available_connections_gauge.set(limit_connections.available_permits() as f64);

        let chain_usage_config = TransformContextConfig {
            chain_name: source_name.clone(),
            up_chain_protocol: codec.protocol(),
        };
        let chain_builder = chain_config
            .get_builder(chain_usage_config)
            .await
            .map_err(|x| vec![format!("{x:?}")])?;

        let mut errors = chain_builder
            .validate()
            .iter()
            .map(|x| format!("  {x}"))
            .collect::<Vec<String>>();

        let Some(port) = listen_addr
            .rsplit_once(':')
            .and_then(|(_, p)| p.parse::<u16>().ok())
        else {
            return Err(vec![format!(
                "Invalid listening address {listen_addr:?}, must follow the format ip_address:port e.g. 10.0.0.1:9042"
            )]);
        };

        let listener = if let Some(listener) = hot_reload_listeners.remove(&port) {
            info!(
                "Using hot reloaded listener for {} source on [{}]",
                source_name, listen_addr
            );
            Some(listener)
        } else {
            match create_listener(&listen_addr).await {
                Ok(listener) => {
                    info!("Created new listener for {}", listen_addr);
                    Some(listener)
                }
                Err(error) => {
                    errors.push(format!("{error:?}"));
                    None
                }
            }
        };

        if !errors.is_empty() {
            errors.insert(0, format!("{source_name} source:"));
            return Err(errors);
        }

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
            connections_opened,
            timeout,
            connection_handles: Arc::new(tokio::sync::Mutex::new(vec![])),
            transport,
            hot_reload_rx,
            gradual_shutdown_rx,
            port,
            socket_handed_off: false,
            draining_complete_rx: None,
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
            if self.listener.is_none() && !self.socket_handed_off {
                self.listener = Some(create_listener(&self.listen_addr).await?);
            }

            self.connection_count = self.connection_count.wrapping_add(1);
            let span =
                crate::connection_span::span(self.connection_count, self.source_name.as_str());
            let transport = self.transport;

            async {
                tokio::select! {
                    // Wait for a permit to become available and accept new connection
                    stream_result = async {
                        // Wait for a permit to become available
                        let permit = if self.hard_connection_limit {
                            match self.limit_connections.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_e) => {
                                    //close the socket too full!
                                    self.listener = None;
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                    return Err(anyhow!("Connection limit reached, retrying"));
                                }
                            }
                        } else {
                            self.limit_connections.clone().acquire_owned().await?
                        };

                        // Only accept new connections if socket hasn't been handed off
                        if self.socket_handed_off {
                            // Socket handed off, do not accept new connections
                            futures::future::pending::<Result<(TcpStream, OwnedSemaphorePermit)>>().await
                        } else {
                            let stream = Self::accept(&mut self.listener).await?;
                            Ok::<(TcpStream, OwnedSemaphorePermit), anyhow::Error>((stream, permit))
                        }
                    } => {
                        let (stream, permit) = match stream_result {
                            Ok(result) => result,
                            Err(e) => {
                                // If this was a connection limit error, continue to next iteration
                                if e.to_string().contains("Connection limit reached") {
                                    return Ok(());
                                }
                                return Err(e);
                            }
                        };

                        debug!("got socket");
                        self.available_connections_gauge.set(self.limit_connections.available_permits() as f64);
                        self.connections_opened.increment(1);

                        let client_details = stream.peer_addr()
                            .map(|p| p.ip().to_string())
                            .unwrap_or_else(|_| "Unknown Peer".to_string());
                        tracing::debug!("New connection from {}", client_details);

                        let force_run_chain = Arc::new(Notify::new());
                        let context = TransformContextBuilder{
                            force_run_chain: force_run_chain.clone(),
                            client_details:client_details.clone(),
                        };

                        let handler = Handler{
                            chain: self.chain_builder.build(context),
                            codec: self.codec.clone(),
                            shutdown: Shutdown::new(self.trigger_shutdown_rx.clone()),
                            tls: self.tls.clone(),
                            pending_requests: PendingRequests::new(self.codec.protocol()),
                            timeout: self.timeout,
                            _permit: permit,
                        };
                        // Spawn a new task to process the connections.
                        let handle = tokio::spawn(async move{
                            // Process the connection. If an error is encountered, log it.
                            if let Err(err) = handler.run(stream, transport, force_run_chain, client_details).await{
                                error!("{:?}", err.context("connection was unexpectedly terminated"));
                            }
                        }.in_current_span());
                        let tracked_connection = TrackedConnection::new(handle);
                        self.connection_handles.lock().await.push(tracked_connection);
                        // Only prune the list every so often
                        // theres no point in doing it every iteration because most likely none of the handles will have completed
                        if self.connection_count % 1000 == 0{
                            self.connection_handles.lock().await.retain(|x| !x.is_finished());
                        }
                        Ok::<(), anyhow::Error>(())
                    },
                    // Hot reload request handling
                    hot_reload_request = self.hot_reload_rx.recv() => {
                        if let Some(request) = hot_reload_request{
                            self.handle_hot_reload_request(request).await;
                            // After handing off the socket FD, mark it as handed off and clear the listener
                            // This prevents attempting to recreate the listener on the same address
                            self.socket_handed_off = true;
                            self.listener = None;
                        }
                        Ok::<(), anyhow::Error>(())
                    },
                    // Gradual shutdown request handling
                    gradual_shutdown_request = self.gradual_shutdown_rx.recv() => {
                        if let Some(request) = gradual_shutdown_request{
                            self.handle_gradual_shutdown_request(request).await;
                        }
                        Ok::<(), anyhow::Error>(())
                    },
                    // Wait for gradual shutdown draining to complete
                    _ = async {
                        if let Some(rx) = &mut self.draining_complete_rx {
                            rx.await.ok();
                        } else {
                            futures::future::pending::<()>().await
                        }
                    } => {
                        info!("Gradual shutdown draining completed, exiting main loop");
                        // Clear the receiver now that we've successfully received the signal
                        self.draining_complete_rx = None;
                        #[allow(clippy::needless_return)]
                        return Ok(());
                    }
                }
            }
            .instrument(span)
            .await?;
        }
    }

    pub async fn shutdown(&mut self) {
        let mut handles = self.connection_handles.lock().await;
        let futures: Vec<_> = handles.iter_mut().map(|tc| &mut tc.handle).collect();
        join_all(futures).await;
    }

    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(listener: &mut Option<TcpListener>) -> Result<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match listener.as_mut().unwrap().accept().await {
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

    /// Handle hot reload request by extracting file descriptor and responding
    async fn handle_hot_reload_request(&mut self, request: HotReloadListenerRequest) {
        let response = if let Some(listener) = self.listener.take() {
            let port = self.port;

            // Convert tokio::TcpListener to std::net::TcpListener to OwnedFd
            match listener.into_std() {
                Ok(std_listener) => {
                    let owned_fd: std::os::unix::io::OwnedFd = std_listener.into();
                    tracing::info!("Hot reload: Extracted socket OwnedFd for port {}", port);

                    HotReloadListenerResponse::HotReloadResponse {
                        port,
                        listener_socket_fd: owned_fd,
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to convert listener to std: {:?}", e);
                    HotReloadListenerResponse::NoListenerAvailable
                }
            }
        } else {
            tracing::warn!("Hot reload request received but no listener available");
            HotReloadListenerResponse::NoListenerAvailable
        };

        // Send response back through oneshot channel
        if request.return_chan.send(response).is_err() {
            tracing::error!("Failed to send hot reload response - receiver dropped");
        }
    }

    /// Handle gradual shutdown request by spawning a background task to drain connections
    async fn handle_gradual_shutdown_request(&mut self, request: GradualShutdownRequest) {
        info!("Gradual shutdown request received for {}", self.source_name);

        let connection_handles = self.connection_handles.clone();
        let source_name = self.source_name.clone();

        // Create a oneshot channel to signal when draining is complete
        let (draining_complete_tx, draining_complete_rx) = tokio::sync::oneshot::channel();
        self.draining_complete_rx = Some(draining_complete_rx);

        // Spawn background task to drain connections gradually
        tokio::spawn(async move {
            info!("[{}] Starting gradual connection draining", source_name);

            loop {
                // Wait 10 seconds before each drain cycle
                tokio::time::sleep(Duration::from_secs(10)).await;

                let mut handles = connection_handles.lock().await;

                // Remove finished connections
                handles.retain(|tc| !tc.is_finished());

                let total_connections = handles.len();

                if total_connections == 0 {
                    info!("[{}] All connections have been drained", source_name);
                    break;
                }

                // Calculate 10% of connections to drain (at least 1 if there are any connections)
                let connections_to_drain =
                    std::cmp::max(1, (total_connections as f64 * 0.1).ceil() as usize);

                info!(
                    "[{}] Draining {} out of {} connections (10%)",
                    source_name, connections_to_drain, total_connections
                );

                // Abort the first N connections
                for tc in handles.iter().take(connections_to_drain) {
                    tc.abort_handle.abort();
                }

                // Remove the aborted connections from the list
                handles.drain(..connections_to_drain);

                info!(
                    "[{}] {} connections remaining after drain",
                    source_name,
                    handles.len()
                );
            }

            info!("[{}] Gradual shutdown completed", source_name);

            // Signal that draining is complete
            let _ = draining_complete_tx.send(());
        });

        // Acknowledge the gradual shutdown request
        if request.return_chan.send(()).is_err() {
            tracing::error!("Failed to send gradual shutdown acknowledgment - receiver dropped");
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
    pending_requests: PendingRequests,
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
    timeout: Option<Duration>,
    _permit: OwnedSemaphorePermit,
}

async fn spawn_websocket_read_write_tasks<
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: CodecBuilder + 'static,
>(
    codec: C,
    stream: S,
    in_tx: mpsc::Sender<Messages>,
    mut out_rx: UnboundedReceiver<Messages>,
    out_tx: UnboundedSender<Messages>,
    websocket_subprotocol: &str,
) {
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
                                // Entire message is reallocated and copied here due to 
                                // incompatibility between tokio codecs and tungstenite.
                                let message = decoder.decode(&mut BytesMut::from(ws_message_data.as_ref()));
                                match message {
                                    Ok(Some(message)) => {
                                        if in_tx.send(message).await.is_err() {
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
                                            error!("Failed to send RespondAndThenCloseConnection message: {err}");
                                        }
                                        return;
                                    }
                                    Err(CodecReadError::Parser(err)) => {
                                        // TODO we need to send a close message to the client, protocol error
                                        warn!("failed to decode message: {err:?}");
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
                    match writer.send(WsMessage::Close(None)).await {
                        Ok(_) => {}
                        Err(err) => {
                            panic!("{err}"); // TODO
                        }
                    }
                    return;
                }
            }
        }
        .in_current_span(),
    );
}

pub fn spawn_read_write_tasks<
    C: CodecBuilder + 'static,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
>(
    codec: C,
    rx: R,
    tx: W,
    in_tx: mpsc::Sender<Messages>,
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
                                    if in_tx.send(messages).await.is_err() {
                                        // main task has shutdown, this task is no longer needed
                                        return;
                                    }
                                }
                                Err(CodecReadError::RespondAndThenCloseConnection(messages)) => {
                                    if let Err(err) = out_tx.send(messages) {
                                        error!("Failed to send RespondAndThenCloseConnection message: {err}");
                                    }
                                    return;
                                }
                                Err(CodecReadError::Parser(err)) => {
                                    warn!("failed to decode message: {err:?}");
                                    return;
                                }
                                Err(CodecReadError::Io(err)) => {
                                    // I suspect (but have not confirmed) that UnexpectedEof occurs here when the ssl client
                                    // does not send "close notify" before terminating the connection.
                                    // We shouldnt report that as a warning because its common for clients to do
                                    // that for performance reasons.
                                    if !matches!(err.kind(), ErrorKind::UnexpectedEof) {
                                        warn!("failed to receive message on tcp stream: {err:?}");
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
                            if matches!(
                                err.kind(),
                                ErrorKind::BrokenPipe | ErrorKind::ConnectionReset
                            ) {
                                debug!("client disconnected before it could receive a response");
                                return;
                            } else {
                                error!("failed to send message to client: {err:?}");
                            }
                        }
                        Ok(_) => {}
                    }
                } else {
                    return;
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
    pub async fn run(
        mut self,
        stream: TcpStream,
        transport: Transport,
        force_run_chain: Arc<Notify>,
        client_details: String,
    ) -> Result<()> {
        stream.set_nodelay(true)?;

        // limit buffered incoming messages to 10,000 per connection.
        // A particular scenario we are concerned about is if it takes longer to send to the server
        // than for the client to send to us, the buffer will grow indefinitely, increasing latency until the buffer triggers an OoM.
        // To avoid that we have currently hardcoded a limit of 10,000 but if we start hitting that in production we should make this user configurable.
        let (in_tx, in_rx) = mpsc::channel::<Messages>(10_000);
        let (out_tx, out_rx) = mpsc::unbounded_channel::<Messages>();

        let local_addr = stream.local_addr()?;

        let codec_builder = self.codec.clone();

        match transport {
            Transport::WebSocket => {
                let websocket_subprotocol = codec_builder.protocol().websocket_subprotocol();

                if let Some(tls) = &self.tls {
                    let tls_stream = match tls.accept(stream).await {
                        Ok(x) => x,
                        Err(AcceptError::Disconnected) => return Ok(()),
                        Err(AcceptError::Failure(err)) => return Err(err),
                    };
                    spawn_websocket_read_write_tasks(
                        codec_builder,
                        tls_stream,
                        in_tx,
                        out_rx,
                        out_tx.clone(),
                        websocket_subprotocol,
                    )
                    .await;
                } else {
                    spawn_websocket_read_write_tasks(
                        codec_builder,
                        stream,
                        in_tx,
                        out_rx,
                        out_tx.clone(),
                        websocket_subprotocol,
                    )
                    .await;
                };
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
            .run_loop(&client_details, local_addr, in_rx, out_tx, force_run_chain)
            .await;

        // Only flush messages if we are shutting down due to shotover shutdown or client disconnect
        // If a Transform::transform returns an Err the transform is no longer in a usable state and needs to be destroyed without reusing.
        if let Ok(CloseReason::ShotoverShutdown | CloseReason::ClientClosed) = result {
            match self.chain.process_request(&mut ChainState::flush()).await {
                Ok(_) => {}
                Err(e) => error!(
                    "{:?}",
                    e.context(format!(
                        "encountered an error when flushing the chain {} for shutdown",
                        self.chain.name,
                    ))
                ),
            }
        }

        result.map(|_| ())
    }

    async fn receive_with_timeout(
        timeout: Option<Duration>,
        in_rx: &mut mpsc::Receiver<Vec<Message>>,
        client_details: &str,
    ) -> Option<Vec<Message>> {
        if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, in_rx.recv()).await {
                Ok(messages) => messages,
                Err(_) => {
                    debug!(
                        "Dropping connection to {client_details} due to being idle for more than {timeout:?}"
                    );
                    None
                }
            }
        } else {
            in_rx.recv().await
        }
    }

    async fn run_loop(
        &mut self,
        client_details: &str,
        local_addr: SocketAddr,
        mut in_rx: mpsc::Receiver<Messages>,
        out_tx: mpsc::UnboundedSender<Messages>,
        force_run_chain: Arc<Notify>,
    ) -> Result<CloseReason> {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown signal
            debug!("Waiting for message {client_details}");
            tokio::select! {
                biased;
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(CloseReason::ShotoverShutdown);
                }
                () = force_run_chain.notified() => {
                    let mut requests = vec!();
                    while let Ok(x) = in_rx.try_recv() {
                        requests.extend(x);
                    }
                    debug!("running transform chain because a transform in the chain requested that a chain run occur");
                    if let Some(close_reason) = self.send_receive_chain(local_addr, &out_tx, requests).await? {
                        return Ok(close_reason)
                    }
                },
                requests = Self::receive_with_timeout(self.timeout, &mut in_rx, client_details) => {
                    match requests {
                        Some(mut requests) => {
                            while let Ok(x) = in_rx.try_recv() {
                                requests.extend(x);
                            }
                            debug!("running transform chain because requests received from client");
                            if let Some(close_reason) = self.send_receive_chain(local_addr, &out_tx, requests).await? {
                                return Ok(close_reason)
                            }
                        }
                        // Either we timed out the connection or the client disconnected, so terminate this connection
                        None => return Ok(CloseReason::ClientClosed),
                    }
                },
            };
        }

        Ok(CloseReason::ShotoverShutdown)
    }

    async fn send_receive_chain(
        &mut self,
        local_addr: SocketAddr,
        out_tx: &mpsc::UnboundedSender<Messages>,
        requests: Messages,
    ) -> Result<Option<CloseReason>> {
        trace!("running transform chain with requests: {requests:?}");
        let mut wrapper = ChainState::new_with_addr(requests, local_addr);

        self.pending_requests.process_requests(&wrapper.requests);
        let responses = match self.chain.process_request(&mut wrapper).await {
            Ok(x) => x,
            Err(err) => {
                let err = err.context(
                    "Chain failed to send and/or receive messages, the connection will now be closed.",
                );
                // The connection is going to be closed once we return Err.
                // So first make a best effort attempt of responding to any pending requests with an error response.
                out_tx.send(self.pending_requests.to_errors(&err))?;
                return Err(err);
            }
        };
        self.pending_requests.process_responses(&responses);

        // send the result of the process up stream
        if !responses.is_empty() {
            debug!("sending {} responses to client", responses.len());
            trace!("sending response to client: {responses:?}");
            if out_tx.send(responses).is_err() {
                // the client has disconnected so we should terminate this connection
                return Ok(Some(CloseReason::ClientClosed));
            }
        }

        // if requested by a transform, close connection AFTER sending any responses back to the client
        if wrapper.close_client_connection {
            return Ok(Some(CloseReason::TransformRequested));
        }

        Ok(None)
    }
}

/// Indicates that the connection to the client must be closed.
enum CloseReason {
    TransformRequested,
    ClientClosed,
    ShotoverShutdown,
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

/// Keeps track of all currently pending requests.
/// This allows error responses to be generated if the connection needs to be terminated before the response comes back.
enum PendingRequests {
    /// The protocol is in order.
    Ordered(Vec<Result<Metadata>>),
    /// The protocol is out of order.
    Unordered(MessageIdMap<Result<Metadata>>),
    /// If a protocol does not support error messages then no point keeping track of the requests at all
    Unsupported,
}

impl PendingRequests {
    fn new(message_type: MessageType) -> Self {
        match message_type {
            #[cfg(feature = "valkey")]
            MessageType::Valkey => PendingRequests::Ordered(vec![]),
            #[cfg(feature = "cassandra")]
            MessageType::Cassandra => PendingRequests::Unordered(Default::default()),
            #[cfg(feature = "kafka")]
            MessageType::Kafka => PendingRequests::Unsupported,
            #[cfg(feature = "opensearch")]
            MessageType::OpenSearch => PendingRequests::Unsupported,
            MessageType::Dummy => PendingRequests::Unsupported,
        }
    }

    fn process_requests(&mut self, requests: &[Message]) {
        match self {
            PendingRequests::Ordered(pending_requests) => {
                pending_requests.extend(requests.iter().map(|x| x.metadata()))
            }
            PendingRequests::Unordered(pending_requests) => {
                for request in requests {
                    pending_requests.insert(request.id(), request.metadata());
                }
            }
            PendingRequests::Unsupported => {}
        }
    }

    fn process_responses(&mut self, responses: &[Message]) {
        match self {
            PendingRequests::Ordered(pending_requests) => {
                let responses_received = responses
                    .iter()
                    .filter(|x| x.request_id().is_some())
                    .count();
                if responses_received > pending_requests.len() {
                    tracing::error!(
                        "received more responses than requests, this violates the transform invariants"
                    );
                    std::mem::drop(pending_requests.drain(..))
                } else {
                    std::mem::drop(pending_requests.drain(..responses_received))
                }
            }
            PendingRequests::Unordered(pending_requests) => {
                for response in responses {
                    if let Some(id) = response.request_id() {
                        pending_requests.remove(&id);
                    }
                }
            }
            PendingRequests::Unsupported => {}
        }
    }

    fn to_errors(&self, err: &anyhow::Error) -> Vec<Message> {
        // An internal error occured and we need to terminate the connection because we can no
        // longer make any guarantees about the state its in.
        // However before we do that we need to return error responses for all the pending requests for two reasons:
        // * Poorly programmed clients may hang forever waiting for a response
        // * We want to give the user a hint as to what went wrong
        //     + they might not know to check the shotover logs
        //     + they may not be able to correlate which error in the shotover logs corresponds to their failed message
        match self {
            PendingRequests::Ordered(pending_requests) => {
                pending_requests.iter().filter_map(|pending_request| {
                    let meta = match pending_request {
                        Ok(meta) => meta,
                        Err(err) => {
                            tracing::error!("Failed to parse request into metadata {err:?}");
                            return None;
                        }
                    };

                    match meta.to_error_response(format!(
                        "Internal shotover (or custom transform) bug: {err:?}"
                    )) {
                        Ok(response) => Some(response),
                        Err(err) => {
                            tracing::error!("Failed to create an error from the request even though the protocol supports it {err:?}");
                            None
                        }
                    }
                }).collect()
            }
            PendingRequests::Unordered(pending_requests) => {
                pending_requests.iter().filter_map(|(id, pending_request)| {
                    let meta = match pending_request {
                        Ok(meta) => meta,
                        Err(err) => {
                            tracing::error!("Failed to parse request into metadata {err:?}");
                            return None;
                        }
                    };

                    match meta.to_error_response(format!(
                        "Internal shotover (or custom transform) bug: {err:?}"
                    )) {
                        Ok(mut response) => {
                            response.set_request_id(*id);
                            Some(response)
                        }
                        Err(err) => {
                            tracing::error!("Failed to create an error from the request even though the protocol supports it {err:?}");
                            None
                        }
                    }
                }).collect()
            }
            PendingRequests::Unsupported => {
                // If we cant produce an error then nothing we can do, just continue on and close the connection.
                vec![]
            }
        }
    }
}
