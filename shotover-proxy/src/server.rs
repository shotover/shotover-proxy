use crate::codec::{CodecBuilder, CodecReadError};
use crate::message::Messages;
use crate::tls::{AcceptError, TlsAcceptor};
use crate::transforms::chain::{TransformChain, TransformChainBuilder};
use crate::transforms::Wrapper;
use anyhow::{anyhow, Context, Result};
use futures::future::join_all;
use futures::{SinkExt, StreamExt};
use metrics::{register_gauge, Gauge};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::timeout;
use tokio::time::Duration;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::Instrument;
use tracing::{debug, error, info, warn};

pub struct TcpCodecListener<C: CodecBuilder> {
    chain: TransformChainBuilder,
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
}

impl<C: CodecBuilder + 'static> TcpCodecListener<C> {
    #![allow(clippy::too_many_arguments)]
    pub async fn new(
        chain: TransformChainBuilder,
        source_name: String,
        listen_addr: String,
        hard_connection_limit: bool,
        codec: C,
        limit_connections: Arc<Semaphore>,
        trigger_shutdown_rx: watch::Receiver<bool>,
        tls: Option<TlsAcceptor>,
        timeout: Option<u64>,
    ) -> Result<Self> {
        let available_connections_gauge =
            register_gauge!("shotover_available_connections", "source" => source_name.clone());
        available_connections_gauge.set(limit_connections.available_permits() as f64);

        let listener = Some(create_listener(&listen_addr).await?);

        Ok(TcpCodecListener {
            chain,
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
        info!("accepting inbound connections");

        loop {
            // Wait for a permit to become available
            //
            // `acquire` returns a permit that is bound via a lifetime to the
            // semaphore. When the permit value is dropped, it is automatically
            // returned to the semaphore. This is convenient in many cases.
            // However, in this case, the permit must be returned in a different
            // task than it is acquired in (the handler task). To do this, we
            // "forget" the permit, which drops the permit value **without**
            // incrementing the semaphore's permits. Then, in the handler task
            // we manually add a new permit when processing completes.
            if self.hard_connection_limit {
                match self.limit_connections.try_acquire() {
                    Ok(p) => {
                        if self.listener.is_none() {
                            self.listener = Some(create_listener(&self.listen_addr).await?);
                        }
                        p.forget();
                    }
                    Err(_e) => {
                        if self.listener.is_some() {
                            //close the socket too full!
                            self.listener = None;
                        }
                        tokio::time::sleep(Duration::new(1, 0)).await;
                        continue;
                    }
                }
            } else {
                self.limit_connections.acquire().await?.forget();
                if self.listener.is_none() {
                    self.listener = Some(create_listener(&self.listen_addr).await?);
                }
            }

            self.connection_count = self.connection_count.wrapping_add(1);
            let span = tracing::error_span!(
                "connection",
                id = self.connection_count,
                source = self.source_name.as_str(),
            );
            async {
                // Accept a new socket. This will attempt to perform error handling.
                // The `accept` method internally attempts to recover errors, so an
                // error here is non-recoverable.
                let socket = self.accept().await?;

                debug!("got socket");
                self.available_connections_gauge
                    .set(self.limit_connections.available_permits() as f64);

                let peer = socket
                    .peer_addr()
                    .map(|p| format!("{}", p.ip()))
                    .unwrap_or_else(|_| "Unknown peer".to_string());

                let conn_string = socket
                    .peer_addr()
                    .map(|p| format!("{}:{}", p.ip(), p.port()))
                    .unwrap_or_else(|_| "Unknown peer".to_string());

                // Create the necessary per-connection handler state.
                socket.set_nodelay(true)?;

                let (pushed_messages_tx, pushed_messages_rx) =
                    tokio::sync::mpsc::unbounded_channel::<Messages>();

                let mut handler = Handler {
                    chain: self.chain.build_with_pushed_messages(pushed_messages_tx),
                    client_details: peer,
                    conn_details: conn_string,
                    source_details: self.source_name.clone(),

                    // The connection state needs a handle to the max connections
                    // semaphore. When the handler is done processing the
                    // connection, a permit is added back to the semaphore.
                    codec: self.codec.clone(),
                    limit_connections: self.limit_connections.clone(),

                    // Receive shutdown notifications.
                    shutdown: Shutdown::new(self.trigger_shutdown_rx.clone()),

                    terminate_tasks: None,
                    tls: self.tls.clone(),
                    timeout: self.timeout,
                };

                // Spawn a new task to process the connections.
                self.connection_handles.push(tokio::spawn(
                    async move {
                        tracing::debug!("New connection from {}", handler.conn_details);

                        // Process the connection. If an error is encountered, log it.
                        if let Err(err) = handler.run(socket, pushed_messages_rx).await {
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
    client_details: String,
    conn_details: String,

    #[allow(dead_code)]
    source_details: String,
    codec: C,

    /// Max connection semaphore.
    ///
    /// When the handler is dropped, a permit is returned to this semaphore. If
    /// the listener is waiting for connections to close, it will be notified of
    /// the newly available permit and resume accepting connections.
    limit_connections: Arc<Semaphore>,

    /// Listen for shutdown notifications.
    ///
    /// A wrapper around the `broadcast::Receiver` paired with the sender in
    /// `Listener`. The connection handler processes requests from the
    /// connection until the peer disconnects **or** a shutdown notification is
    /// received from `shutdown`. In the latter case, any in-flight work being
    /// processed for the peer is continued until it reaches a safe state, at
    /// which point the connection is terminated.
    shutdown: Shutdown,

    terminate_tasks: Option<watch::Sender<()>>,
    tls: Option<TlsAcceptor>,

    /// Timeout in seconds after which to kill an idle connection. No timeout means connections will never be timed out.
    timeout: Option<u64>,
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
    mut terminate_tasks_rx: watch::Receiver<()>,
) {
    let (decoder, encoder) = codec.build();
    let mut reader = FramedRead::new(rx, decoder);
    let mut writer = FramedWrite::new(tx, encoder);

    // Shutdown flows
    //
    // main task shuts down due to transform error:
    // 1. The main task terminates, sending terminate_tasks_tx and dropping the first out_tx
    // 2. The reader task detects change on terminate_tasks_rx and terminates, the last out_tx instance is dropped
    // 3. The writer task detects that the last out_tx is dropped by out_rx returning None and terminates
    //
    // client closes connection:
    // 1. The reader task detects that the client has closed the connection via reader returning None and terminates, dropping in_tx and the first out_tx
    // 2. The main task detects that in_tx is dropped by in_rx returning None and terminates, dropping the last out_tx
    // 3. The writer task detects that the last out_tx is dropped by out_rx returning None and terminates

    // reader task
    tokio::spawn(
        async move {
            loop {
                tokio::select! {
                    result = reader.next() => {
                        if let Some(message) = result {
                            match message {
                                Ok(messages) => {
                                    if let Err(error) = in_tx.send(messages) {
                                        warn!("failed to pass on received message: {}", error);
                                        return;
                                    }
                                }
                                Err(CodecReadError::RespondAndThenCloseConnection(messages)) => {
                                    if let Err(err) = out_tx.send(messages) {
                                        error!("Failed to send RespondAndThenShutdown message: {:?}", err);
                                    }
                                    return;
                                }
                                Err(CodecReadError::Parser(err)) => {
                                    warn!("failed to decode message: {:?}", err);
                                    return;
                                }
                                Err(CodecReadError::Io(err)) => {
                                    warn!("failed to receive message on tcp stream: {:?}", err);
                                    return;
                                }
                            }
                        } else {
                            debug!("client has closed the connection");
                            return;
                        }
                    }
                    _ = terminate_tasks_rx.changed() => {
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
                    if let Err(err) = writer.send(message).await {
                        error!("failed to send or encode message: {:?}", err);
                    }
                } else {
                    // Main task has ended.
                    // First flush out any remaining messages.
                    // Then end the task thus closing the connection by dropping the write half
                    while let Ok(message) = out_rx.try_recv() {
                        if let Err(err) = writer.send(message).await {
                            error!(
                                "while flushing messages: failed to send or encode message: {err:?}",
                            );
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
    pub async fn run(
        &mut self,
        stream: TcpStream,
        pushed_messages_rx: UnboundedReceiver<Messages>,
    ) -> Result<()> {
        let (terminate_tx, terminate_rx) = watch::channel::<()>(());
        self.terminate_tasks = Some(terminate_tx);

        let (in_tx, in_rx) = mpsc::unbounded_channel::<Messages>();
        let (out_tx, out_rx) = mpsc::unbounded_channel::<Messages>();

        let local_addr = stream.local_addr()?;

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
                terminate_rx,
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
                terminate_rx,
            );
        };

        let result = self
            .process_messages(local_addr, in_rx, out_tx, pushed_messages_rx)
            .await;

        // Flush messages regardless of if we are shutting down due to a failure or due to application shutdown
        match self
            .chain
            .process_request(
                Wrapper::flush_with_chain_name(self.chain.name.clone()),
                self.client_details.clone(),
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
        local_addr: SocketAddr,
        mut in_rx: mpsc::UnboundedReceiver<Messages>,
        out_tx: mpsc::UnboundedSender<Messages>,
        mut pushed_messages_rx: UnboundedReceiver<Messages>,
    ) -> Result<()> {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        let mut idle_time_seconds: u64 = 1;

        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown signal
            debug!("Waiting for message");
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
                                    debug!("Connection Idle for more than {} seconds {}", timeout, self.conn_details);
                                } else {
                                    debug!("Dropping. Connection Idle for more than {} seconds {}", timeout, self.conn_details);
                                    return Ok(());
                                }
                            }
                            idle_time_seconds *= 2;
                            continue
                        }
                    }
                },
                Some(res) = pushed_messages_rx.recv() => {
                    reverse_chain = true;
                    res
                },
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            debug!("Received raw message {:?}", messages);
            debug!("client details: {:?}", &self.client_details);

            let mut error_report_messages = if reverse_chain {
                // Avoid allocating for reverse chains as we dont make use of this value in that case
                vec![]
            } else {
                // This clone should be cheap as cloning a Message that has never had `.frame()` called should result in no new allocations.
                messages.clone()
            };

            let wrapper = Wrapper::new_with_client_details(
                messages,
                self.client_details.clone(),
                self.chain.name.clone(),
                local_addr,
            );

            let modified_messages = if reverse_chain {
                self.chain
                    .process_request_rev(wrapper, self.client_details.clone())
                    .await
                    .context("Chain failed to receive pushed messages/events, the connection will now be closed.")?
            } else {
                match self
                    .chain
                    .process_request(wrapper, self.client_details.clone())
                    .await
                    .context("Chain failed to send and/or receive messages, the connection will now be closed.")
                {
                    Ok(x) => x,
                    Err(err) => {
                        // An internal error occured and we need to terminate the connection because we can no longer make any gaurantees about the state its in.
                        // However before we do that we need to return errors for all the messages in this batch for two reasons:
                        // * Poorly programmed clients may hang forever waiting for a response
                        // * We want to give the user a hint as to what went wrong
                        //     + they might not know to check the shotover logs
                        //     + they may not be able to correlate which error in the shotover logs corresponds to their failed message
                        for m in &mut error_report_messages {
                            *m = m.to_error_response(format!("Internal shotover (or custom transform) bug: {err:?}"));
                        }
                        out_tx.send(error_report_messages)?;
                        return Err(err);
                    }
                }
            };

            debug!("sending message: {:?}", modified_messages);
            // send the result of the process up stream
            out_tx.send(modified_messages)?;
        }

        Ok(())
    }
}

impl<C: CodecBuilder> Drop for Handler<C> {
    fn drop(&mut self) {
        // Add a permit back to the semaphore.
        //
        // Doing so unblocks the listener if the max number of
        // connections has been reached.
        //
        // This is done in a `Drop` implementation in order to guarantee that
        // the permit is added even if the task handling the connection panics.
        // If `add_permit` was called at the end of the `run` function and some
        // bug causes a panic. The permit would never be returned to the
        // semaphore.

        self.limit_connections.add_permits(1);

        if let Some(terminate_tasks) = &self.terminate_tasks {
            terminate_tasks.send(()).ok();
        }
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
