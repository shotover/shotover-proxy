use crate::codec::CodecBuilder;
use crate::config::chain::TransformChainConfig;
use crate::connection::{Connection, ConnectionError};
use crate::message::{Message, Messages};
use crate::sources::Transport;
use crate::tls::TlsAcceptor;
use crate::transforms::chain::{TransformChain, TransformChainBuilder};
use crate::transforms::{TransformContextBuilder, TransformContextConfig, Wrapper};
use anyhow::{anyhow, Context, Result};
use futures::future::join_all;
use metrics::{gauge, Gauge};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{watch, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::Duration;
use tracing::Instrument;
use tracing::{debug, error};

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

    /// Timeout after which to kill an idle connection. No timeout means connections will never be timed out.
    timeout: Option<Duration>,

    connection_handles: Vec<JoinHandle<()>>,

    transport: Transport,
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
    ) -> Result<Self, Vec<String>> {
        let available_connections_gauge =
            gauge!("shotover_available_connections_count", "source" => source_name.clone());
        available_connections_gauge.set(limit_connections.available_permits() as f64);

        let chain_usage_config = TransformContextConfig {
            chain_name: source_name.clone(),
            protocol: codec.protocol(),
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

        let listener = match create_listener(&listen_addr).await {
            Ok(listener) => Some(listener),
            Err(error) => {
                errors.push(format!("{error:?}"));
                None
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
            let span =
                crate::connection_span::span(self.connection_count, self.source_name.as_str());
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

                let force_run_chain = Arc::new(Notify::new());
                let context = TransformContextBuilder {
                    force_run_chain: force_run_chain.clone(),
                };

                let handler = Handler {
                    chain: self
                        .chain_builder
                        .build_with_pushed_messages(pushed_messages_tx, context),
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
                        if let Err(err) = handler.run(stream, transport, force_run_chain).await {
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
    timeout: Option<Duration>,
    pushed_messages_rx: UnboundedReceiver<Messages>,
    _permit: OwnedSemaphorePermit,
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
    ) -> Result<()> {
        stream.set_nodelay(true)?;

        let client_details = stream
            .peer_addr()
            .map(|p| p.ip().to_string())
            .unwrap_or_else(|_| "Unknown peer".to_string());
        tracing::debug!("New connection from {}", client_details);

        let local_addr = stream.local_addr()?;
        let codec_builder = self.codec.clone();

        let Some(connection) =
            Connection::new_source(stream, codec_builder, &self.tls, transport).await?
        else {
            return Ok(());
        };

        let result = self
            .process_messages(&client_details, local_addr, connection, force_run_chain)
            .await;

        // Flush messages regardless of if we are shutting down due to a failure or due to application shutdown
        match self.chain.process_request(Wrapper::flush()).await {
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

    async fn receive_with_timeout(
        timeout: Option<Duration>,
        connection: &mut Connection,
        client_details: &str,
    ) -> Result<Vec<Message>, ConnectionError> {
        if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, connection.recv()).await {
                Ok(messages) => messages,
                Err(_) => {
                    debug!("Dropping connection to {client_details} due to being idle for more than {timeout:?}");
                    None
                }
            }
        } else {
            connection.recv().await
        }
    }

    async fn process_messages(
        &mut self,
        client_details: &str,
        local_addr: SocketAddr,
        mut connection: Connection,
        force_run_chain: Arc<Notify>,
    ) -> Result<()> {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown signal
            debug!("Waiting for message {client_details}");
            let responses = tokio::select! {
                biased;
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
                Some(responses) = self.pushed_messages_rx.recv() => {
                    debug!("Received unrequested responses from destination {:?}", responses);
                    self.process_backward(client_details, local_addr, responses).await?
                }
                () = force_run_chain.notified() => {
                    let mut requests = vec!();
                    while let Ok(x) = connection.try_recv() {
                        requests.extend(x);
                    }
                    debug!("A transform in the chain requested that a chain run occur, requests {:?}", requests);
                    self.process_forward(client_details, local_addr, &mut connection, requests).await?
                },
                requests = Self::receive_with_timeout(self.timeout, &mut connection, client_details) => {
                    match requests {
                        Ok(mut requests) => {
                            while let Ok(x) = connection.try_recv() {
                                requests.extend(x);
                            }
                            debug!("Received requests from client {:?}", requests);
                            self.process_forward(client_details, local_addr, &mut connection, requests).await?
                        }
                        Err(err) => {
                            // Either we timed out the connection or the client disconnected, so terminate this connection
                            return Ok(())
                        }
                    }
                },
            };

            // send the result of the process up stream
            if !responses.is_empty() {
                debug!("sending response to client: {:?}", responses);
                if connection.send(responses).is_err() {
                    // the client has disconnected so we should terminate this connection
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    async fn process_forward(
        &mut self,
        client_details: &str,
        local_addr: SocketAddr,
        connection: &mut Connection,
        requests: Messages,
    ) -> Result<Messages> {
        // This clone should be cheap as cloning requests Message that has never had `.frame()`
        // called should result in no new allocations.
        let mut error_report_messages = requests.clone();

        let wrapper =
            Wrapper::new_with_client_details(requests, client_details.to_owned(), local_addr);

        match self.chain.process_request(wrapper).await.context(
            "Chain failed to send and/or receive messages, the connection will now be closed.",
        ) {
            Ok(x) => Ok(x),
            Err(err) => {
                // An internal error occured and we need to terminate the connection because we can no
                // longer make any guarantees about the state its in.
                // However before we do that we need to return errors for all the messages in this batch for two reasons:
                // * Poorly programmed clients may hang forever waiting for a response
                // * We want to give the user a hint as to what went wrong
                //     + they might not know to check the shotover logs
                //     + they may not be able to correlate which error in the shotover logs corresponds to their failed message
                for m in &mut error_report_messages {
                    #[allow(clippy::single_match)]
                    match m.to_error_response(format!(
                        "Internal shotover (or custom transform) bug: {err:?}"
                    )) {
                        Ok(new_m) => *m = new_m,
                        Err(_) => {
                            // If we cant produce an error then nothing we can do, just continue on and close the connection.
                        }
                    }
                }
                connection.send(error_report_messages)?;
                Err(err)
            }
        }
    }

    async fn process_backward(
        &mut self,
        client_details: &str,
        local_addr: SocketAddr,
        responses: Messages,
    ) -> Result<Messages> {
        let wrapper =
            Wrapper::new_with_client_details(responses, client_details.to_owned(), local_addr);

        self.chain.process_request_rev(wrapper).await.context(
            "Chain failed to receive pushed messages/events, the connection will now be closed.",
        )
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
