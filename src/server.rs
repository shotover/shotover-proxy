use crate::transforms::build_chain_from_config;
use crate::transforms::chain::TransformChain;
use anyhow::Result;
use futures::StreamExt;
use metrics::gauge;
use shotover_transforms::Wrapper;
use shotover_transforms::{Messages, TopicHolder};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time;
use tokio::time::timeout;
use tokio::time::Duration;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{Decoder, Encoder};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, error, info, trace, warn};

pub struct TcpCodecListener<C>
where
    C: Decoder<Item = Messages> + Encoder<Messages, Error = anyhow::Error> + Clone + Send,
{
    /// Shared database handle.
    ///
    /// Contains the key / value store as well as the broadcast channels for
    /// pub/sub.
    ///
    /// This is a wrapper around an `Arc`. This enables `db` to be cloned and
    /// passed into the per connection state (`Handler`).
    pub chain: TransformChain,

    pub source_name: String,

    /// TCP listener supplied by the `run` caller.
    pub listener: Option<TcpListener>,
    pub listen_addr: String,
    pub hard_connection_limit: bool,

    pub codec: C,

    /// Limit the max number of connections.
    ///
    /// A `Semaphore` is used to limit the max number of connections. Before
    /// attempting to accept a new connection, a permit is acquired from the
    /// semaphore. If none are available, the listener waits for one.
    ///
    /// When handlers complete processing a connection, the permit is returned
    /// to the semaphore.
    pub limit_connections: Arc<Semaphore>,

    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shutdown is initiated, a `()` value is sent via
    /// the broadcast::Sender. Each active connection receives it, reaches a
    /// safe terminal state, and completes the task.
    pub notify_shutdown: broadcast::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all connection handlers completing. When a
    /// connection handler is initialized, it is assigned a clone of
    /// `shutdown_complete_tx`. When the listener shuts down, it drops the
    /// sender held by this `shutdown_complete_tx` field. Once all handler tasks
    /// complete, all clones of the `Sender` are also dropped. This results in
    /// `shutdown_complete_rx.recv()` completing with `None`. At this point, it
    /// is safe to exit the server process.
    pub shutdown_complete_tx: mpsc::Sender<()>,
}

impl<C> TcpCodecListener<C>
where
    C: 'static + Decoder<Item = Messages> + Encoder<Messages, Error = anyhow::Error> + Clone + Send,
{
    /// Run the server
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` if accepting returns an error. This can happen for a
    /// number reasons that resolve over time. For example, if the underlying
    /// operating system has reached an internal limit for max number of
    /// sockets, accept will fail.
    ///
    /// The process is not able to detect when a transient error resolves
    /// itself. One strategy for handling this is to implement a back off
    /// strategy, which is what we do here.
    pub async fn run(&mut self) -> Result<()>
    where
        <C as Decoder>::Error: std::marker::Send + std::fmt::Debug,
    {
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
            // self.limit_connections.acquire().await.forget();

            if self.hard_connection_limit {
                match self.limit_connections.try_acquire() {
                    Ok(p) => {
                        if self.listener.is_none() {
                            self.listener =
                                Some(TcpListener::bind(self.listen_addr.clone()).await.unwrap());
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
                    self.listener =
                        Some(TcpListener::bind(self.listen_addr.clone()).await.unwrap());
                }
            }

            // Accept a new socket. This will attempt to perform error handling.
            // The `accept` method internally attempts to recover errors, so an
            // error here is non-recoverable.
            let socket = self.accept().await?;

            gauge!("shotover_available_connections", self.limit_connections.available_permits() as i64 ,"source" => self.source_name.clone());

            let peer = socket
                .peer_addr()
                .map(|p| format!("{}", p.ip()))
                .unwrap_or_else(|_| "Unknown peer".to_string());

            let conn_string = socket
                .peer_addr()
                .map(|p| format!("{}:{}", p.ip(), p.port()))
                .unwrap_or_else(|_| "Unknown peer".to_string());

            // Create the necessary per-connection handler state.
            info!(
                "New connection from {}",
                socket
                    .peer_addr()
                    .map(|p| format!("{}", p))
                    .unwrap_or_else(|_| "Unknown peer".to_string())
            );

            socket.set_nodelay(true)?;

            let mut handler = Handler {
                // Get a handle to the shared database. Internally, this is an
                // `Arc`, so a clone only increments the ref count.
                // chain: self.chain.clone(),
                chain: build_chain_from_config(
                    self.chain.name.clone(),
                    self.chain.config_objs.as_slice(),
                    &TopicHolder::new(),
                )
                .await?,
                client_details: peer,
                conn_details: conn_string,
                source_details: self.source_name.clone(),

                // Initialize the connection state. This allocates read/write
                // buffers to perform redis protocol frame parsing.

                // The connection state needs a handle to the max connections
                // semaphore. When the handler is done processing the
                // connection, a permit is added back to the semaphore.
                codec: self.codec.clone(),
                // connection: Framed::new(socket, self.codec.clone()),
                limit_connections: self.limit_connections.clone(),

                // Receive shutdown notifications.
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),

                // Notifies the receiver half once all clones are
                // dropped.
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };
            // let chain = self.chain.clone();

            // Spawn a new task to process the connections. Tokio tasks are like
            // asynchronous green threads and are executed concurrently.
            tokio::spawn(async move {
                // Process the connection. If an error is encountered, log it.
                if let Err(err) = handler.run(socket).await {
                    error!(cause = ?err, "connection error");
                }
            });
        }
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

pub struct Handler<C>
where
    C: Decoder<Item = Messages> + Encoder<Messages, Error = anyhow::Error> + Clone + Send,
{
    /// Shared source handle.
    ///
    /// When a command is received from `connection`, it is applied with `db`.
    /// The implementation of the command is in the `cmd` module. Each command
    /// will need to interact with `db` in order to complete the work.
    chain: TransformChain,
    client_details: String,
    conn_details: String,

    #[allow(dead_code)]
    source_details: String,
    codec: C,

    // connection: Framed<S, C>,
    /// The TCP connection decorated with the redis protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.

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

    /// Not used directly. Instead, when `Handler` is dropped...?
    _shutdown_complete: mpsc::Sender<()>,
}

impl<C> Handler<C>
where
    C: Decoder<Item = Messages> + Encoder<Messages, Error = anyhow::Error> + Clone + Send + 'static,
    // S: AsyncRead + AsyncWrite + Unpin,
{
    /// Process a single connection.
    ///
    /// Request frames are read from the socket and processed. Responses are
    /// written back to the socket.
    ///
    /// Currently, pipelining is not implemented. Pipelining is the ability to
    /// process more than one request concurrently per connection without
    /// interleaving frames. See for more details:
    /// https://redis.io/topics/pipelining
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    // #[instrument(skip(self))]
    pub async fn run(&mut self, stream: TcpStream) -> Result<()>
    where
        <C as Decoder>::Error: std::fmt::Debug,
    {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        let mut idle_time: u64 = 1;

        let (in_tx, mut in_rx) = tokio::sync::mpsc::unbounded_channel::<Messages>();
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<Messages>();

        let (rx, tx) = stream.into_split();

        let mut reader = FramedRead::new(rx, self.codec.clone());
        let writer = FramedWrite::new(tx, self.codec.clone());

        tokio::spawn(async move {
            while let Some(maybe_message) = reader.next().await {
                match maybe_message {
                    Ok(resp_messages) => {
                        let _ = in_tx.send(resp_messages);
                    }
                    Err(e) => {
                        warn!("Frame error - {:?}", e);
                        break;
                    }
                };
            }
        });

        tokio::spawn(async move {
            let rx_stream = UnboundedReceiverStream::new(out_rx).map(|x| Ok(x));
            let r = rx_stream.forward(writer).await;
            debug!("Stream ended {:?}", r);
        });

        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal

            trace!("Waiting for message");
            let frame = tokio::select! {
                res = timeout(Duration::from_secs(idle_time) , in_rx.recv()) => {
                    match res {
                        Ok(maybe_message) => {
                            idle_time = 1;
                            match maybe_message {
                                Some(m) => m,
                                None => return Ok(())
                            }
                        },
                        Err(_) => {
                            match idle_time {
                                0..=10 => trace!("Connection Idle for more than {} seconds {}", idle_time, self.conn_details),
                                11..=35 => trace!("Connection Idle for more than {} seconds {}", idle_time, self.conn_details),
                                _ => {
                                    debug!("Dropping Connection Idle for more than {} seconds {}", idle_time, self.conn_details);
                                    return Ok(())
                                }
                            }
                            idle_time *= 2;
                            continue
                        }
                    }

                },
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            // If `None` is returned from `read_frame()` then the peer closed
            // the socket. There is no further work to do and the task can be
            // terminated.

            trace!("Received raw message {:?}", frame);
            match self
                .chain
                .process_request(Wrapper::new(frame), self.client_details.clone())
                .await
            {
                Ok(modified_message) => {
                    out_tx.send(modified_message)?;
                    // let _ = self.chain.lua_runtime.gc_collect(); // TODO is this a good idea??
                }
                Err(e) => {
                    error!("chain processing error - {}", e);
                    return Ok(());
                }
            }

            // match frame {
            //     Ok(message) => {
            //
            //     }
            //     Err(e) => {
            //         trace!("Error handling message in TcpStream source: {:?}", e);
            //         return Ok(());
            //     }
            // }
        }

        Ok(())
    }
}

impl<C> Drop for Handler<C>
where
    C: Decoder<Item = Messages> + Encoder<Messages, Error = anyhow::Error> + Clone + Send,
    //       S: AsyncRead + AsyncWrite + Drop,
{
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
    }
}
/// Listens for the server shutdown signal.
///
/// Shutdown is signalled using a `broadcast::Receiver`. Only a single value is
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
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
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

        // Cannot receive a "lag error" as only one value is ever sent.
        let _ = self.notify.recv().await;

        // Remember that the signal has been received.
        self.shutdown = true;
    }
}
