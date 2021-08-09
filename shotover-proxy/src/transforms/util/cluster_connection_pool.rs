use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use derivative::Derivative;
use futures::StreamExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::trace;
use tracing::{debug, warn};

use crate::server::CodecReadHalf;
use crate::server::CodecWriteHalf;
use crate::transforms::util::ConnectionError;
use crate::transforms::util::Request;
use crate::{message::Messages, server::Codec};

type Address = String;
pub type Connection = UnboundedSender<Request>;
pub type Lane = HashMap<Address, Vec<Connection>>;

#[async_trait]
pub trait Authenticator<T> {
    type Error: std::error::Error + Sync + Send + 'static;
    async fn authenticate(&self, sender: &mut Connection, token: &T) -> Result<(), Self::Error>;
}

// TODO: Replace with trait_alias (RFC#1733).
pub trait Token: Send + Sync + std::hash::Hash + Eq + Clone + fmt::Debug {}
impl<T: Send + Sync + std::hash::Hash + Eq + Clone + fmt::Debug> Token for T {}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct ConnectionPool<C: Codec, A: Authenticator<T>, T: Token> {
    lanes: Arc<Mutex<HashMap<Option<T>, Lane>>>,

    #[derivative(Debug = "ignore")]
    codec: C,

    #[derivative(Debug = "ignore")]
    authenticator: A,
}

impl<C: Codec + 'static, A: Authenticator<T>, T: Token> ConnectionPool<C, A, T> {
    // TODO: Support non-authenticated connection pools (with RFC#1216?).
    pub fn new_with_auth(codec: C, authenticator: A) -> Self {
        Self {
            lanes: Arc::new(Mutex::new(HashMap::new())),
            codec,
            authenticator,
        }
    }

    /// Try and grab an existing connection, if it's closed (e.g. the listener on the other side
    /// has closed due to a TCP error), we'll try to reconnect and return the new connection while
    /// updating the connection map. Errors are returned when a connection can't be established.
    pub async fn get_connections(
        &self,
        address: Address,
        token: &Option<T>,
        connection_count: usize,
    ) -> Result<Vec<Connection>, ConnectionError<A::Error>> {
        // TODO: Extract return type using generic associated types (RFC#1598).

        trace!(
            "getting {} pool connections to {} with token: {:?}",
            connection_count,
            address,
            token
        );

        let mut lanes = self.lanes.lock().await;
        let lane = lanes.entry(token.clone()).or_insert_with(HashMap::new);

        let mut connections: Vec<Connection> = lane
            .get_mut(&address)
            .map(|existing_connections| {
                existing_connections.retain(|connection| !connection.is_closed());
                existing_connections
                    .iter()
                    .filter(|connection| !connection.is_closed())
                    .take(connection_count)
                    .cloned()
            })
            .into_iter()
            .flatten()
            .collect();

        let shortfall = connection_count - connections.len();

        if shortfall > 0 {
            // We need more connections, but let's create one at a time for now.
            connections.append(&mut self.new_connections(&address, token, 1).await?);
        }

        // NOTE: This replaces the whole lane (i.e. disowns the old one).
        // IDEA: Maintain weak references so the pool can track active count including disowned?
        lane.insert(address, connections.clone());

        return Ok(connections);
    }

    /// Create multiple connections not owned by the pool.
    async fn new_connections(
        &self,
        address: &Address,
        token: &Option<T>,
        connection_count: usize,
    ) -> Result<Vec<Connection>, ConnectionError<A::Error>> {
        let mut connections: Vec<Connection> = Vec::new();
        let mut errors = Vec::new();

        for i in 0..connection_count {
            match self.new_connection(address, &token).await {
                Ok(connection) => {
                    connections.push(connection);
                }
                Err(error) => {
                    debug!(
                        "Could not authenticate to upstream TCP service for connection {}/{} to {} - {}",
                        i + 1,
                        connection_count,
                        address,
                        error
                    );
                    errors.push(error);
                }
            }
        }

        if connections.is_empty() && !errors.is_empty() {
            // On total failure, propagate any error.
            return Err(errors.into_iter().next().unwrap());
        } else if connections.len() < connection_count {
            warn!(
                "attempted {} connections, but only {} succeeded",
                connection_count,
                connections.len()
            );
        }

        Ok(connections)
    }

    /// Create a connection that is not owned by the pool.
    pub async fn new_connection(
        &self,
        address: &Address,
        token: &Option<T>,
    ) -> Result<Connection, ConnectionError<A::Error>> {
        let stream: TcpStream = TcpStream::connect(address)
            .await
            .map_err(ConnectionError::IO)?;

        let mut connection = spawn_from_stream(&self.codec, stream);

        if let Some(token) = token {
            self.authenticator
                .authenticate(&mut connection, token)
                .await
                .map_err(ConnectionError::Authenticator)?;
        }

        Ok(connection)
    }
}

pub fn spawn_from_stream<C: Codec + 'static>(codec: &C, stream: TcpStream) -> Connection {
    let (read, write) = stream.into_split();
    let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
    let (return_tx, return_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(tx_process(
        write,
        out_rx,
        return_tx,
        shutdown_rx,
        codec.clone(),
    ));
    tokio::spawn(rx_process(read, return_rx, shutdown_tx, codec.clone()));

    out_tx
}

async fn tx_process<C: CodecWriteHalf>(
    write: OwnedWriteHalf,
    out_rx: UnboundedReceiver<Request>,
    return_tx: Connection,
    shutdown_rx: oneshot::Receiver<()>,
    codec: C,
) -> Result<()> {
    let in_w = FramedWrite::new(write, codec.clone());
    let rx_stream = UnboundedReceiverStream::new(out_rx).map(|x| {
        let ret = Ok(Messages {
            messages: vec![x.messages.clone()],
        });
        return_tx.send(x)?;
        ret
    });

    tokio::select! {
       _ = rx_stream.forward(in_w) => trace!("connection write-closed by writer"),
       _ = shutdown_rx => {
           trace!("connection write-closed by reader");
           debug!("connection closed by remote upstream")
       },
    }

    Ok(())
}

async fn rx_process<C: CodecReadHalf>(
    read: OwnedReadHalf,
    mut return_rx: UnboundedReceiver<Request>,
    shutdown_tx: oneshot::Sender<()>,
    codec: C,
) -> Result<()> {
    let mut in_r = FramedRead::new(read, codec.clone());

    while let Some(maybe_req) = in_r.next().await {
        match maybe_req {
            Ok(req) => {
                for m in req {
                    if let Some(Request {
                        messages,
                        return_chan: Some(ret),
                        message_id: _,
                    }) = return_rx.recv().await
                    {
                        // If the receiver hangs up, just silently ignore
                        let _ = ret.send((messages, Ok(Messages { messages: vec![m] })));
                    }
                }
            }
            Err(e) => {
                debug!("Couldn't decode message from upstream host {:?}", e);

                // Stop writer if not already stopped.
                let _ = shutdown_tx.send(());

                return Err(anyhow!(
                    "Couldn't decode message from upstream host {:?}",
                    e
                ));
            }
        }
    }

    trace!("connection read-closed");

    // Stop writer if not already stopped.
    let _ = shutdown_tx.send(());

    Ok(())
}
