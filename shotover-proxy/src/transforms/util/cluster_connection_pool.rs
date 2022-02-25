use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use derivative::Derivative;
use futures::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, trace, warn, Instrument};

use super::Response;
use crate::server::Codec;
use crate::server::CodecReadHalf;
use crate::server::CodecWriteHalf;
use crate::tls::{TlsConfig, TlsConnector};
use crate::transforms::util::{ConnectionError, Request};

pub type Connection = UnboundedSender<Request>;
pub type Lane = HashMap<String, Vec<Connection>>;

#[async_trait]
pub trait Authenticator<T> {
    type Error: std::error::Error + Sync + Send + 'static;
    async fn authenticate(&self, sender: &mut Connection, token: &T) -> Result<(), Self::Error>;
}

#[derive(thiserror::Error, Debug)]
pub enum NoopError {}

#[derive(Clone)]
pub struct NoopAuthenticator {}

#[async_trait]
impl Authenticator<()> for NoopAuthenticator {
    type Error = NoopError;

    async fn authenticate(&self, _sender: &mut Connection, _token: &()) -> Result<(), Self::Error> {
        Ok(())
    }
}

// TODO: Replace with trait_alias (rust-lang/rust#41517).
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

    #[derivative(Debug = "ignore")]
    tls: Option<TlsConnector>,
}

impl<C: Codec + 'static> ConnectionPool<C, NoopAuthenticator, ()> {
    pub fn new(codec: C, tls: Option<TlsConfig>) -> Result<Self> {
        ConnectionPool::new_with_auth(codec, NoopAuthenticator {}, tls)
    }
}

impl<C: Codec + 'static, A: Authenticator<T>, T: Token> ConnectionPool<C, A, T> {
    pub fn new_with_auth(codec: C, authenticator: A, tls: Option<TlsConfig>) -> Result<Self> {
        Ok(Self {
            lanes: Arc::new(Mutex::new(HashMap::new())),
            tls: tls.map(TlsConnector::new).transpose()?,
            codec,
            authenticator,
        })
    }

    /// Try and grab an existing connection, if it's closed (e.g. the listener on the other side
    /// has closed due to a TCP error), we'll try to reconnect and return the new connection while
    /// updating the connection map. Errors are returned when a connection can't be established.
    pub async fn get_connections(
        &self,
        address: &str,
        token: &Option<T>,
        connection_count: usize,
    ) -> Result<Vec<Connection>, ConnectionError<A::Error>> {
        debug!(
            "getting {} pool connections to {} with token: {:?}",
            connection_count, address, token
        );

        let mut lanes = self.lanes.lock().await;
        let lane = lanes.entry(token.clone()).or_insert_with(HashMap::new);
        let address = address.to_string();

        let mut connections: Vec<Connection> = lane
            .get_mut(&address)
            .map(|existing_connections| {
                existing_connections.retain(|connection| !connection.is_closed());
                existing_connections.iter().take(connection_count).cloned()
            })
            .into_iter()
            .flatten()
            .collect();

        let shortfall_count = connection_count.saturating_sub(connections.len());

        if shortfall_count > 0 {
            // IDEA: Set min/max connections at the pool level? Limit number of new connections per call?
            connections.append(
                &mut self
                    .new_unpooled_connections(&address, token, shortfall_count)
                    .await?,
            );
        }

        // NOTE: This replaces the whole lane, disowning the old one.
        // IDEA: Maintain weak references so the pool can track disowned connections?
        lane.insert(address, connections.clone());

        Ok(connections)
    }

    async fn new_unpooled_connections(
        &self,
        address: &str,
        token: &Option<T>,
        connection_count: usize,
    ) -> Result<Vec<Connection>, ConnectionError<A::Error>> {
        let mut connections = Vec::new();
        let mut errors = Vec::new();

        for i in 1..=connection_count {
            match self.new_unpooled_connection(address, token).await {
                Ok(connection) => {
                    connections.push(connection);
                }
                Err(error) => {
                    debug!(
                        "Failed to connect to upstream TCP service for connection {}/{} to {} - {}",
                        i, connection_count, address, error
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

    pub async fn new_unpooled_connection(
        &self,
        address: &str,
        token: &Option<T>,
    ) -> Result<Connection, ConnectionError<A::Error>> {
        let stream = timeout(Duration::from_secs(3), TcpStream::connect(address))
            .await
            .map_err(|e| ConnectionError::IO(e.into()))?
            .map_err(ConnectionError::IO)?;

        let mut connection = if let Some(tls) = &self.tls {
            let tls_stream = tls
                .connect_unverified_hostname(stream)
                .await
                .map_err(ConnectionError::TLS)?;
            let (rx, tx) = tokio::io::split(tls_stream);
            spawn_read_write_tasks(&self.codec, rx, tx)
        } else {
            let (rx, tx) = stream.into_split();
            spawn_read_write_tasks(&self.codec, rx, tx)
        };

        if let Some(token) = token {
            self.authenticator
                .authenticate(&mut connection, token)
                .await
                .map_err(ConnectionError::Authenticator)?;
        }

        Ok(connection)
    }
}

pub fn spawn_read_write_tasks<
    C: Codec + 'static,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
>(
    codec: &C,
    stream_rx: R,
    stream_tx: W,
) -> Connection {
    let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
    let (return_tx, return_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();

    let codec_clone = codec.clone();

    tokio::spawn(async move {
        tokio::select! {
            result = tx_process(stream_tx, out_rx, return_tx, codec_clone) => if let Err(e) = result {
                trace!("connection write-closed with error: {:?}", e);
            } else {
                trace!("connection write-closed gracefully");
            },
            _ = closed_rx => {
                trace!("connection write-closed by remote upstream");
            },
        }
    }.in_current_span());

    let codec_clone = codec.clone();

    tokio::spawn(
        async move {
            if let Err(e) = rx_process(stream_rx, return_rx, codec_clone).await {
                trace!("connection read-closed with error: {:?}", e);
            } else {
                trace!("connection read-closed gracefully");
            }

            // Signal the writer to also exit, which then closes `out_tx` - what we consider as the connection.
            closed_tx.send(())
        }
        .in_current_span(),
    );

    out_tx
}

async fn tx_process<C: CodecWriteHalf, W: AsyncWrite + Unpin + Send + 'static>(
    write: W,
    out_rx: UnboundedReceiver<Request>,
    return_tx: UnboundedSender<Request>,
    codec: C,
) -> Result<()> {
    let in_w = FramedWrite::new(write, codec);
    let rx_stream = UnboundedReceiverStream::new(out_rx).map(|x| {
        let ret = Ok(vec![x.message.clone()]);
        return_tx.send(x)?;
        ret
    });
    rx_stream.forward(in_w).await
}

async fn rx_process<C: CodecReadHalf, R: AsyncRead + Unpin + Send + 'static>(
    read: R,
    mut return_rx: UnboundedReceiver<Request>,
    codec: C,
) -> Result<()> {
    let mut in_r = FramedRead::new(read, codec);

    while let Some(maybe_req) = in_r.next().await {
        match maybe_req {
            Ok(req) => {
                for m in req {
                    if let Some(Request {
                        message,
                        return_chan: Some(ret),
                        ..
                    }) = return_rx.recv().await
                    {
                        // If the receiver hangs up, just silently ignore
                        let _ = ret.send(Response {
                            original: message,
                            response: Ok(vec![m]),
                        });
                    }
                }
            }
            Err(e) => {
                debug!("Couldn't decode message from upstream host {:?}", e);
                return Err(anyhow!(
                    "Couldn't decode message from upstream host {:?}",
                    e
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use std::mem;
    use std::time::Duration;

    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    use super::spawn_read_write_tasks;
    use crate::codec::redis::RedisCodec;

    #[tokio::test]
    async fn test_remote_shutdown() {
        let (log_writer, _log_guard) = tracing_appender::non_blocking(std::io::stdout());
        mem::forget(_log_guard);

        let builder = tracing_subscriber::fmt()
            .with_writer(log_writer)
            .with_env_filter("INFO")
            .with_filter_reloading();

        let _handle = builder.reload_handle();
        builder.try_init().ok();

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let remote = tokio::spawn(async move {
            // Accept connection and immediately close.
            listener.accept().await.is_ok()
        });

        let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let (rx, tx) = stream.into_split();
        let codec = RedisCodec::new();
        let sender = spawn_read_write_tasks(&codec, rx, tx);

        assert!(remote.await.unwrap());

        assert!(
            // NOTE: Typically within 1-10ms.
            timeout(Duration::from_millis(100), sender.closed())
                .await
                .is_ok(),
            "local did not detect remote shutdown"
        );
    }

    #[tokio::test]
    async fn test_local_shutdown() {
        let (log_writer, _log_guard) = tracing_appender::non_blocking(std::io::stdout());
        mem::forget(_log_guard);

        let builder = tracing_subscriber::fmt()
            .with_writer(log_writer)
            .with_env_filter("INFO")
            .with_filter_reloading();

        let _handle = builder.reload_handle();
        builder.try_init().ok();

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let remote = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            // Discard bytes until EOF.
            let mut buffer = [0; 1];
            while socket.read(&mut buffer[..]).await.unwrap() > 0 {}
        });

        let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let (rx, tx) = stream.into_split();
        let codec = RedisCodec::new();

        // Drop sender immediately.
        std::mem::drop(spawn_read_write_tasks(&codec, rx, tx));

        assert!(
            // NOTE: Typically within 1-10ms.
            timeout(Duration::from_millis(100), remote).await.is_ok(),
            "remote did not detect local shutdown"
        );
    }
}
