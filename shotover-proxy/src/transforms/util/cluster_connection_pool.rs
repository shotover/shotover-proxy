use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use derivative::Derivative;
use futures::StreamExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::{Decoder, FramedRead, FramedWrite};
use tracing::{debug, info, trace};

use crate::server::CodecReadHalf;
use crate::server::CodecWriteHalf;
use crate::transforms::util::Request;
use crate::{message::Messages, server::Codec};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct ConnectionPool<C: Codec> {
    host_set: Arc<Mutex<HashSet<String>>>,
    queue_map: Arc<Mutex<HashMap<String, Vec<UnboundedSender<Request>>>>>,

    #[derivative(Debug = "ignore")]
    codec: C,

    #[derivative(Debug = "ignore")]
    auth_func: fn(&ConnectionPool<C>, &mut UnboundedSender<Request>) -> Result<()>,
}

impl<C: Codec + 'static> ConnectionPool<C> {
    pub fn new(hosts: Vec<String>, codec: C) -> Self {
        ConnectionPool {
            host_set: Arc::new(Mutex::new(HashSet::from_iter(hosts.into_iter()))),
            queue_map: Arc::new(Mutex::new(HashMap::new())),
            codec,
            auth_func: |_, _| Ok(()),
        }
    }

    pub fn new_with_auth(
        hosts: Vec<String>,
        codec: C,
        auth_func: fn(&ConnectionPool<C>, &mut UnboundedSender<Request>) -> Result<()>,
    ) -> Self {
        ConnectionPool {
            host_set: Arc::new(Mutex::new(HashSet::from_iter(hosts.into_iter()))),
            queue_map: Arc::new(Mutex::new(HashMap::new())),
            codec,
            auth_func,
        }
    }

    /// Try and grab an existing connection, if it's closed (e.g. the listener on the other side
    /// has closed due to a TCP error), we'll try to reconnect and return the new connection while
    /// updating the connection map. Errors are returned when a connection can't be established.
    pub async fn get_connections(
        &self,
        host: &String,
        connection_count: i32,
    ) -> Result<Vec<UnboundedSender<Request>>> {
        let mut queue_map = self.queue_map.lock().await;
        if let Some(x) = queue_map.get(host) {
            if x.iter().all(|x| !x.is_closed()) {
                return Ok(x.clone());
            }
        }
        let connections = self.new_connections(&host, connection_count).await?;
        queue_map.insert(host.clone(), connections.clone());
        Ok(connections)
    }

    pub async fn new_connections(
        &self,
        host: &String,
        connection_count: i32,
    ) -> Result<Vec<UnboundedSender<Request>>>
    where
        <C as Decoder>::Error: std::marker::Send,
    {
        let mut connections: Vec<UnboundedSender<Request>> = Vec::new();

        for _i in 0..connection_count {
            let stream = TcpStream::connect(host).await?;
            let mut out_tx = spawn_from_stream(&self.codec, stream);

            match (self.auth_func)(&self, &mut out_tx) {
                Ok(_) => {
                    connections.push(out_tx);
                }
                Err(e) => {
                    info!("Could not authenticate to upstream TCP service - {}", e);
                }
            }
        }

        if connections.len() == 0 {
            Err(anyhow!("Couldn't connect to upstream TCP service"))
        } else {
            Ok(connections)
        }
    }
}

pub fn spawn_from_stream<C: Codec + 'static>(
    codec: &C,
    stream: TcpStream,
) -> UnboundedSender<Request> {
    let (read, write) = stream.into_split();
    let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
    let (return_tx, return_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();

    let codec_clone = codec.clone();

    tokio::spawn(async move {
        tokio::select! {
            result = tx_process(write, out_rx, return_tx, codec_clone) => if let Err(e) = result {
                trace!("connection write-closed with error: {:?}", e);
            } else {
                trace!("connection write-closed gracefully");
            },
            _ = closed_rx => {
                trace!("connection write-closed by remote upstream");
            },
        }
    });

    let codec_clone = codec.clone();

    tokio::spawn(async move {
        if let Err(e) = rx_process(read, return_rx, codec_clone).await {
            trace!("connection read-closed with error: {:?}", e);
        } else {
            trace!("connection read-closed gracefully");
        }

        // Signal the writer to also exit, which then closes `out_tx` - what we consider as the connection.
        closed_tx.send(())
    });

    out_tx
}

async fn tx_process<C: CodecWriteHalf>(
    write: OwnedWriteHalf,
    out_rx: UnboundedReceiver<Request>,
    return_tx: UnboundedSender<Request>,
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
    rx_stream.forward(in_w).await
}

async fn rx_process<C: CodecReadHalf>(
    read: OwnedReadHalf,
    mut return_rx: UnboundedReceiver<Request>,
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
    use crate::protocols::redis_codec::RedisCodec;
    use crate::transforms::util::cluster_connection_pool::spawn_from_stream;
    use std::mem;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_remote_shutdown() {
        let (log_writer, _log_guard) = tracing_appender::non_blocking(std::io::stdout());
        mem::forget(_log_guard);

        let builder = tracing_subscriber::fmt()
            .with_writer(log_writer)
            .with_env_filter("TRACE")
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
        let codec = RedisCodec::new(true, 3);
        let sender = spawn_from_stream(&codec, stream);

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
        let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
        mem::forget(_guard);

        let builder = tracing_subscriber::fmt()
            .with_writer(non_blocking)
            .with_env_filter("TRACE")
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
        let codec = RedisCodec::new(true, 3);

        // Drop sender immediately.
        let _ = spawn_from_stream(&codec, stream);

        assert!(
            // NOTE: Typically within 1-10ms.
            timeout(Duration::from_millis(100), remote).await.is_ok(),
            "remote did not detect local shutdown"
        );
    }
}
