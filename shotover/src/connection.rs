//! This is the one true connection implementation that all other transforms + server.rs should be ported to.

use crate::codec::{CodecBuilder, CodecReadError, CodecWriteError};
use crate::message::{Message, Messages};
use crate::tcp;
use crate::tls::{TlsConnector, ToHostname};
use futures::{SinkExt, StreamExt};
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{split, AsyncRead, AsyncWrite};
use tokio::net::ToSocketAddrs;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, Notify};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::error;
use tracing::Instrument;

pub struct Connection {
    in_rx: mpsc::Receiver<Vec<Message>>,
    out_tx: mpsc::UnboundedSender<Vec<Message>>,
    connection_closed_rx: mpsc::Receiver<ConnectionError>,
    error: Option<ConnectionError>,
}

impl Connection {
    pub async fn new<A: ToSocketAddrs + ToHostname + std::fmt::Debug, C: CodecBuilder + 'static>(
        host: A,
        codec_builder: C,
        tls: &mut Option<TlsConnector>,
        connect_timeout: Duration,
        force_run_chain: Option<Arc<Notify>>,
    ) -> anyhow::Result<Self> {
        let destination = tokio::net::lookup_host(&host).await?.next().unwrap();
        let (in_tx, in_rx) = mpsc::channel::<Messages>(10_000);
        let (out_tx, out_rx) = mpsc::unbounded_channel::<Messages>();
        let (connection_closed_tx, connection_closed_rx) = mpsc::channel(1);

        if let Some(tls) = tls.as_mut() {
            let tls_stream = tls.connect(connect_timeout, host).await?;
            let (rx, tx) = split(tls_stream);
            spawn_read_write_tasks(
                codec_builder,
                rx,
                tx,
                in_tx,
                out_rx,
                out_tx.clone(),
                force_run_chain,
                connection_closed_tx,
            );
        } else {
            let tcp_stream = tcp::tcp_stream(connect_timeout, destination).await?;
            let (rx, tx) = tcp_stream.into_split();
            spawn_read_write_tasks(
                codec_builder,
                rx,
                tx,
                in_tx,
                out_rx,
                out_tx.clone(),
                force_run_chain,
                connection_closed_tx,
            );
        }

        Ok(Connection {
            in_rx,
            out_tx,
            connection_closed_rx,
            error: None,
        })
    }

    fn set_get_error(&mut self) -> ConnectionError {
        self.error = Some(self.connection_closed_rx.try_recv().unwrap());
        self.error.clone().unwrap()
    }

    /// Send messages.
    /// If there is a problem with the connection an error is returned.
    pub fn send(&mut self, messages: Vec<Message>) -> Result<(), ConnectionError> {
        if let Some(error) = &self.error {
            Err(error.clone())
        } else {
            self.out_tx.send(messages).map_err(|_| self.set_get_error())
        }
    }

    /// Receives messages, if there are no messages available it awaits until there are messages.
    /// If there is a problem with the connection an error is returned.
    pub async fn recv(&mut self) -> Result<Vec<Message>, ConnectionError> {
        if let Some(error) = &self.error {
            Err(error.clone())
        } else {
            match self.in_rx.recv().await {
                Some(value) => Ok(value),
                None => Err(self.set_get_error()),
            }
        }
    }

    /// Attempts to receive messages, if there are no messages available it immediately returns an empty vec.
    /// If there is a problem with the connection an error is returned.
    pub fn try_recv(&mut self) -> Result<Vec<Message>, ConnectionError> {
        if let Some(error) = &self.error {
            Err(error.clone())
        } else {
            match self.in_rx.try_recv() {
                Ok(value) => Ok(value),
                Err(TryRecvError::Disconnected) => Err(self.set_get_error()),
                Err(TryRecvError::Empty) => Ok(vec![]),
            }
        }
    }
}

/// This represents an unrecoverable error to the connection.
/// The connection is no longer usable after this error is received.
#[derive(thiserror::Error, Debug, Clone)]
pub enum ConnectionError {
    #[error("The other side of this connection closed the connection")]
    OtherSideClosed,
    #[error("Shotover closed the connection due to protocol requirements")]
    ShotoverClosed,
    #[error("Message decode error {0}")]
    MessageDecode(Arc<anyhow::Error>),
    #[error("Message encode error {0}")]
    MessageEncode(Arc<anyhow::Error>),
    #[error("IO error {0}")]
    Io(Arc<std::io::Error>),
}

#[allow(clippy::too_many_arguments)]
fn spawn_read_write_tasks<
    C: CodecBuilder + 'static,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
>(
    codec: C,
    rx: R,
    tx: W,
    in_tx: mpsc::Sender<Messages>,
    out_rx: UnboundedReceiver<Messages>,
    out_tx: UnboundedSender<Messages>,
    force_run_chain: Option<Arc<Notify>>,
    connection_closed_tx: mpsc::Sender<ConnectionError>,
) {
    let (decoder, encoder) = codec.build();
    let reader = FramedRead::new(rx, decoder);
    let writer = FramedWrite::new(tx, encoder);

    // Shutdown flows
    //
    // The Connection is dropped:
    // 1. The Connection is dropped, dropping in_rx and the first out_tx
    // 2. The reader task detects that in_rx has dropped and terminates, the last out_tx instance is dropped
    // 3. The writer task detects that the last out_tx is dropped by out_rx returning None and terminates
    //
    // Client closes connection and then shotover tries to receive:
    // 1.   The reader task detects that the client has closed the connection via reader returning None and terminates,
    // 1.1. in_tx and the first out_tx are dropped
    // 1.2. connect_closed_tx is sent `ConnectionError::OtherSideClosed`
    // 2.   The `Connection::recv/recv_try` detects that in_tx is dropped by in_rx returning None and returns the ConnectionError::OtherSideClosed received from connect_closed_rx.
    // 2.1. `Connection::recv/recv_try` prevents any future sends or receives by storing the ConnectionError
    // 3.   Once the user handles the error by dropping the Connection out_tx is dropped, the writer task detects this by out_rx returning None causing the task to terminate.
    // 3.1. The writer task could also close early by detecting that the client has closed the connection via writer returning BrokenPipe
    //
    // Client closes connection and then shotover tries to send:
    // if a send or recv has not been attempted yet the send will appear to have succeeded.
    // if a recv was already attempted, then the logic is the same as the above example.
    // if a send was already attempted, then the following logic occurs:
    // 1.  Connection::send sends a message to the writer task via out_tx.
    // 2.  The writer task attempts to send the mesage to the writer but it returns a BrokenPipe or ConnectionReset error.
    // 3.1 The writer task task sends an OtherSideClosed error to the Connection.
    // 3.2 The writer task terminates.
    // 4.  Connection::send sends a message to the writer task via out_tx but detects the writer task terminated due due to out_tx returning None.
    // 4.1 Connection::send checks connection_closed_rx for the error, stores it and returns it to the caller.

    let connection_closed_tx2 = connection_closed_tx.clone();
    tokio::spawn(
        async move {
            match reader_task::<C, _>(reader, in_tx, out_tx, force_run_chain).await {
                Ok(()) => {}
                Err(err) => {
                    connection_closed_tx2.try_send(err).ok();
                }
            }
        }
        .in_current_span(),
    );

    tokio::spawn(
        async move {
            match writer_task::<C, _>(writer, out_rx).await {
                Ok(()) => {}
                Err(err) => {
                    connection_closed_tx.try_send(err).ok();
                }
            }
        }
        .in_current_span(),
    );
}

async fn reader_task<C: CodecBuilder + 'static, R: AsyncRead + Unpin + Send + 'static>(
    mut reader: FramedRead<R, <C as CodecBuilder>::Decoder>,
    in_tx: mpsc::Sender<Messages>,
    out_tx: UnboundedSender<Messages>,
    force_run_chain: Option<Arc<Notify>>,
) -> Result<(), ConnectionError> {
    loop {
        tokio::select! {
            biased;
            _ = in_tx.closed() => {
                // shotover is no longer listening for responses, this task is no longer needed
                return Ok(());
            }
            result = reader.next() => {
                if let Some(message) = result {
                    match message {
                        Ok(messages) => {
                            if in_tx.send(messages).await.is_err() {
                                // main task has shutdown, this task is no longer needed
                                return Ok(());
                            }
                            if let Some(force_run_chain) = force_run_chain.as_ref() {
                                force_run_chain.notify_one();
                            }
                        }
                        Err(CodecReadError::RespondAndThenCloseConnection(messages)) => {
                            if let Err(err) = out_tx.send(messages) {
                                error!("Failed to send RespondAndThenCloseConnection message: {:?}", err);
                            }
                            return Err(ConnectionError::ShotoverClosed);
                        }
                        Err(CodecReadError::Parser(err)) => {
                            return Err(ConnectionError::MessageDecode(Arc::new(err)));
                        }
                        Err(CodecReadError::Io(err)) => {
                            return Err(ConnectionError::Io(Arc::new(err)));
                        }
                    }
                } else {
                    return Err(ConnectionError::OtherSideClosed);
                }
            }
        }
    }
}

async fn writer_task<C: CodecBuilder + 'static, W: AsyncWrite + Unpin + Send + 'static>(
    mut writer: FramedWrite<W, <C as CodecBuilder>::Encoder>,
    mut out_rx: UnboundedReceiver<Messages>,
) -> Result<(), ConnectionError> {
    loop {
        if let Some(message) = out_rx.recv().await {
            match writer.send(message).await {
                Err(CodecWriteError::Encoder(err)) => {
                    return Err(ConnectionError::MessageEncode(Arc::new(err)));
                }
                Err(CodecWriteError::Io(err)) => {
                    if matches!(
                        err.kind(),
                        ErrorKind::BrokenPipe | ErrorKind::ConnectionReset
                    ) {
                        return Err(ConnectionError::OtherSideClosed);
                    } else {
                        return Err(ConnectionError::Io(Arc::new(err)));
                    }
                }
                Ok(()) => {}
            }
        } else {
            // shotover is no longer sending responses, this task is no longer needed
            return Ok(());
        }
    }
}
