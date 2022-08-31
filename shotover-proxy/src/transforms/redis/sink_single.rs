use crate::codec::redis::RedisCodec;
use crate::error::ChainResponse;
use crate::frame::Frame;
use crate::frame::RedisFrame;
use crate::message::{Message, Messages};
use crate::tls::{AsyncStream, TlsConnector, TlsConnectorConfig};
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures::stream::{SplitSink, SplitStream};
use futures::{FutureExt, SinkExt, StreamExt};
use metrics::{register_counter, Counter};
use serde::Deserialize;
use std::fmt::Debug;
use std::pin::Pin;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::Instrument;

#[derive(Deserialize, Debug, Clone)]
pub struct RedisSinkSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub tls: Option<TlsConnectorConfig>,
}

impl RedisSinkSingleConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<Transforms> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Transforms::RedisSinkSingle(RedisSinkSingle::new(
            self.address.clone(),
            tls,
            chain_name,
        )))
    }
}

type RedisFramed = Framed<Pin<Box<dyn AsyncStream + Send + Sync>>, RedisCodec>;

struct Connection {
    outbound_tx: SplitSink<RedisFramed, Messages>,
    response_messages_rx: mpsc::UnboundedReceiver<Message>,
    sent_message_type_tx: mpsc::UnboundedSender<MessageType>,
}

pub struct RedisSinkSingle {
    address: String,
    tls: Option<TlsConnector>,
    connection: Option<Connection>,
    chain_name: String,
    failed_requests: Counter,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
}

impl Clone for RedisSinkSingle {
    fn clone(&self) -> Self {
        RedisSinkSingle {
            address: self.address.clone(),
            tls: self.tls.clone(),
            connection: None,
            chain_name: self.chain_name.clone(),
            failed_requests: self.failed_requests.clone(),
            pushed_messages_tx: None,
        }
    }
}

impl RedisSinkSingle {
    pub fn new(address: String, tls: Option<TlsConnector>, chain_name: String) -> RedisSinkSingle {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name.clone(), "transform" => "RedisSinkSingle");

        RedisSinkSingle {
            address,
            tls,
            connection: None,
            chain_name,
            failed_requests,
            pushed_messages_tx: None,
        }
    }
}

#[async_trait]
impl Transform for RedisSinkSingle {
    fn is_terminating(&self) -> bool {
        true
    }

    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        // Return immediately if we have no messages.
        // If we tried to send no messages we would block forever waiting for a reply that will never come.
        if message_wrapper.messages.is_empty() {
            return Ok(message_wrapper.messages);
        }

        if self.connection.is_none() {
            let tcp_stream = timeout(
                Duration::from_secs(3),
                TcpStream::connect(self.address.clone()),
            )
            .await?
            .map_err(|e| anyhow::Error::new(e).context("Failed to connect to upstream"))?;

            let generic_stream = if let Some(tls) = self.tls.as_mut() {
                let tls_stream = tls.connect_unverified_hostname(tcp_stream).await?;
                Box::pin(tls_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>
            } else {
                Box::pin(tcp_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>
            };

            let (outbound_tx, outbound_rx) = Framed::new(generic_stream, RedisCodec::new()).split();
            let (response_messages_tx, response_messages_rx) = mpsc::unbounded_channel();
            let (sent_message_type_tx, sent_message_type_rx) = mpsc::unbounded_channel();

            tokio::spawn(
                server_response_processing_task(
                    outbound_rx,
                    self.pushed_messages_tx.clone(),
                    response_messages_tx,
                    sent_message_type_rx,
                )
                .in_current_span(),
            );
            self.connection = Some(Connection {
                response_messages_rx,
                sent_message_type_tx,
                outbound_tx,
            })
        }

        let connection = self.connection.as_mut().unwrap();

        for message in &mut message_wrapper.messages {
            let ty = if let Some(Frame::Redis(RedisFrame::Array(array))) = message.frame() {
                if let Some(RedisFrame::BulkString(bytes)) = array.first() {
                    match bytes.to_ascii_uppercase().as_slice() {
                        b"SUBSCRIBE" | b"PSUBSCRIBE" | b"SSUBSCRIBE" => MessageType::Subscribe,
                        b"UNSUBSCRIBE" | b"PUNSUBSCRIBE" | b"SUNSUBSCRIBE" => {
                            MessageType::Unsubscribe
                        }
                        b"RESET" => MessageType::Reset,
                        _ => MessageType::Other,
                    }
                } else {
                    MessageType::Other
                }
            } else {
                MessageType::Other
            };
            connection
                .sent_message_type_tx
                .send(ty)
                .map_err(|_| anyhow!("Failed to send message type because RedisSinkSingle response processing task is dead"))?;
        }

        let messages_len = message_wrapper.messages.len();
        connection
            .outbound_tx
            .send(message_wrapper.messages)
            .await
            .context("Failed to send messages to redis destination")?;

        let mut result = Vec::with_capacity(messages_len);
        while result.len() < messages_len {
            let mut message = connection
                .response_messages_rx
                .recv()
                .await
                .ok_or_else(|| anyhow!("Failed to receive message because RedisSinkSingle response processing task is dead"))?;
            if let Some(Frame::Redis(RedisFrame::Error(_))) = message.frame() {
                self.failed_requests.increment(1);
            }
            result.push(message);
        }
        Ok(result)
    }

    fn set_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }
}

/// Processes responses coming in from the server.
/// Responses are then filtered into either the regular chain or pushed messages chain
/// depending on if they are a subscription or response message.
///
/// A separate task is needed to process the incoming messages so that subscription messages can be sent immediately
/// without waiting for an incoming request to trigger the RedisSinkSingle transform again.
///
/// The task will end silently if either the RedisSinkSingle transform is dropped or the server closes the connection.
async fn server_response_processing_task(
    mut outbound_rx: SplitStream<RedisFramed>,
    subscribe_tx: Option<mpsc::UnboundedSender<Messages>>,
    response_messages_tx: mpsc::UnboundedSender<Message>,
    mut sent_message_type: mpsc::UnboundedReceiver<MessageType>,
) {
    let mut is_subscribed = true;
    loop {
        tokio::select! {
            responses = outbound_rx.next().fuse() => {
                if process_server_response(
                    responses,
                    &subscribe_tx,
                    &response_messages_tx,
                    &mut is_subscribed,
                    &mut sent_message_type
                ).await {
                    return;
                }
            },
            _ = response_messages_tx.closed() => {
                tracing::debug!("RedisSinkSingle dropped, redis single subscription task shutting down");
                return;
            },
        }
    }
}

/// returns true when the task should shutdown
async fn process_server_response(
    responses: Option<Result<Messages>>,
    subscribe_tx: &Option<mpsc::UnboundedSender<Messages>>,
    response_messages_tx: &mpsc::UnboundedSender<Message>,
    is_subscribed: &mut bool,
    sent_message_type: &mut mpsc::UnboundedReceiver<MessageType>,
) -> bool {
    match responses {
        Some(Ok(messages)) => {
            for mut message in messages {
                // Notes on subscription responses
                //
                // There are 3 types of pubsub responses and the type is determined by the first value in the array:
                // * `subscribe` - a response to a SUBSCRIBE, PSUBSCRIBE or SSUBSCRIBE request
                // * `unsubscribe` - a response to an UNSUBSCRIBE, PUNSUBSCRIBE or SUNSUBSCRIBE request
                // * `message` - a subscription message
                //
                // Additionally redis will:
                // * accept a few regular commands while in pubsub mode: PING, RESET and QUIT
                // * return an error response when a nonexistent or non pubsub compatible command is used
                //
                // Note: PING has a custom response when in pubsub mode.
                //       It returns an array ['pong', $pingMessage] instead of directly returning $pingMessage.
                //       But this doesnt cause any problems for us.

                // Determine if message is a `message` subscription message
                //
                // Because PING, RESET, QUIT and error responses never return a RedisFrame::Array starting with `message`,
                // they have no way to collide with the `message` value of a subscription message.
                // So while we are in subscription mode we can use that to determine if an
                // incoming message is a subscription message.
                let is_subscription_message = if *is_subscribed {
                    if let Some(Frame::Redis(RedisFrame::Array(array))) = message.frame() {
                        if let [RedisFrame::BulkString(ty), ..] = array.as_slice() {
                            ty.as_ref() == b"message"
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                };

                // Update is_subscribed state
                //
                // In order to make sense of a response we need the main task to
                // send us the type of its corresponding request.
                //
                // In order to keep the incoming request MessageTypes in sync with their corresponding responses
                // we must only process a MessageType when the message is not a subscription message.
                // This is fine because subscription messages cannot affect the is_subscribed state.
                if !is_subscription_message {
                    match sent_message_type.recv().await {
                        Some(MessageType::Subscribe) | Some(MessageType::Unsubscribe) => {
                            if let Some(Frame::Redis(RedisFrame::Array(array))) = message.frame() {
                                if let Some(RedisFrame::Integer(number_of_subscribed_channels)) =
                                    array.get(2)
                                {
                                    *is_subscribed = *number_of_subscribed_channels != 0;
                                }
                            }
                        }
                        Some(MessageType::Other) => {}
                        Some(MessageType::Reset) => {
                            *is_subscribed = false;
                        }
                        None => {
                            tracing::debug!("RedisSinkSingle dropped after a message was received from server, RedisSinkSingle request processor task shutting down");
                            return true;
                        }
                    }
                }

                // Route the message down the correct path:
                // * `message` subscription messages:
                //    needs to be routed down the pushed_messages chain
                // * everything else:
                //    needs to be routed down the regular chain
                if is_subscription_message {
                    // subscribe_tx may not exist if we are e.g. in an alternate chain of a tee transform
                    if let Some(subscribe_tx) = subscribe_tx {
                        if let Err(mpsc::error::SendError(_)) = subscribe_tx.send(vec![message]) {
                            tracing::debug!("shotover chain is terminated, will continue running until Transform is dropped");
                        }
                    }
                } else if let Err(mpsc::error::SendError(_)) = response_messages_tx.send(message) {
                    tracing::debug!("RedisSinkSingle dropped after a message was received from server, RedisSinkSingle request processor task shutting down");
                    return true;
                }
            }
            false
        }
        Some(Err(err)) => {
            tracing::error!("encountered error in redis stream: {err:?}");
            true
        }
        None => {
            tracing::debug!("sink stream ended, redis single subscription task shutting down");
            true
        }
    }
}

#[derive(Debug)]
enum MessageType {
    Other,
    Subscribe,
    Unsubscribe,
    Reset,
}
