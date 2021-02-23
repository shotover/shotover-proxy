use crate::error::ChainResponse;
use crate::message::Message;
use bytes::Bytes;

pub mod cluster_connection_pool;
mod unordered_cluster_connection_pool;

#[derive(Debug)]
pub struct Request {
    pub messages: Message,
    pub return_chan: Option<tokio::sync::oneshot::Sender<Response>>,
    pub message_id: Option<Bytes>,
}

pub type Response = (Message, ChainResponse);
