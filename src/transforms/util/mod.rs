use crate::error::ChainResponse;
use crate::message::Message;

pub mod cluster_connection_pool;

#[derive(Debug)]
pub struct Request {
    pub messages: Message,
    pub return_chan: Option<tokio::sync::oneshot::Sender<Response>>,
}

pub type Response = (Message, ChainResponse);
