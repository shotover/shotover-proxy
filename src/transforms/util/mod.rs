use shotover_transforms::ChainResponse;
use shotover_transforms::Message;

pub mod cluster_connection_pool;
pub mod unordered_cluster_connection_pool;

#[derive(Debug)]
pub struct Request {
    pub messages: Message,
    pub return_chan: Option<tokio::sync::oneshot::Sender<Response>>,
    pub message_id: Option<u16>,
}

pub type Response = (Message, ChainResponse);
