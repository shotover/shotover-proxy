use crate::ChainResponse;
use crate::Message;
use serde::{Deserialize, Serialize};

pub mod cluster_connection_pool;
pub mod unordered_cluster_connection_pool;

#[derive(Debug)]
pub struct Request {
    pub messages: Message,
    pub return_chan: Option<tokio::sync::oneshot::Sender<Response>>,
    pub message_id: Option<u16>,
}

pub type Response = (Message, ChainResponse);

#[allow(non_camel_case_types)]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum CoalesceBehavior {
    COUNT(usize),
    WAIT_MS(u128),
    COUNT_OR_WAIT(usize, u128),
}

impl Default for CoalesceBehavior {
    fn default() -> Self {
        return CoalesceBehavior::COUNT(1);
    }
}
