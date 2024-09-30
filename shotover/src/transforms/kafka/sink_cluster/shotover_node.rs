use crate::transforms::kafka::sink_cluster::kafka_node::KafkaAddress;
use atomic_enum::atomic_enum;
use kafka_protocol::messages::BrokerId;
use kafka_protocol::protocol::StrBytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct ShotoverNodeConfig {
    pub address: String,
    pub rack: String,
    pub broker_id: i32,
}

impl ShotoverNodeConfig {
    pub fn build(self) -> anyhow::Result<ShotoverNode> {
        Ok(ShotoverNode {
            address: KafkaAddress::from_str(&self.address)?,
            rack: StrBytes::from_string(self.rack),
            broker_id: BrokerId(self.broker_id),
            state: Arc::new(AtomicShotoverNodeState::new(ShotoverNodeState::Up)),
        })
    }
}

#[derive(Clone)]
pub struct ShotoverNode {
    pub address: KafkaAddress,
    pub rack: StrBytes,
    pub broker_id: BrokerId,
    #[allow(unused)]
    state: Arc<AtomicShotoverNodeState>,
}

#[atomic_enum]
#[derive(PartialEq)]
pub enum ShotoverNodeState {
    Up,
    Down,
}
