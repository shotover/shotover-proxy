use crate::tcp::tcp_stream;
use crate::transforms::kafka::sink_cluster::kafka_node::KafkaAddress;
use atomic_enum::atomic_enum;
use kafka_protocol::messages::BrokerId;
use kafka_protocol::protocol::StrBytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct ShotoverNodeConfig {
    pub address_for_clients: String,
    pub address_for_peers: String,
    pub rack: String,
    pub broker_id: i32,
}

impl ShotoverNodeConfig {
    pub(crate) fn build(self) -> anyhow::Result<ShotoverNode> {
        Ok(ShotoverNode {
            address_for_clients: KafkaAddress::from_str(&self.address_for_clients)?,
            address_for_peers: KafkaAddress::from_str(&self.address_for_peers)?,
            rack: StrBytes::from_string(self.rack),
            broker_id: BrokerId(self.broker_id),
            state: Arc::new(AtomicShotoverNodeState::new(ShotoverNodeState::Up)),
        })
    }
}

#[derive(Clone)]
pub(crate) struct ShotoverNode {
    pub address_for_clients: KafkaAddress,
    pub address_for_peers: KafkaAddress,
    pub rack: StrBytes,
    pub broker_id: BrokerId,
    #[allow(unused)]
    state: Arc<AtomicShotoverNodeState>,
}

impl ShotoverNode {
    #![allow(unused)]
    pub(crate) fn is_up(&self) -> bool {
        self.state.load(Ordering::Relaxed) == ShotoverNodeState::Up
    }

    pub(crate) fn set_state(&self, state: ShotoverNodeState) {
        self.state.store(state, Ordering::Relaxed)
    }
}

#[atomic_enum]
#[derive(PartialEq)]
pub(crate) enum ShotoverNodeState {
    Up,
    Down,
}

pub(crate) fn start_shotover_peers_check(
    shotover_peers: Vec<ShotoverNode>,
    check_shotover_peers_delay_ms: u64,
    connect_timeout: Duration,
) {
    if !shotover_peers.is_empty() {
        tokio::spawn(async move {
            // Wait for all shotover nodes to start
            sleep(Duration::from_secs(10)).await;
            loop {
                match check_shotover_peers(
                    &shotover_peers,
                    check_shotover_peers_delay_ms,
                    connect_timeout,
                )
                .await
                {
                    Ok(_) => {}
                    Err(err) => {
                        tracing::error!(
                            "Restarting the shotover peers check due to error: {err:?}"
                        );
                    }
                };
            }
        });
    }
}

async fn check_shotover_peers(
    shotover_peers: &[ShotoverNode],
    check_shotover_peers_delay_ms: u64,
    connect_timeout: Duration,
) -> Result<(), anyhow::Error> {
    let mut shotover_peers_cycle = shotover_peers.iter().cycle();
    let mut rng = StdRng::from_rng(rand::thread_rng())?;
    let check_shotover_peers_delay_ms = check_shotover_peers_delay_ms as i64;
    loop {
        if let Some(shotover_peer) = shotover_peers_cycle.next() {
            let tcp_stream = tcp_stream(
                connect_timeout,
                (
                    shotover_peer.address_for_peers.host.as_str(),
                    shotover_peer.address_for_peers.port as u16,
                ),
            )
            .await;
            match tcp_stream {
                Ok(_) => {
                    shotover_peer.set_state(ShotoverNodeState::Up);
                }
                Err(_) => {
                    tracing::warn!(
                        "Shotover peer {} is down",
                        shotover_peer.address_for_clients
                    );
                    shotover_peer.set_state(ShotoverNodeState::Down);
                }
            }
            let random_delay = (check_shotover_peers_delay_ms
                + rng.gen_range(
                    -check_shotover_peers_delay_ms / 10..check_shotover_peers_delay_ms / 10,
                )) as u64;
            sleep(Duration::from_millis(random_delay)).await;
        }
    }
}
