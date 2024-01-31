mod bench;

use crate::common::*;
use crate::ShotoverBench;
use bench::*;

pub fn benches() -> Vec<ShotoverBench> {
    itertools::iproduct!(
        [RedisTopology::Cluster3, RedisTopology::Single],
        [
            Shotover::None,
            Shotover::Standard,
            Shotover::ForcedMessageParsed
        ],
        [RedisOperation::Get, RedisOperation::Set],
        [Encryption::None, Encryption::Tls]
    )
    .map(|(topology, shotover, operation, encryption)| {
        Box::new(RedisBench::new(topology, shotover, operation, encryption)) as ShotoverBench
    })
    .collect()
}
