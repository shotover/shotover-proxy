mod bench;

use crate::common::*;
use crate::ShotoverBench;
use bench::*;

pub fn benches() -> Vec<ShotoverBench> {
    itertools::iproduct!(
        [ValkeyTopology::Cluster3, ValkeyTopology::Single],
        [
            Shotover::None,
            Shotover::Standard,
            Shotover::ForcedMessageParsed
        ],
        [ValkeyOperation::Get, ValkeyOperation::Set],
        [Encryption::None, Encryption::Tls]
    )
    .map(|(topology, shotover, operation, encryption)| {
        Box::new(ValkeyBench::new(topology, shotover, operation, encryption)) as ShotoverBench
    })
    .collect()
}
