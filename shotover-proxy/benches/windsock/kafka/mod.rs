mod bench;

use crate::ShotoverBench;
use crate::common::*;
use bench::*;

pub fn benches() -> Vec<ShotoverBench> {
    itertools::iproduct!(
        [
            Shotover::None,
            Shotover::Standard,
            Shotover::ForcedMessageParsed
        ],
        [
            KafkaTopology::Single,
            KafkaTopology::Cluster1,
            KafkaTopology::Cluster3
        ],
        [Size::B12, Size::KB1, Size::KB100]
    )
    .map(|(shotover, topology, size)| {
        Box::new(KafkaBench::new(shotover, topology, size)) as ShotoverBench
    })
    .collect()
}
