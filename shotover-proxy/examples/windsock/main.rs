mod cassandra;
mod common;
mod cassandra_protocols;
mod kafka;
mod profilers;
mod redis;

use crate::cassandra::*;
use crate::common::*;
use crate::kafka::*;
use crate::redis::*;
use cassandra::*;
use common::*;
use cassandra_protocols::*;
use kafka::*;
use std::path::Path;
use tracing_subscriber::EnvFilter;
use windsock::{Bench, Windsock};

fn main() {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(non_blocking)
        .init();

    // The benches and tests automatically set the working directory to CARGO_MANIFEST_DIR.
    // We need to do the same as the DockerCompose + ShotoverProcess types rely on this.
    std::env::set_current_dir(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("shotover-proxy"),
    )
    .unwrap();

    let kafka_benches = itertools::iproduct!(
        [
            Shotover::None,
            Shotover::Standard,
            Shotover::ForcedMessageParsed
        ],
        [Size::B1, Size::KB1, Size::KB100,]
    )
    .map(|(shotover, size)| Box::new(KafkaBench::new(shotover, size)) as Box<dyn Bench>);

    let redis_benches = itertools::iproduct!(
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
        Box::new(RedisBench::new(topology, shotover, operation, encryption)) as Box<dyn Bench>
    });

    Windsock::new(
        vec![
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::None,
                Compression::None,
                Operation::ReadI64,
            )) as Box<dyn Bench>,
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::None,
                Compression::None,
                Operation::WriteBlob,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::None,
                Compression::Lz4,
                Operation::ReadI64,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::Standard,
                Compression::None,
                Operation::ReadI64,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::Standard,
                Compression::Lz4,
                Operation::ReadI64,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::None,
                Compression::None,
                Operation::ReadI64,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::None,
                Compression::Lz4,
                Operation::ReadI64,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::Standard,
                Compression::None,
                Operation::ReadI64,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::Standard,
                Compression::Lz4,
                Operation::ReadI64,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Mocked,
                Topology::Single,
                Shotover::None,
                Compression::None,
                Operation::ReadI64,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Mocked,
                Topology::Single,
                Shotover::Standard,
                Compression::None,
                Operation::ReadI64,
            )),
            Box::new(CassandraProtocolBench::new(
                CassandraProtocol::V3,
                Topology::Single,
                Shotover::Standard,
            )),
            Box::new(CassandraProtocolBench::new(
                CassandraProtocol::V4,
                Topology::Single,
                Shotover::Standard,
            )),
            Box::new(CassandraProtocolBench::new(
                CassandraProtocol::V5,
                Topology::Single,
                Shotover::Standard,
            )),
            Box::new(CassandraProtocolBench::new(
                CassandraProtocol::V3,
                Topology::Cluster3,
                Shotover::Standard,
            )),
            Box::new(CassandraProtocolBench::new(
                CassandraProtocol::V4,
                Topology::Cluster3,
                Shotover::Standard,
            )),
            Box::new(CassandraProtocolBench::new(
                CassandraProtocol::V5,
                Topology::Cluster3,
                Shotover::Standard,
            )),
            Box::new(CassandraProtocolBench::new(
                CassandraProtocol::V3,
                Topology::Single,
                Shotover::None,
            )),
            Box::new(CassandraProtocolBench::new(
                CassandraProtocol::V4,
                Topology::Single,
                Shotover::None,
            )),
            Box::new(CassandraProtocolBench::new(
                CassandraProtocol::V5,
                Topology::Single,
                Shotover::None,
            )),
            Box::new(CassandraProtocolBench::new(
                CassandraProtocol::V3,
                Topology::Cluster3,
                Shotover::None,
            )),
            Box::new(CassandraProtocolBench::new(
                CassandraProtocol::V4,
                Topology::Cluster3,
                Shotover::None,
            )),
            Box::new(CassandraProtocolBench::new(
                CassandraProtocol::V5,
                Topology::Cluster3,
                Shotover::None,
            )),
        ]
        .into_iter()
        .chain(kafka_benches)
        .chain(redis_benches)
        .collect(),
        &["release"],
    )
    .run();
}
