mod cassandra;
mod common;
mod kafka;
mod redis;

use crate::cassandra::*;
use crate::common::*;
use crate::kafka::*;
use crate::redis::*;
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

    Windsock::new(
        vec![
            Box::new(KafkaBench::new(Shotover::None, Size::B1)) as Box<dyn Bench>,
            Box::new(KafkaBench::new(Shotover::None, Size::KB1)),
            Box::new(KafkaBench::new(Shotover::None, Size::KB100)),
            Box::new(KafkaBench::new(Shotover::Standard, Size::B1)),
            Box::new(KafkaBench::new(Shotover::Standard, Size::KB1)),
            Box::new(KafkaBench::new(Shotover::Standard, Size::KB100)),
            Box::new(KafkaBench::new(Shotover::ForcedMessageParsed, Size::B1)),
            Box::new(KafkaBench::new(Shotover::ForcedMessageParsed, Size::KB1)),
            Box::new(KafkaBench::new(Shotover::ForcedMessageParsed, Size::KB100)),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::None,
                Compression::None,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::None,
                Compression::None,
                Operation::Blob,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::None,
                Compression::Lz4,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::Standard,
                Compression::None,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::Standard,
                Compression::Lz4,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::None,
                Compression::None,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::None,
                Compression::Lz4,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::Standard,
                Compression::None,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::Standard,
                Compression::Lz4,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Mocked,
                Topology::Single,
                Shotover::None,
                Compression::None,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Mocked,
                Topology::Single,
                Shotover::Standard,
                Compression::None,
                Operation::Read,
            )),
        ]
        .into_iter()
        .chain(
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
                Box::new(RedisBench::new(topology, shotover, operation, encryption))
                    as Box<dyn Bench>
            }),
        )
        .collect(),
        &["release"],
    )
    .run();
}
