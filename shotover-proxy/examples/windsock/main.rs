mod aws;
mod cassandra;
mod common;
mod kafka;
mod profilers;
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
    if Path::new(env!("CARGO_MANIFEST_DIR")).exists() {
        std::env::set_current_dir(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("shotover-proxy"),
        )
        .unwrap();
    }

    let cassandra_benches = itertools::iproduct!(
        [CassandraDb::Cassandra],
        [Topology::Single, Topology::Cluster3],
        [Shotover::None, Shotover::Standard],
        [Compression::None, Compression::Lz4],
        [Operation::ReadI64, Operation::WriteBlob],
        [
            CassandraProtocol::V3,
            CassandraProtocol::V4,
            CassandraProtocol::V5
        ],
        [CassandraDriver::Scylla, CassandraDriver::CdrsTokio]
    )
    .filter_map(
        |(cassandra, topology, shotover, compression, operation, protocol, driver)| {
            if driver == CassandraDriver::Scylla && protocol != CassandraProtocol::V4 {
                return None;
            }

            if driver == CassandraDriver::CdrsTokio
                && (operation != Operation::ReadI64 || topology != Topology::Single)
            {
                return None;
            }

            Some(Box::new(CassandraBench::new(
                cassandra,
                topology,
                shotover,
                compression,
                operation,
                protocol,
                driver,
            )) as Box<dyn Bench>)
        },
    );

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
                CassandraDb::Mocked,
                Topology::Single,
                Shotover::None,
                Compression::None,
                Operation::ReadI64,
                CassandraProtocol::V4,
                CassandraDriver::Scylla,
            )) as Box<dyn Bench>,
            Box::new(CassandraBench::new(
                CassandraDb::Mocked,
                Topology::Single,
                Shotover::Standard,
                Compression::None,
                Operation::ReadI64,
                CassandraProtocol::V4,
                CassandraDriver::Scylla,
            )),
        ]
        .into_iter()
        .chain(cassandra_benches)
        .chain(kafka_benches)
        .chain(redis_benches)
        .collect(),
        Some(aws::cloud::AwsCloud::new_boxed()),
        &["release"],
    )
    .run();
}
