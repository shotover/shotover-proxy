mod cassandra;
mod common;
mod kafka;

use cassandra::*;
use common::*;
use kafka::*;
use std::path::Path;
use tracing_subscriber::EnvFilter;
use windsock::Windsock;

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
            Box::new(KafkaBench::new(Shotover::None, Size::B1)),
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
                128,
                Compression::None,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::None,
                128,
                Compression::None,
                Operation::Blob,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::None,
                128,
                Compression::Lz4,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::Standard,
                128,
                Compression::None,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::Standard,
                128,
                Compression::Lz4,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::None,
                128,
                Compression::None,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::None,
                128,
                Compression::Lz4,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::Standard,
                128,
                Compression::None,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::Standard,
                128,
                Compression::Lz4,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Mocked,
                Topology::Single,
                Shotover::None,
                128,
                Compression::None,
                Operation::Read,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Mocked,
                Topology::Single,
                Shotover::Standard,
                128,
                Compression::None,
                Operation::Read,
            )),
        ],
        &["release"],
    )
    .run();
}
