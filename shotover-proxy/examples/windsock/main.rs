mod cassandra;
mod kafka;

use cassandra::*;
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
            Box::new(KafkaBench::new()),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::None,
                Compression::None,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::None,
                Compression::Lz4,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::Standard,
                Compression::None,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::Standard,
                Compression::Lz4,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::None,
                Compression::None,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::None,
                Compression::Lz4,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::Standard,
                Compression::None,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::Standard,
                Compression::Lz4,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Mocked,
                Topology::Single,
                Shotover::None,
                Compression::None,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Mocked,
                Topology::Single,
                Shotover::Standard,
                Compression::None,
            )),
        ],
        &["release"],
    )
    .run();
}
