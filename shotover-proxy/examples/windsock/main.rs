mod cassandra;
mod kafka;

use cassandra::*;
use kafka::*;
use std::path::Path;
use windsock::Windsock;

fn main() {
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
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Single,
                Shotover::Standard,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::None,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Cassandra,
                Topology::Cluster3,
                Shotover::Standard,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Mocked,
                Topology::Single,
                Shotover::None,
            )),
            Box::new(CassandraBench::new(
                CassandraDb::Mocked,
                Topology::Single,
                Shotover::Standard,
            )),
        ],
        &["release"],
    )
    .run();
}
