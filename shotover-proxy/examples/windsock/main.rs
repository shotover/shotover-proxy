use windsock::Windsock;

mod cassandra;
mod kafka;

use cassandra::*;
use kafka::*;

fn main() {
    test_helpers::bench::init();

    Windsock::new(vec![
        Box::new(KafkaBench::new()),
        Box::new(CassandraBench::new(
            CassandraDb::Cassandra,
            Topology::Single,
        )),
        Box::new(CassandraBench::new(
            CassandraDb::Cassandra,
            Topology::Cluster3,
        )),
        // CassandraDb::Mocked needs to be run last because the mocked db can not yet be shutdown
        // TODO: allow shutting down the mocked db
        Box::new(CassandraBench::new(CassandraDb::Mocked, Topology::Single)),
    ])
    .run();
}
