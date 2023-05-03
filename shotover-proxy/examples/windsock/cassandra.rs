use async_trait::async_trait;
use std::thread::JoinHandle;
use test_helpers::{
    docker_compose::{docker_compose, DockerCompose},
    flamegraph::Perf,
    latte::Latte,
    shotover_process::ShotoverProcessBuilder,
};
use tokio::sync::mpsc::UnboundedSender;
use windsock::{Bench, Report, Tags};

pub enum CassandraDb {
    Cassandra,
    Mocked,
}

enum CassandraDbInstance {
    Compose(DockerCompose),
    Mocked(JoinHandle<()>),
}

pub enum Topology {
    Single,
    Cluster3,
}

pub struct CassandraBench {
    db: CassandraDb,
    topology: Topology,
}

impl CassandraBench {
    pub fn new(db: CassandraDb, topology: Topology) -> Self {
        CassandraBench { db, topology }
    }
}

#[async_trait]
impl Bench for CassandraBench {
    fn tags(&self) -> Tags {
        [
            (
                "name".to_owned(),
                match &self.db {
                    CassandraDb::Cassandra => "cassandra".to_owned(),
                    CassandraDb::Mocked => "cassandra-mocked".to_owned(),
                },
            ),
            (
                "topology".to_owned(),
                match &self.topology {
                    Topology::Single => "single".to_owned(),
                    Topology::Cluster3 => "cluster3".to_owned(),
                },
            ),
            // TODO: split with and without shotover into seperate cases
            // variants: None, Standard, ForcedMessageParsed
            ("shotover".to_owned(), "with_and_without".to_owned()),
            // TODO: run with different OPS values
            ("OPS".to_owned(), "10000000".to_owned()),
            // TODO: run with different connection count
            ("connections".to_owned(), "TODO".to_owned()),
            // TODO: run with different message types
            ("message_type".to_owned(), "write1000bytes".to_owned()),
        ]
        .into_iter()
        .collect()
    }

    async fn run(&self, flamegraph: bool, _local: bool, _reporter: UnboundedSender<Report>) {
        let latte = Latte::new(10000000, 1);
        let cassandra_address = match &self.topology {
            Topology::Single => "127.0.0.1:9043",
            Topology::Cluster3 => "172.16.1.2:9044",
        };
        let config_dir = match &self.topology {
            Topology::Single => "tests/test-configs/cassandra-passthrough",
            Topology::Cluster3 => "tests/test-configs/cassandra-cluster-v4",
        };
        let bench = "read";
        {
            let _db_instance = match (&self.db, &self.topology) {
                (CassandraDb::Cassandra, _) => CassandraDbInstance::Compose(docker_compose(
                    &format!("{config_dir}/docker-compose.yaml"),
                )),
                (CassandraDb::Mocked, Topology::Single) => {
                    CassandraDbInstance::Mocked(test_helpers::mock_cassandra::start(1, 9043))
                }
                (CassandraDb::Mocked, Topology::Cluster3) => {
                    panic!("Mocked cassandra database does not provide a clustered mode")
                }
            };
            let shotover =
                ShotoverProcessBuilder::new_with_topology(&format!("{config_dir}/topology.yaml"))
                    .start()
                    .await;
            let perf = if flamegraph {
                Some(Perf::new(shotover.child.as_ref().unwrap().id().unwrap()))
            } else {
                None
            };

            println!("Benching Shotover ...");
            if !matches!(self.db, CassandraDb::Mocked) {
                latte.init(bench, cassandra_address);
            }
            latte.bench(bench, "127.0.0.1:9042");

            shotover.shutdown_and_then_consume_events(&[]).await;
            if let Some(perf) = perf {
                perf.flamegraph();
            }

            println!("Benching Direct Cassandra ...");
            if !matches!(self.db, CassandraDb::Mocked) {
                latte.init(bench, cassandra_address);
            }
            latte.bench(bench, cassandra_address);
        }

        latte.compare(
            &format!("read-{cassandra_address}.json"),
            "read-127.0.0.1:9042.json",
        );
    }
}
