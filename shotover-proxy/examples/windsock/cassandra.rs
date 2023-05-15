use async_trait::async_trait;
use scylla::SessionBuilder;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use test_helpers::{
    docker_compose::{docker_compose, DockerCompose},
    flamegraph::Perf,
    mock_cassandra::MockHandle,
    shotover_process::ShotoverProcessBuilder,
};
use tokio::sync::mpsc::UnboundedSender;
use windsock::{Bench, Report};

pub enum CassandraDb {
    Cassandra,
    Mocked,
}

enum CassandraDbInstance {
    Compose(DockerCompose),
    Mocked(MockHandle),
}

pub enum Topology {
    Single,
    Cluster3,
}

pub enum Shotover {
    None,
    Standard,
    #[allow(dead_code)]
    ForcedMessageParsed,
}

pub struct CassandraBench {
    db: CassandraDb,
    topology: Topology,
    shotover: Shotover,
}

impl CassandraBench {
    pub fn new(db: CassandraDb, topology: Topology, shotover: Shotover) -> Self {
        CassandraBench {
            db,
            topology,
            shotover,
        }
    }
}

#[async_trait]
impl Bench for CassandraBench {
    fn tags(&self) -> HashMap<String, String> {
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
            (
                "shotover".to_owned(),
                match self.shotover {
                    Shotover::None => "none".to_owned(),
                    Shotover::Standard => "standard".to_owned(),
                    Shotover::ForcedMessageParsed => "forced-message-parsed".to_owned(),
                },
            ),
            // TODO: run with different OPS values
            ("OPS".to_owned(), "unlimited".to_owned()),
            // TODO: run with different connection count
            ("connections".to_owned(), "128".to_owned()),
            // TODO: run with different message types
            ("message_type".to_owned(), "read_bigint".to_owned()),
        ]
        .into_iter()
        .collect()
    }

    async fn run(&self, flamegraph: bool, _local: bool, reporter: UnboundedSender<Report>) {
        let address = match (&self.topology, &self.shotover) {
            (Topology::Single, Shotover::None) => "127.0.0.1:9043",
            (Topology::Single, Shotover::Standard) => "127.0.0.1:9042",
            (Topology::Cluster3, Shotover::None) => "172.16.1.2:9044",
            (Topology::Cluster3, Shotover::Standard) => "127.0.0.1:9042",
            (_, Shotover::ForcedMessageParsed) => todo!(),
        };
        let config_dir = match &self.topology {
            Topology::Single => "tests/test-configs/cassandra-passthrough",
            Topology::Cluster3 => "tests/test-configs/cassandra-cluster-v4",
        };
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
            let shotover = match self.shotover {
                Shotover::Standard => Some(
                    ShotoverProcessBuilder::new_with_topology(&format!(
                        "{config_dir}/topology.yaml"
                    ))
                    .start()
                    .await,
                ),
                Shotover::None => None,
                Shotover::ForcedMessageParsed => todo!(),
            };
            let perf = if flamegraph {
                if let Some(shotover) = &shotover {
                    Some(Perf::new(shotover.child.as_ref().unwrap().id().unwrap()))
                } else {
                    todo!()
                }
            } else {
                None
            };

            let session = Arc::new(
                SessionBuilder::new()
                    .known_nodes(&[address])
                    .user("cassandra", "cassandra")
                    .build()
                    .await
                    .unwrap(),
            );

            let row_count = 1000usize;

            if let CassandraDb::Cassandra = self.db {
                session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", ()).await.unwrap();
                session.await_schema_agreement().await.unwrap();
                session
                    .query("DROP TABLE IF EXISTS ks.bench", ())
                    .await
                    .unwrap();
                session.await_schema_agreement().await.unwrap();
                session
                    .query("CREATE TABLE ks.bench(id bigint PRIMARY KEY)", ())
                    .await
                    .unwrap();
                session.await_schema_agreement().await.unwrap();
                tokio::time::sleep(Duration::from_secs(1)).await; //TODO: this should not be needed >:[

                for i in 0..row_count {
                    session
                        .query("INSERT INTO ks.bench(id) VALUES (:id)", (i as i64,))
                        .await
                        .unwrap();
                }
            }

            let bench_query = session
                .prepare("SELECT * FROM ks.bench WHERE id = :id")
                .await
                .unwrap();
            let mut tasks = vec![];
            for _ in 0..128 {
                let session = session.clone();
                let reporter = reporter.clone();
                let bench_query = bench_query.clone();
                tasks.push(tokio::spawn(async move {
                    loop {
                        let i = rand::random::<u32>() % row_count as u32;
                        let instant = Instant::now();
                        session.execute(&bench_query, (i as i64,)).await.unwrap();
                        if reporter
                            .send(Report::QueryCompletedIn(instant.elapsed()))
                            .is_err()
                        {
                            // The benchmark has completed and the reporter no longer wants to receive reports so just shutdown
                            return;
                        }
                    }
                }));
            }

            // warm up and then start
            // TODO: properly reevaluate our warmup time once we have graphs showing throughput and latency over time.
            tokio::time::sleep(Duration::from_secs(2)).await;
            reporter.send(Report::Start).unwrap();
            let start = Instant::now();

            tokio::time::sleep(Duration::from_secs(15)).await;
            reporter.send(Report::FinishedIn(start.elapsed())).unwrap();

            // make sure the tasks complete before we drop the database they are connecting to
            for task in tasks {
                task.await.unwrap();
            }
            if let Some(shotover) = shotover {
                shotover.shutdown_and_then_consume_events(&[]).await;
            }

            if let Some(perf) = perf {
                perf.flamegraph();
            }
        }
    }
}
