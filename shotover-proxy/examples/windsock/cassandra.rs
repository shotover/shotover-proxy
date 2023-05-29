use async_trait::async_trait;
use scylla::{
    prepared_statement::PreparedStatement, transport::Compression as ScyllaCompression, Session,
    SessionBuilder,
};
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
use windsock::{Bench, BenchTask, Report};

const ROW_COUNT: usize = 1000;

pub enum Compression {
    None,
    Lz4,
}

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

struct CoreCount {
    /// Cores to be assigned to the bench's tokio runtime
    bench: usize,
    /// Cores per shotover instance
    shotover: usize,
    /// Cores per DB instance
    cassandra: usize,
}

pub struct CassandraBench {
    db: CassandraDb,
    topology: Topology,
    shotover: Shotover,
    compression: Compression,
}

impl CassandraBench {
    pub fn new(
        db: CassandraDb,
        topology: Topology,
        shotover: Shotover,
        compression: Compression,
    ) -> Self {
        CassandraBench {
            db,
            topology,
            shotover,
            compression,
        }
    }

    fn core_count(&self) -> CoreCount {
        CoreCount {
            bench: 1,
            shotover: 1,
            // TODO: actually use this to configure actual cassandra instances, currently only affects cassandra-mocked
            cassandra: 1,
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
            // TODO: run with different message types
            ("message_type".to_owned(), "read_bigint".to_owned()),
            (
                "compression".to_owned(),
                match self.compression {
                    Compression::None => "none".to_owned(),
                    Compression::Lz4 => "lz4".to_owned(),
                },
            ),
        ]
        .into_iter()
        .collect()
    }

    fn cores_required(&self) -> usize {
        self.core_count().bench
    }

    async fn run(
        &self,
        flamegraph: bool,
        _local: bool,
        runtime_seconds: u32,
        operations_per_second: Option<u64>,
        reporter: UnboundedSender<Report>,
    ) {
        let core_count = self.core_count();

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
                (CassandraDb::Mocked, Topology::Single) => CassandraDbInstance::Mocked(
                    test_helpers::mock_cassandra::start(core_count.cassandra, 9043),
                ),
                (CassandraDb::Mocked, Topology::Cluster3) => {
                    panic!("Mocked cassandra database does not provide a clustered mode")
                }
            };
            let shotover = match self.shotover {
                Shotover::Standard => Some(
                    ShotoverProcessBuilder::new_with_topology(&format!(
                        "{config_dir}/topology.yaml"
                    ))
                    .with_cores(core_count.shotover as u32)
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
                    .compression(match self.compression {
                        Compression::None => None,
                        Compression::Lz4 => Some(ScyllaCompression::Lz4),
                    })
                    .build()
                    .await
                    .unwrap(),
            );

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

                for i in 0..ROW_COUNT {
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

            let tasks = BenchTaskCassandra {
                session,
                query: bench_query,
            }
            .spawn_tasks(reporter.clone(), operations_per_second)
            .await;

            // warm up and then start
            tokio::time::sleep(Duration::from_secs(1)).await;
            reporter.send(Report::Start).unwrap();
            let start = Instant::now();

            for _ in 0..runtime_seconds {
                let second = Instant::now();
                tokio::time::sleep(Duration::from_secs(1)).await;
                reporter
                    .send(Report::SecondPassed(second.elapsed()))
                    .unwrap();
            }

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

#[derive(Clone)]
struct BenchTaskCassandra {
    session: Arc<Session>,
    query: PreparedStatement,
}

#[async_trait]
impl BenchTask for BenchTaskCassandra {
    async fn run_one_operation(&self) {
        let i = rand::random::<u32>() % ROW_COUNT as u32;
        self.session
            .execute(&self.query, (i as i64,))
            .await
            .unwrap();
    }
}
