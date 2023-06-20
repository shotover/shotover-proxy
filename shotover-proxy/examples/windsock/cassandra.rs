use crate::{common::Shotover, profilers::ProfilerRunner};
use async_trait::async_trait;
use rand::prelude::*;
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
    mock_cassandra::MockHandle,
    shotover_process::ShotoverProcessBuilder,
};
use tokio::sync::mpsc::UnboundedSender;
use windsock::{Bench, BenchTask, Profiling, Report};

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

struct CoreCount {
    /// Cores to be assigned to the bench's tokio runtime
    bench: usize,
    /// Cores per shotover instance
    shotover: usize,
    /// Cores per DB instance
    cassandra: usize,
}

#[derive(Clone)]
pub enum Operation {
    ReadI64,
    WriteBlob,
}

impl Operation {
    async fn prepare(&self, session: &Arc<Session>, db: &CassandraDb) {
        if let CassandraDb::Cassandra = db {
            session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", ()).await.unwrap();
            session.await_schema_agreement().await.unwrap();
            session
                .query("DROP TABLE IF EXISTS ks.bench", ())
                .await
                .unwrap();
            session.await_schema_agreement().await.unwrap();

            match self {
                Operation::ReadI64 => {
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
                Operation::WriteBlob => {
                    session
                        .query(
                            "CREATE TABLE ks.bench(id bigint PRIMARY KEY, data BLOB)",
                            (),
                        )
                        .await
                        .unwrap();
                    session.await_schema_agreement().await.unwrap();
                    tokio::time::sleep(Duration::from_secs(1)).await; //TODO: this should not be needed >:[
                }
            }
        }
    }

    async fn run(
        &self,
        session: &Arc<Session>,
        reporter: UnboundedSender<Report>,
        operations_per_second: Option<u64>,
        runtime_seconds: u32,
    ) {
        let bench_query = match self {
            Operation::ReadI64 => session
                .prepare("SELECT * FROM ks.bench WHERE id = :id")
                .await
                .unwrap(),
            Operation::WriteBlob => session
                .prepare("INSERT INTO ks.bench (id, data) VALUES (:id, :data)")
                .await
                .unwrap(),
        };

        let tasks = BenchTaskCassandra {
            session: session.clone(),
            query: bench_query,
            operation: self.clone(),
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
    }
}

pub struct CassandraBench {
    db: CassandraDb,
    topology: Topology,
    shotover: Shotover,
    compression: Compression,
    operation: Operation,
}

impl CassandraBench {
    pub fn new(
        db: CassandraDb,
        topology: Topology,
        shotover: Shotover,
        compression: Compression,
        operation: Operation,
    ) -> Self {
        CassandraBench {
            db,
            topology,
            shotover,
            compression,
            operation,
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
            self.shotover.to_tag(),
            (
                "operation".to_owned(),
                match self.operation {
                    Operation::ReadI64 => "read_i64".to_owned(),
                    Operation::WriteBlob => "write_blob".to_owned(),
                },
            ),
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

    fn supported_profilers(&self) -> Vec<String> {
        ProfilerRunner::supported_profilers(self.shotover)
    }

    fn cores_required(&self) -> usize {
        self.core_count().bench
    }

    async fn run(
        &self,
        profiling: Profiling,
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
            let mut profiler = ProfilerRunner::new(profiling);
            let shotover = match self.shotover {
                Shotover::Standard => Some(
                    ShotoverProcessBuilder::new_with_topology(&format!(
                        "{config_dir}/topology.yaml"
                    ))
                    .with_cores(core_count.shotover as u32)
                    .with_profile(profiler.shotover_profile())
                    .start()
                    .await,
                ),
                Shotover::None => None,
                Shotover::ForcedMessageParsed => todo!(),
            };
            profiler.run(&shotover);

            let session = Arc::new(
                SessionBuilder::new()
                    .known_nodes([address])
                    .user("cassandra", "cassandra")
                    .compression(match self.compression {
                        Compression::None => None,
                        Compression::Lz4 => Some(ScyllaCompression::Lz4),
                    })
                    .build()
                    .await
                    .unwrap(),
            );

            self.operation.prepare(&session, &self.db).await;

            self.operation
                .run(&session, reporter, operations_per_second, runtime_seconds)
                .await;

            if let Some(shotover) = shotover {
                shotover.shutdown_and_then_consume_events(&[]).await;
            }
        }
    }
}

#[derive(Clone)]
struct BenchTaskCassandra {
    session: Arc<Session>,
    query: PreparedStatement,
    operation: Operation,
}

#[async_trait]
impl BenchTask for BenchTaskCassandra {
    async fn run_one_operation(&self) {
        let i = rand::random::<u32>() % ROW_COUNT as u32;
        match self.operation {
            Operation::ReadI64 => {
                self.session
                    .execute(&self.query, (i as i64,))
                    .await
                    .unwrap();
            }
            Operation::WriteBlob => {
                let blob = {
                    let mut rng = rand::thread_rng();
                    rng.gen::<[u8; 16]>()
                };

                self.session
                    .execute(&self.query, (i as i64, blob))
                    .await
                    .unwrap();
            }
        }
    }
}
