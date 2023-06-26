use crate::{common::Shotover, profilers::ProfilerRunner};
use async_trait::async_trait;
use cdrs_tokio::{
    cluster::{
        session::{
            Session as CdrsTokioSession, SessionBuilder as CdrsTokioSessionBuilder,
            TcpSessionBuilder,
        },
        NodeTcpConfigBuilder, TcpConnectionManager,
    },
    compression::Compression as CdrsCompression,
    frame::Version,
    load_balancing::RoundRobinLoadBalancingStrategy,
    query::{PreparedQuery as CdrsPrepared, QueryParamsBuilder, QueryValues},
    statement::StatementParams,
    transport::TransportTcp,
};
use rand::prelude::*;
use scylla::{
    prepared_statement::PreparedStatement as ScyllaPrepared,
    transport::Compression as ScyllaCompression, Session as ScyllaSession,
    SessionBuilder as ScyllaSessionBuilder,
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

#[derive(Clone)]
pub enum Compression {
    None,
    Lz4,
}

#[derive(Clone)]
pub enum CassandraDb {
    Cassandra,
    Mocked,
}

enum CassandraDbInstance {
    Compose(DockerCompose),
    Mocked(MockHandle),
}

#[derive(Clone, PartialEq)]
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

#[derive(Clone, PartialEq)]
pub enum Operation {
    ReadI64,
    WriteBlob,
}

impl Operation {
    async fn prepare(&self, session: &Arc<CassandraSession>, db: &CassandraDb) {
        if let CassandraDb::Cassandra = db {
            session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }").await;
            session.await_schema_agreement().await;
            session.query("DROP TABLE IF EXISTS ks.bench").await;
            session.await_schema_agreement().await;

            match self {
                Operation::ReadI64 => {
                    session
                        .query("CREATE TABLE ks.bench(id bigint PRIMARY KEY)")
                        .await;
                    session.await_schema_agreement().await;
                    tokio::time::sleep(Duration::from_secs(1)).await; //TODO: this should not be needed >:[

                    for i in 0..ROW_COUNT {
                        session
                            .query(&format!("INSERT INTO ks.bench(id) VALUES ({})", i))
                            .await;
                    }
                }
                Operation::WriteBlob => {
                    session
                        .query("CREATE TABLE ks.bench(id bigint PRIMARY KEY, data BLOB)")
                        .await;
                    session.await_schema_agreement().await;
                    tokio::time::sleep(Duration::from_secs(1)).await; //TODO: this should not be needed >:[
                }
            }
        }
    }

    async fn run(
        &self,
        session: &Arc<CassandraSession>,
        reporter: UnboundedSender<Report>,
        operations_per_second: Option<u64>,
        runtime_seconds: u32,
    ) {
        let bench_query = match self {
            Operation::ReadI64 => {
                session
                    .prepare("SELECT * FROM ks.bench WHERE id = :id")
                    .await
            }
            Operation::WriteBlob => {
                session
                    .prepare("INSERT INTO ks.bench (id, data) VALUES (:id, :data)")
                    .await
            }
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

#[derive(Clone)]
pub enum PreparedStatement {
    Scylla(ScyllaPrepared),
    Cdrs(CdrsPrepared),
}

impl PreparedStatement {
    fn as_scylla(&self) -> &ScyllaPrepared {
        match self {
            Self::Scylla(p) => p,
            _ => panic!("Not Scylla"),
        }
    }

    fn as_cdrs(&self) -> &CdrsPrepared {
        match self {
            Self::Cdrs(p) => p,
            _ => panic!("Not Cdrs"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum CassandraProtocol {
    V3,
    V4,
    V5,
}

#[derive(Copy, Clone, PartialEq)]
pub enum CassandraDriver {
    Scylla,
    CdrsTokio,
}

pub enum CassandraSession {
    Scylla(ScyllaSession),
    CdrsTokio(
        CdrsTokioSession<
            TransportTcp,
            TcpConnectionManager,
            RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
        >,
    ),
}

impl CassandraSession {
    async fn query(&self, query: &str) {
        match self {
            Self::Scylla(session) => {
                session.query(query, ()).await.unwrap();
            }
            Self::CdrsTokio(session) => {
                session.query(query).await.unwrap();
            }
        }
    }

    async fn prepare(&self, query: &str) -> PreparedStatement {
        match self {
            Self::Scylla(session) => {
                PreparedStatement::Scylla(session.prepare(query).await.unwrap())
            }
            Self::CdrsTokio(session) => {
                PreparedStatement::Cdrs(session.prepare(query).await.unwrap())
            }
        }
    }

    async fn execute(&self, prepared_statement: &PreparedStatement, value: i64) {
        match self {
            Self::Scylla(session) => {
                session
                    .execute(prepared_statement.as_scylla(), (value,))
                    .await
                    .unwrap();
            }
            Self::CdrsTokio(session) => {
                let query_params = QueryParamsBuilder::new()
                    .with_values(QueryValues::SimpleValues(vec![value.into()]))
                    .build();

                let params = StatementParams {
                    query_params,
                    is_idempotent: false,
                    keyspace: None,
                    token: None,
                    routing_key: None,
                    tracing: true,
                    warnings: false,
                    speculative_execution_policy: None,
                    retry_policy: None,
                    beta_protocol: false,
                };

                session
                    .exec_with_params(prepared_statement.as_cdrs(), &params)
                    .await
                    .unwrap();
            }
        }
    }

    async fn execute_blob(
        &self,
        prepared_statement: &PreparedStatement,
        value: i64,
        blob: [u8; 16],
    ) {
        match self {
            Self::Scylla(session) => {
                session
                    .execute(prepared_statement.as_scylla(), (value, blob))
                    .await
                    .unwrap();
            }
            Self::CdrsTokio(session) => {
                let query_params = QueryParamsBuilder::new()
                    .with_values(QueryValues::SimpleValues(vec![
                        value.into(),
                        blob.to_vec().into(),
                    ]))
                    .build();

                let params = StatementParams {
                    query_params,
                    is_idempotent: false,
                    keyspace: None,
                    token: None,
                    routing_key: None,
                    tracing: true,
                    warnings: false,
                    speculative_execution_policy: None,
                    retry_policy: None,
                    beta_protocol: false,
                };

                session
                    .exec_with_params(prepared_statement.as_cdrs(), &params)
                    .await
                    .unwrap();
            }
        }
    }

    async fn await_schema_agreement(&self) {
        match self {
            Self::Scylla(session) => {
                session.await_schema_agreement().await.unwrap();
            }
            Self::CdrsTokio(_session) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

pub struct CassandraBench {
    db: CassandraDb,
    topology: Topology,
    shotover: Shotover,
    compression: Compression,
    operation: Operation,
    protocol: CassandraProtocol,
    driver: CassandraDriver,
}

impl CassandraBench {
    pub fn new(
        db: CassandraDb,
        topology: Topology,
        shotover: Shotover,
        compression: Compression,
        operation: Operation,
        protocol: CassandraProtocol,
        driver: CassandraDriver,
    ) -> Self {
        CassandraBench {
            db,
            topology,
            shotover,
            compression,
            operation,
            protocol,
            driver,
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
            (
                "protocol".to_owned(),
                match self.protocol {
                    CassandraProtocol::V3 => "v3".to_owned(),
                    CassandraProtocol::V4 => "v4".to_owned(),
                    CassandraProtocol::V5 => "v5".to_owned(),
                },
            ),
            (
                "driver".to_owned(),
                match self.driver {
                    CassandraDriver::Scylla => "scylla".to_owned(),
                    CassandraDriver::CdrsTokio => "cdrs-tokio".to_owned(),
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

            let session = match (self.protocol, self.driver) {
                (CassandraProtocol::V4, CassandraDriver::Scylla) => {
                    Arc::new(CassandraSession::Scylla(
                        ScyllaSessionBuilder::new()
                            .known_nodes([address])
                            .user("cassandra", "cassandra")
                            .compression(match self.compression {
                                Compression::None => None,
                                Compression::Lz4 => Some(ScyllaCompression::Lz4),
                            })
                            .build()
                            .await
                            .unwrap(),
                    ))
                }
                (_, CassandraDriver::Scylla) => {
                    panic!("Must use CassandraProtocol::V4 with CassandraDriver:Scylla");
                }
                (_, CassandraDriver::CdrsTokio) => {
                    let cluster_config = NodeTcpConfigBuilder::new()
                        .with_contact_point(address.into())
                        .with_version(match self.protocol {
                            CassandraProtocol::V3 => Version::V3,
                            CassandraProtocol::V4 => Version::V4,
                            CassandraProtocol::V5 => Version::V5,
                        })
                        .build()
                        .await
                        .unwrap();
                    Arc::new(CassandraSession::CdrsTokio(
                        TcpSessionBuilder::new(
                            RoundRobinLoadBalancingStrategy::new(),
                            cluster_config,
                        )
                        .with_compression(match self.compression {
                            Compression::None => CdrsCompression::None,
                            Compression::Lz4 => CdrsCompression::Lz4,
                        })
                        .build()
                        .await
                        .unwrap(),
                    ))
                }
            };

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
    session: Arc<CassandraSession>,
    query: PreparedStatement,
    operation: Operation,
}

#[async_trait]
impl BenchTask for BenchTaskCassandra {
    async fn run_one_operation(&self) {
        let i = rand::random::<u32>() % ROW_COUNT as u32;
        match self.operation {
            Operation::ReadI64 => {
                self.session.execute(&self.query, i as i64).await;
            }
            Operation::WriteBlob => {
                let blob = {
                    let mut rng = rand::thread_rng();
                    rng.gen::<[u8; 16]>()
                };

                self.session.execute_blob(&self.query, i as i64, blob).await;
            }
        }
    }
}
