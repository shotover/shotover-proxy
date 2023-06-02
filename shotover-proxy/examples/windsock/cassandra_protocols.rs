use super::cassandra::*;
use crate::common::Shotover;
use async_trait::async_trait;
use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::frame::Version;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::TransportTcp;
use std::sync::Arc;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use test_helpers::{
    docker_compose::docker_compose, flamegraph::Perf, shotover_process::ShotoverProcessBuilder,
};
use tokio::sync::mpsc::UnboundedSender;
use windsock::{Bench, BenchTask, Report};

pub enum CassandraProtocol {
    V3,
    V4,
    V5,
}

pub struct CassandraProtocolBench {
    shotover: Shotover,
    protocol: CassandraProtocol,
    topology: Topology,
    // TODO compression
}

impl CassandraProtocolBench {
    pub fn new(protocol: CassandraProtocol, topology: Topology, shotover: Shotover) -> Self {
        CassandraProtocolBench {
            protocol,
            topology,
            shotover,
        }
    }
}

#[async_trait]
impl Bench for CassandraProtocolBench {
    fn tags(&self) -> HashMap<String, String> {
        [
            ("name".to_owned(), "cassandra_protocol".to_owned()),
            (
                "protocol".to_owned(),
                match self.protocol {
                    CassandraProtocol::V3 => "v3".to_owned(),
                    CassandraProtocol::V4 => "v4".to_owned(),
                    CassandraProtocol::V5 => "v5".to_owned(),
                },
            ),
            (
                "topology".to_owned(),
                match self.topology {
                    Topology::Single => "single".to_owned(),
                    Topology::Cluster3 => "cluster3".to_owned(),
                },
            ),
            self.shotover.to_tag(),
        ]
        .iter()
        .cloned()
        .collect()
    }

    async fn run(
        &self,
        flamegraph: bool,
        _local: bool,
        runtime_seconds: u32,
        operations_per_second: Option<u64>,
        reporter: UnboundedSender<Report>,
    ) {
        let address = match (&self.topology, &self.shotover) {
            (Topology::Single, Shotover::None) => "127.0.0.1:9043",
            (Topology::Single, Shotover::Standard) => "127.0.0.1:9042",
            (Topology::Cluster3, Shotover::None) => "172.16.1.2:9044",
            (Topology::Cluster3, Shotover::Standard) => "127.0.0.1:9042",
            (_, _) => todo!(),
        };

        let config_dir = match &self.topology {
            Topology::Single => "tests/test-configs/cassandra-passthrough",
            Topology::Cluster3 => "tests/test-configs/cassandra-cluster-v4",
        };

        {
            let _db_instance = docker_compose(&format!("{config_dir}/docker-compose.yaml"));
            let shotover = match self.shotover {
                Shotover::Standard => Some(
                    ShotoverProcessBuilder::new_with_topology(&format!(
                        "{config_dir}/topology.yaml"
                    ))
                    .start()
                    .await,
                ),
                Shotover::None => None,
                _ => todo!(),
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
            let session = Arc::new(
                TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
                    .build()
                    .await
                    .unwrap(),
            );

            let row_count = 1000usize;

            session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }").await.unwrap();
            session
                .query("DROP TABLE IF EXISTS ks.bench")
                .await
                .unwrap();
            session
                .query("CREATE TABLE ks.bench(id int PRIMARY KEY)")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            for i in 0..row_count {
                session
                    .query(format!("INSERT INTO ks.bench(id) VALUES ({})", i))
                    .await
                    .unwrap();
            }

            tokio::time::sleep(Duration::from_secs(2)).await;

            let bench_task = BenchTaskCassandra { session, row_count };

            reporter.send(Report::Start).unwrap();
            let start = Instant::now();

            let tasks = bench_task
                .spawn_tasks(reporter.clone(), operations_per_second)
                .await;

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
    session: Arc<
        Session<
            TransportTcp,
            TcpConnectionManager,
            RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
        >,
    >,
    row_count: usize,
}

#[async_trait]
impl BenchTask for BenchTaskCassandra {
    async fn run_one_operation(&self) {
        let i = rand::random::<u32>() % self.row_count as u32;
        self.session
            .query(format!("SELECT * FROM ks.bench WHERE id = {}", i))
            .await
            .unwrap();
    }
}
