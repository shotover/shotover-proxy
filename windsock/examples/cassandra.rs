use anyhow::Result;
use async_trait::async_trait;
use docker_compose_runner::{DockerCompose, Image};
use scylla::SessionBuilder;
use scylla::{transport::Compression, Session};
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc::UnboundedSender;
use windsock::{Bench, BenchParameters, BenchTask, Profiling, Report, Windsock};

fn main() {
    set_working_dir();
    Windsock::new(
        vec![
            Box::new(CassandraBench::new(Some(Compression::Lz4))),
            Box::new(CassandraBench::new(None)),
        ],
        None,
        &["release"],
    )
    .run();
}

struct CassandraBench {
    compression: Option<Compression>,
}

impl CassandraBench {
    fn new(compression: Option<Compression>) -> Self {
        CassandraBench { compression }
    }
}

#[async_trait]
impl Bench for CassandraBench {
    fn tags(&self) -> HashMap<String, String> {
        [
            ("name".to_owned(), "cassandra".to_owned()),
            ("topology".to_owned(), "single".to_owned()),
            ("message_type".to_owned(), "write1000bytes".to_owned()),
            (
                "compression".to_owned(),
                match &self.compression {
                    Some(Compression::Lz4) => "LZ4".to_owned(),
                    Some(Compression::Snappy) => "Snappy".to_owned(),
                    None => "None".to_owned(),
                },
            ),
        ]
        .into_iter()
        .collect()
    }

    async fn orchestrate_cloud(
        &self,
        _running_in_release: bool,
        _profiling: Profiling,
        _bench_parameters: BenchParameters,
    ) -> Result<()> {
        todo!()
    }

    async fn orchestrate_local(
        &self,
        _running_in_release: bool,
        _profiling: Profiling,
        parameters: BenchParameters,
    ) -> Result<()> {
        let _docker_compose = docker_compose("examples/cassandra-docker-compose.yaml");
        let address = "127.0.0.1:9042";

        self.execute_run(address, &parameters).await;

        Ok(())
    }

    async fn run_bencher(
        &self,
        _resources: &str,
        parameters: BenchParameters,
        reporter: UnboundedSender<Report>,
    ) {
        let session = Arc::new(
            SessionBuilder::new()
                .known_nodes(["172.16.1.2:9042"])
                .user("cassandra", "cassandra")
                .compression(self.compression)
                .build()
                .await
                .unwrap(),
        );

        let tasks = BenchTaskCassandra { session }
            .spawn_tasks(reporter.clone(), parameters.operations_per_second)
            .await;

        let start = Instant::now();
        reporter.send(Report::Start).unwrap();

        for _ in 0..parameters.runtime_seconds {
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
struct BenchTaskCassandra {
    session: Arc<Session>,
}

#[async_trait]
impl BenchTask for BenchTaskCassandra {
    async fn run_one_operation(&self) -> Result<(), String> {
        self.session
            .query("SELECT * FROM system.peers", ())
            .await
            .map_err(|err| format!("{err:?}"))
            .map(|_| ())
    }
}

fn docker_compose(file_path: &str) -> DockerCompose {
    DockerCompose::new(get_image_waiters(), |_| {}, file_path)
}

fn get_image_waiters() -> &'static [Image] {
    &[Image {
        name: "bitnami/cassandra:4.0.6",
        log_regex_to_wait_for: r"Startup complete",
    }]
}

fn set_working_dir() {
    // tests and benches will set the directory to the directory of the crate, we are acting as a benchmark so we do the same
    std::env::set_current_dir(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join(env!("CARGO_PKG_NAME")),
    )
    .unwrap();
}
