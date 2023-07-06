use crate::common::Shotover;
use crate::profilers::ProfilerRunner;
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};
use test_helpers::{docker_compose::docker_compose, shotover_process::ShotoverProcessBuilder};
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle, time::Instant};
use windsock::{Bench, BenchParameters, Profiling, Report};

pub struct KafkaBench {
    shotover: Shotover,
    message_size: Size,
}

#[derive(Clone)]
pub enum Size {
    B1,
    KB1,
    KB100,
}

impl KafkaBench {
    pub fn new(shotover: Shotover, message_size: Size) -> Self {
        KafkaBench {
            shotover,
            message_size,
        }
    }
}

#[async_trait]
impl Bench for KafkaBench {
    fn cores_required(&self) -> usize {
        2
    }

    fn tags(&self) -> HashMap<String, String> {
        [
            ("name".to_owned(), "kafka".to_owned()),
            ("topology".to_owned(), "single".to_owned()),
            self.shotover.to_tag(),
            match self.message_size {
                Size::B1 => ("size".to_owned(), "1B".to_owned()),
                Size::KB1 => ("size".to_owned(), "1KB".to_owned()),
                Size::KB100 => ("size".to_owned(), "100KB".to_owned()),
            },
        ]
        .into_iter()
        .collect()
    }

    fn supported_profilers(&self) -> Vec<String> {
        ProfilerRunner::supported_profilers(self.shotover)
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
        profiling: Profiling,
        parameters: BenchParameters,
    ) -> Result<()> {
        let config_dir = "tests/test-configs/kafka/bench";
        let _compose = docker_compose(&format!("{}/docker-compose.yaml", config_dir));

        let mut profiler = ProfilerRunner::new(profiling);
        let shotover = match self.shotover {
            Shotover::Standard => Some(
                ShotoverProcessBuilder::new_with_topology(&format!("{config_dir}/topology.yaml"))
                    .with_profile(profiler.shotover_profile())
                    .start()
                    .await,
            ),
            Shotover::None => None,
            Shotover::ForcedMessageParsed => Some(
                ShotoverProcessBuilder::new_with_topology(&format!(
                    "{config_dir}/topology-encode.yaml"
                ))
                .with_profile(profiler.shotover_profile())
                .start()
                .await,
            ),
        };

        let broker_address = match self.shotover {
            Shotover::ForcedMessageParsed | Shotover::Standard => "127.0.0.1:9192",
            Shotover::None => "127.0.0.1:9092",
        };

        profiler.run(&shotover);

        self.execute_run(broker_address, &parameters).await;

        if let Some(shotover) = shotover {
            shotover.shutdown_and_then_consume_events(&[]).await;
        }

        Ok(())
    }

    /// Perform benchmarking utilizing the resources arg to learn ip addresses of cloud resources that have been already setup.
    async fn run_bencher(
        &self,
        resources: &str,
        parameters: BenchParameters,
        reporter: UnboundedSender<Report>,
    ) {
        // only one string field so we just directly store the value in resources
        let broker_address = resources;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", broker_address)
            .set("message.timeout.ms", "5000")
            .create()
            .unwrap();

        let message = match &self.message_size {
            Size::B1 => vec![0; 1],
            Size::KB1 => vec![0; 1024],
            Size::KB100 => vec![0; 1024 * 100],
        };

        let producer = BenchTaskProducerKafka { producer, message };

        // ensure topic exists
        producer.produce_one().await;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", broker_address)
            .set("group.id", "some_group")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .create()
            .unwrap();

        // Need to wait ~5s for this subscribe to complete
        consumer.subscribe(&["topic_foo"]).unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Start consumers first to avoid initial backlog
        let mut tasks = spawn_consumer_tasks(consumer, reporter.clone());
        reporter.send(Report::Start).unwrap();
        tasks.extend(
            producer
                .spawn_tasks(reporter.clone(), parameters.operations_per_second)
                .await,
        );

        let start = Instant::now();

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
struct BenchTaskProducerKafka {
    message: Vec<u8>,
    producer: FutureProducer,
}

#[async_trait]
impl BenchTaskProducer for BenchTaskProducerKafka {
    async fn produce_one(&self) {
        self.producer
            .send(
                FutureRecord::to("topic_foo")
                    .payload(&self.message)
                    .key("Key"),
                Timeout::Never,
            )
            .await
            // Take just the error, ignoring the message contents because large messages result in unreadable noise in the logs.
            .map_err(|e| e.0)
            .unwrap();
    }
}

async fn consume(consumer: &StreamConsumer, reporter: UnboundedSender<Report>) {
    let mut stream = consumer.stream();
    loop {
        stream.next().await.unwrap().unwrap();
        if reporter.send(Report::ConsumeCompleted).is_err() {
            // Errors indicate the reporter has closed so we should end the bench
            return;
        }
    }
}

fn spawn_consumer_tasks(
    consumer: StreamConsumer,
    reporter: UnboundedSender<Report>,
) -> Vec<JoinHandle<()>> {
    let consumer = Arc::new(consumer);
    (0..1000)
        .map(|_| {
            let reporter = reporter.clone();
            let consumer = consumer.clone();
            tokio::spawn(async move {
                tokio::select!(
                    _ = consume(&consumer, reporter.clone()) => {}
                    _ = reporter.closed() => {}
                );
            })
        })
        .collect()
}

#[async_trait]
pub trait BenchTaskProducer: Clone + Send + Sync + 'static {
    async fn produce_one(&self);

    async fn spawn_tasks(
        self,
        reporter: UnboundedSender<Report>,
        operations_per_second: Option<u64>,
    ) -> Vec<JoinHandle<()>> {
        let mut tasks = vec![];
        // 10000 is a generally nice amount of tasks to have, but if we have more tasks than OPS the throughput is very unstable
        let task_count = operations_per_second.map(|x| x.min(10000)).unwrap_or(10000);

        let allocated_time_per_op = operations_per_second
            .map(|ops| (Duration::from_secs(1) * task_count as u32) / ops as u32);
        for _ in 0..task_count {
            let task = self.clone();
            let reporter = reporter.clone();
            tasks.push(tokio::spawn(async move {
                let mut interval = allocated_time_per_op.map(tokio::time::interval);

                loop {
                    if let Some(interval) = &mut interval {
                        interval.tick().await;
                    }

                    let operation_start = Instant::now();
                    tokio::select!(
                        _ = task.produce_one() => {
                            let report = Report::ProduceCompletedIn(operation_start.elapsed());
                            if reporter.send(report).is_err() {
                                // Errors indicate the reporter has closed so we should end the bench
                                return;
                            }
                        }
                        _ = reporter.closed() => {
                            return
                        }
                    );
                }
            }));
        }

        tasks
    }
}
