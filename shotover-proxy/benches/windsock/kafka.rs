use crate::aws::Ec2InstanceWithDocker;
use crate::common::{self, Shotover};
use crate::profilers::{self, CloudProfilerRunner, ProfilerRunner};
use crate::shotover::shotover_process_custom_topology;
use anyhow::Result;
use async_trait::async_trait;
use aws_throwaway::Ec2Instance;
use futures::StreamExt;
use shotover::config::chain::TransformChainConfig;
use shotover::sources::SourceConfig;
use shotover::transforms::debug::force_parse::DebugForceEncodeConfig;
use shotover::transforms::kafka::sink_single::KafkaSinkSingleConfig;
use shotover::transforms::TransformConfig;
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};
use test_helpers::docker_compose::docker_compose;
use test_helpers::rdkafka::config::ClientConfig;
use test_helpers::rdkafka::consumer::{Consumer, StreamConsumer};
use test_helpers::rdkafka::producer::{FutureProducer, FutureRecord};
use test_helpers::rdkafka::util::Timeout;
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

    fn generate_topology_yaml(&self, host_address: String) -> String {
        let mut transforms = vec![];
        if let Shotover::ForcedMessageParsed = self.shotover {
            transforms.push(Box::new(DebugForceEncodeConfig {
                encode_requests: true,
                encode_responses: true,
            }) as Box<dyn TransformConfig>);
        }

        transforms.push(Box::new(KafkaSinkSingleConfig {
            destination_port: 9192,
            connect_timeout_ms: 3000,
            read_timeout: None,
        }));

        common::generate_topology(SourceConfig::Kafka(shotover::sources::kafka::KafkaConfig {
            name: "kafka".to_owned(),
            listen_addr: host_address,
            connection_limit: None,
            hard_connection_limit: None,
            tls: None,
            timeout: None,
            chain: TransformChainConfig(transforms),
        }))
    }

    async fn run_aws_shotover(
        &self,
        instance: Arc<Ec2InstanceWithDocker>,
    ) -> Option<crate::aws::RunningShotover> {
        let ip = instance.instance.private_ip().to_string();
        match self.shotover {
            Shotover::Standard | Shotover::ForcedMessageParsed => {
                let topology = self.generate_topology_yaml(format!("{ip}:9092"));
                Some(instance.run_shotover(&topology).await)
            }
            Shotover::None => None,
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
        profilers::supported_profilers(self.shotover)
    }

    async fn orchestrate_cloud(
        &self,
        _running_in_release: bool,
        profiling: Profiling,
        parameters: BenchParameters,
    ) -> Result<()> {
        let aws = crate::aws::WindsockAws::get().await;

        let (kafka_instance, bench_instance, shotover_instance) = futures::join!(
            aws.create_docker_instance(),
            aws.create_bencher_instance(),
            aws.create_shotover_instance()
        );

        let profiler_instances: HashMap<String, &Ec2Instance> = [
            ("bencher".to_owned(), &bench_instance.instance),
            ("kafka".to_owned(), &kafka_instance.instance),
        ]
        .into();

        // TODO: enable when testing KafkaSinkCluster
        //profiler_instances.insert("shotover".to_owned(), &shotover_instance.instance);

        let mut profiler =
            CloudProfilerRunner::new(self.name(), profiling, profiler_instances).await;

        let kafka_ip = kafka_instance.instance.private_ip().to_string();
        // TODO: make use of this when we start benching KafkaSinkCluster
        let _shotover_ip = shotover_instance.instance.private_ip().to_string();

        let (_, running_shotover) = futures::join!(
            run_aws_kafka(kafka_instance.clone(), 9192),
            self.run_aws_shotover(kafka_instance)
        );

        let destination_address = if running_shotover.is_some() {
            format!("{kafka_ip}:9092")
        } else {
            format!("{kafka_ip}:9192")
        };

        bench_instance
            .run_bencher(
                &self.run_args(&destination_address, &parameters),
                &self.name(),
            )
            .await;

        profiler.finish();

        if let Some(running_shotover) = running_shotover {
            running_shotover.shutdown().await;
        }
        Ok(())
    }

    async fn orchestrate_local(
        &self,
        _running_in_release: bool,
        profiling: Profiling,
        parameters: BenchParameters,
    ) -> Result<()> {
        let config_dir = "tests/test-configs/kafka/bench";
        let _compose = docker_compose(&format!("{}/docker-compose.yaml", config_dir));

        let mut profiler = ProfilerRunner::new(self.name(), profiling);
        let shotover = match self.shotover {
            Shotover::Standard | Shotover::ForcedMessageParsed => {
                let topology_yaml = self.generate_topology_yaml("127.0.0.1:9092".to_owned());
                Some(shotover_process_custom_topology(&topology_yaml, &profiler).await)
            }
            Shotover::None => None,
        };

        let broker_address = match self.shotover {
            Shotover::ForcedMessageParsed | Shotover::Standard => "127.0.0.1:9092",
            Shotover::None => "127.0.0.1:9192",
        };

        profiler.run(&shotover).await;

        self.execute_run(broker_address, &parameters).await;

        if let Some(shotover) = shotover {
            shotover.shutdown_and_then_consume_events(&[]).await;
        }

        Ok(())
    }

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
        producer.produce_one().await.unwrap();

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

async fn run_aws_kafka(instance: Arc<Ec2InstanceWithDocker>, port: i16) {
    let ip = instance.instance.private_ip().to_string();
    instance
        .run_container(
            "bitnami/kafka:3.4.0-debian-11-r22",
            &[
                ("ALLOW_PLAINTEXT_LISTENER".to_owned(), "yes".to_owned()),
                (
                    "KAFKA_CFG_ADVERTISED_LISTENERS".to_owned(),
                    format!("PLAINTEXT://{ip}:{port}"),
                ),
                (
                    "KAFKA_CFG_LISTENERS".to_owned(),
                    format!("PLAINTEXT://:{port},CONTROLLER://:9093"),
                ),
                ("KAFKA_HEAP_OPTS".to_owned(), "-Xmx512M -Xms512M".to_owned()),
            ],
        )
        .await;
}

#[derive(Clone)]
struct BenchTaskProducerKafka {
    message: Vec<u8>,
    producer: FutureProducer,
}

#[async_trait]
impl BenchTaskProducer for BenchTaskProducerKafka {
    async fn produce_one(&self) -> Result<(), String> {
        self.producer
            .send(
                FutureRecord::to("topic_foo")
                    .payload(&self.message)
                    .key("Key"),
                Timeout::Never,
            )
            .await
            // Take just the error, ignoring the message contents because large messages result in unreadable noise in the logs.
            .map_err(|e| format!("{:?}", e.0))
            .map(|_| ())
    }
}

async fn consume(consumer: &StreamConsumer, reporter: UnboundedSender<Report>) {
    let mut stream = consumer.stream();
    loop {
        let report = match stream.next().await.unwrap() {
            Ok(_) => Report::ConsumeCompleted,
            Err(err) => Report::ConsumeErrored {
                message: format!("{err:?}"),
            },
        };
        if reporter.send(report).is_err() {
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
    async fn produce_one(&self) -> Result<(), String>;

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
                        result = task.produce_one() => {
                            let report = match result {
                                Ok(()) => Report::ProduceCompletedIn(operation_start.elapsed()),
                                Err(message) => Report::ProduceErrored {
                                    completed_in: operation_start.elapsed(),
                                    message,
                                },
                            };
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
