use crate::cloud::{
    CloudResources, CloudResourcesRequired, Ec2InstanceWithDocker, Ec2InstanceWithShotover,
    RunningShotover,
};
use crate::common::{self, Shotover};
use crate::profilers::{self, CloudProfilerRunner, ProfilerRunner};
use crate::shotover::shotover_process_custom_topology;
use anyhow::Result;
use async_trait::async_trait;
use aws_throwaway::Ec2Instance;
use futures::StreamExt;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use shotover::config::chain::TransformChainConfig;
use shotover::sources::SourceConfig;
use shotover::transforms::debug::force_parse::DebugForceEncodeConfig;
use shotover::transforms::kafka::sink_cluster::{KafkaSinkClusterConfig, ShotoverNodeConfig};
use shotover::transforms::kafka::sink_single::KafkaSinkSingleConfig;
use shotover::transforms::TransformConfig;
use std::sync::Arc;
use std::time::SystemTime;
use std::{collections::HashMap, time::Duration};
use test_helpers::connection::kafka::cpp::rdkafka::admin::{
    AdminClient, AdminOptions, NewTopic, TopicReplication,
};
use test_helpers::connection::kafka::cpp::rdkafka::client::DefaultClientContext;
use test_helpers::connection::kafka::cpp::rdkafka::config::ClientConfig;
use test_helpers::connection::kafka::cpp::rdkafka::consumer::{Consumer, StreamConsumer};
use test_helpers::connection::kafka::cpp::rdkafka::producer::{FutureProducer, FutureRecord};
use test_helpers::connection::kafka::cpp::rdkafka::util::Timeout;
use test_helpers::connection::kafka::cpp::rdkafka::Message;
use test_helpers::docker_compose::docker_compose;
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle, time::Instant};
use windsock::{Bench, BenchParameters, Profiling, Report};

pub struct KafkaBench {
    shotover: Shotover,
    topology: KafkaTopology,
    message_size: Size,
}

#[derive(Clone)]
pub enum Size {
    // The smallest possible size is 12 bytes since any smaller would be
    // unable to hold the timestamp used for measuring consumer latency
    B12,
    KB1,
    KB100,
}

#[derive(Clone, Copy, PartialEq)]
pub enum KafkaTopology {
    Single,
    Cluster1,
    Cluster3,
}

impl KafkaTopology {
    pub fn to_tag(self) -> (String, String) {
        (
            "topology".to_owned(),
            match self {
                KafkaTopology::Single => "single".to_owned(),
                KafkaTopology::Cluster1 => "cluster1".to_owned(),
                KafkaTopology::Cluster3 => "cluster3".to_owned(),
            },
        )
    }
}

impl KafkaBench {
    pub fn new(shotover: Shotover, topology: KafkaTopology, message_size: Size) -> Self {
        KafkaBench {
            shotover,
            topology,
            message_size,
        }
    }

    fn generate_topology_yaml(&self, host_address: String, kafka_address: String) -> String {
        let mut transforms = vec![];
        if let Shotover::ForcedMessageParsed = self.shotover {
            transforms.push(Box::new(DebugForceEncodeConfig {
                encode_requests: true,
                encode_responses: true,
            }) as Box<dyn TransformConfig>);
        }

        transforms.push(match self.topology {
            KafkaTopology::Single => Box::new(KafkaSinkSingleConfig {
                destination_port: 9192,
                connect_timeout_ms: 3000,
                read_timeout: None,
                tls: None,
            }),
            KafkaTopology::Cluster1 | KafkaTopology::Cluster3 => Box::new(KafkaSinkClusterConfig {
                connect_timeout_ms: 3000,
                read_timeout: None,
                first_contact_points: vec![kafka_address],
                shotover_nodes: vec![ShotoverNodeConfig {
                    address: host_address.parse().unwrap(),
                    rack: "rack1".into(),
                    broker_id: 0,
                }],
                local_shotover_broker_id: 0,
                authorize_scram_over_mtls: None,
                tls: None,
            }),
        });
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

    async fn run_aws_kafka(&self, nodes: Vec<Arc<Ec2InstanceWithDocker>>) {
        match self.topology {
            KafkaTopology::Cluster3 => self.run_aws_kafka_cluster(nodes).await,
            KafkaTopology::Cluster1 | KafkaTopology::Single => {
                self.run_aws_kafka_cluster(vec![nodes[0].clone()]).await
            }
        }
    }

    async fn run_aws_kafka_cluster(&self, nodes: Vec<Arc<Ec2InstanceWithDocker>>) {
        let voters = nodes
            .iter()
            .enumerate()
            .map(|(i, node)| format!("{i}@{}:9093", node.instance.private_ip()))
            .join(",");
        let mut tasks = vec![];
        for (i, node) in nodes.into_iter().enumerate() {
            let ip = node.instance.private_ip().to_string();
            let port = 9192;
            let voters = voters.clone();

            tasks.push(tokio::spawn(async move {
                node.run_container(
                    "bitnami/kafka:3.6.1-debian-11-r24",
                    &[
                        ("ALLOW_PLAINTEXT_LISTENER".to_owned(), "yes".to_owned()),
                        (
                            "KAFKA_CFG_ADVERTISED_LISTENERS".to_owned(),
                            format!("BROKER://{ip}:{port}"),
                        ),
                        (
                            "KAFKA_CFG_LISTENERS".to_owned(),
                            format!("BROKER://:{port},CONTROLLER://:9093"),
                        ),
                        (
                            "KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP".to_owned(),
                            "CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT".to_owned(),
                        ),
                        (
                            "KAFKA_CFG_INTER_BROKER_LISTENER_NAME".to_owned(),
                            "BROKER".to_owned(),
                        ),
                        (
                            "KAFKA_CFG_CONTROLLER_LISTENER_NAMES".to_owned(),
                            "CONTROLLER".to_owned(),
                        ),
                        (
                            "KAFKA_CFG_PROCESS_ROLES".to_owned(),
                            "controller,broker".to_owned(),
                        ),
                        (
                            "KAFKA_HEAP_OPTS".to_owned(),
                            "-Xmx4096M -Xms4096M".to_owned(),
                        ),
                        ("KAFKA_CFG_NODE_ID".to_owned(), i.to_string()),
                        (
                            "KAFKA_KRAFT_CLUSTER_ID".to_owned(),
                            "abcdefghijklmnopqrstuv".to_owned(),
                        ),
                        ("KAFKA_CFG_CONTROLLER_QUORUM_VOTERS".to_owned(), voters),
                    ],
                )
                .await;
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    }

    async fn run_aws_shotover_on_own_instance(
        &self,
        shotover_instance: Option<Arc<Ec2InstanceWithShotover>>,
        kafka_instance: Arc<Ec2InstanceWithDocker>,
    ) -> Option<RunningShotover> {
        match self.shotover {
            Shotover::Standard | Shotover::ForcedMessageParsed => {
                let shotover_instance = shotover_instance.unwrap();
                let shotover_ip = shotover_instance.instance.private_ip().to_string();
                let kafka_ip = kafka_instance.instance.private_ip().to_string();
                let topology = self.generate_topology_yaml(
                    format!("{shotover_ip}:9092"),
                    format!("{kafka_ip}:9192"),
                );
                Some(shotover_instance.run_shotover(&topology).await)
            }
            Shotover::None => None,
        }
    }

    async fn run_aws_shotover_colocated_with_kafka(
        &self,
        instance: Arc<Ec2InstanceWithDocker>,
    ) -> Option<RunningShotover> {
        let ip = instance.instance.private_ip().to_string();
        match self.shotover {
            Shotover::Standard | Shotover::ForcedMessageParsed => {
                let topology =
                    self.generate_topology_yaml(format!("{ip}:9092"), format!("{ip}:9192"));
                Some(instance.run_shotover(&topology).await)
            }
            Shotover::None => None,
        }
    }

    fn include_shotover_in_docker_instance(&self) -> bool {
        match self.topology {
            KafkaTopology::Single => !matches!(self.shotover, Shotover::None),
            KafkaTopology::Cluster1 => false,
            KafkaTopology::Cluster3 => false,
        }
    }

    fn shotover_instance_count(&self) -> usize {
        let need_shotover = !matches!(self.shotover, Shotover::None);
        if need_shotover && !self.include_shotover_in_docker_instance() {
            1
        } else {
            0
        }
    }
}

#[async_trait]
impl Bench for KafkaBench {
    type CloudResourcesRequired = CloudResourcesRequired;
    type CloudResources = CloudResources;

    fn cores_required(&self) -> usize {
        2
    }

    fn tags(&self) -> HashMap<String, String> {
        [
            ("db".to_owned(), "kafka".to_owned()),
            self.topology.to_tag(),
            self.shotover.to_tag(),
            match self.message_size {
                Size::B12 => ("size".to_owned(), "12B".to_owned()),
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

    fn required_cloud_resources(&self) -> Self::CloudResourcesRequired {
        let docker_instance_count = match self.topology {
            KafkaTopology::Single => 1,
            KafkaTopology::Cluster1 => 1,
            KafkaTopology::Cluster3 => 3,
        };
        let shotover_instance_count = self.shotover_instance_count();
        let include_shotover_in_docker_instance = self.include_shotover_in_docker_instance();
        CloudResourcesRequired {
            shotover_instance_count,
            docker_instance_count,
            include_shotover_in_docker_instance,
        }
    }

    async fn orchestrate_cloud(
        &self,
        mut cloud_resources: CloudResources,
        _running_in_release: bool,
        profiling: Profiling,
        parameters: BenchParameters,
    ) -> Result<()> {
        let kafka_instances = cloud_resources.docker;
        let shotover_instance = cloud_resources.shotover.pop();
        let bench_instance = cloud_resources.bencher.unwrap();

        let mut profiler_instances: HashMap<String, &Ec2Instance> =
            [("bencher".to_owned(), &bench_instance.instance)].into();

        // only profile instances that we are actually using for this bench
        for i in 0..self.shotover_instance_count() {
            profiler_instances.insert(
                format!("shotover{i}"),
                &shotover_instance.as_ref().unwrap().instance,
            );
        }
        match self.topology {
            KafkaTopology::Single | KafkaTopology::Cluster1 => {
                profiler_instances.insert("kafka".to_owned(), &kafka_instances[0].instance);
            }
            KafkaTopology::Cluster3 => {
                profiler_instances.insert("kafka1".to_owned(), &kafka_instances[0].instance);
                profiler_instances.insert("kafka2".to_owned(), &kafka_instances[1].instance);
                profiler_instances.insert("kafka3".to_owned(), &kafka_instances[2].instance);
            }
        }

        let kafka_ip = kafka_instances[0].instance.private_ip().to_string();
        let shotover_ip = shotover_instance
            .as_ref()
            .map(|x| x.instance.private_ip().to_string());
        let shotover_connect_ip = match self.topology {
            KafkaTopology::Single => kafka_instances
                .first()
                .as_ref()
                .map(|x| x.instance.connect_ip().to_string()),
            KafkaTopology::Cluster1 | KafkaTopology::Cluster3 => shotover_instance
                .as_ref()
                .map(|x| x.instance.connect_ip().to_string()),
        };

        let mut profiler = CloudProfilerRunner::new(
            self.name(),
            profiling,
            profiler_instances,
            &shotover_connect_ip,
        )
        .await;

        let (_, running_shotover) =
            futures::join!(self.run_aws_kafka(kafka_instances.clone()), async {
                match self.topology {
                    KafkaTopology::Single => {
                        self.run_aws_shotover_colocated_with_kafka(kafka_instances[0].clone())
                            .await
                    }
                    KafkaTopology::Cluster1 | KafkaTopology::Cluster3 => {
                        self.run_aws_shotover_on_own_instance(
                            shotover_instance,
                            kafka_instances[0].clone(),
                        )
                        .await
                    }
                }
            });

        let destination_address =
            if let Shotover::Standard | Shotover::ForcedMessageParsed = self.shotover {
                match &self.topology {
                    KafkaTopology::Single => format!("{kafka_ip}:9092"),
                    KafkaTopology::Cluster1 | KafkaTopology::Cluster3 => {
                        format!("{}:9092", shotover_ip.unwrap())
                    }
                }
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
        let config_dir = match self.topology {
            KafkaTopology::Single | KafkaTopology::Cluster1 => "tests/test-configs/kafka/bench",
            KafkaTopology::Cluster3 => "tests/test-configs/kafka/cluster-1-rack",
        };
        let _compose = docker_compose(&format!("{}/docker-compose.yaml", config_dir));

        let kafka_address = match self.topology {
            KafkaTopology::Single | KafkaTopology::Cluster1 => "127.0.0.1:9192",
            KafkaTopology::Cluster3 => "172.16.1.2:9092",
        };

        let mut profiler = ProfilerRunner::new(self.name(), profiling);
        let shotover = match self.shotover {
            Shotover::Standard | Shotover::ForcedMessageParsed => {
                let topology_yaml = self
                    .generate_topology_yaml("127.0.0.1:9092".to_owned(), kafka_address.to_owned());
                Some(shotover_process_custom_topology(&topology_yaml, &profiler).await)
            }
            Shotover::None => None,
        };

        let broker_address = match self.shotover {
            Shotover::ForcedMessageParsed | Shotover::Standard => "127.0.0.1:9092",
            Shotover::None => kafka_address,
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

        setup_topic(broker_address).await;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", broker_address)
            .set("debug", "all")
            .set("message.timeout.ms", "5000")
            .create()
            .unwrap();

        let message = match &self.message_size {
            Size::B12 => vec![0; 12],
            Size::KB1 => vec![0; 1024],
            Size::KB100 => vec![0; 1024 * 100],
        };

        let mut producer = BenchTaskProducerKafka { producer, message };

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

#[derive(Clone)]
struct BenchTaskProducerKafka {
    message: Vec<u8>,
    producer: FutureProducer,
}

#[async_trait]
impl BenchTaskProducer for BenchTaskProducerKafka {
    async fn produce_one(&mut self) -> Result<(), String> {
        // overwrite timestamp portion of the message with current timestamp
        serialize_system_time(SystemTime::now(), &mut self.message);

        // key is set to None which will result in round robin routing between all brokers
        let record: FutureRecord<(), _> = FutureRecord::to("topic_foo").payload(&self.message);
        self.producer
            .send(record, Timeout::Never)
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
            Ok(record) => {
                let produce_instant: SystemTime =
                    deserialize_system_time(record.payload().unwrap());
                // If time has gone backwards the results of this benchmark are compromised, so just unwrap elapsed()
                Report::ConsumeCompletedIn(Some(produce_instant.elapsed().unwrap()))
            }
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

/// Writes the system time into the first 12 bytes.
///
/// SystemTime is used instead of Instance because Instance does not expose
/// a way to retrieve the inner value and it is therefore impossible to serialize/deserialize.
fn serialize_system_time(time: SystemTime, dest: &mut [u8]) {
    let duration_since_epoch = time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let secs = duration_since_epoch.as_secs();
    let nanos = duration_since_epoch.subsec_nanos();
    dest[0..8].copy_from_slice(&secs.to_be_bytes());
    dest[8..12].copy_from_slice(&nanos.to_be_bytes());
}

/// Reads the system time from the first 12 bytes
fn deserialize_system_time(source: &[u8]) -> SystemTime {
    let secs = u64::from_be_bytes(source[0..8].try_into().unwrap());
    let nanos = u32::from_be_bytes(source[8..12].try_into().unwrap());
    SystemTime::UNIX_EPOCH
        .checked_add(Duration::new(secs, nanos))
        .unwrap()
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
    async fn produce_one(&mut self) -> Result<(), String>;

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
            let mut task = self.clone();
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

async fn setup_topic(broker_address: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", broker_address)
        .create()
        .unwrap();
    for topic in admin
        .create_topics(
            &[NewTopic {
                name: "topic_foo",
                num_partitions: 3,
                replication: TopicReplication::Fixed(1),
                config: vec![],
            }],
            &AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(60)))),
        )
        .await
        .unwrap()
    {
        assert_eq!("topic_foo", topic.unwrap());
    }

    // Need to delay starting bench to avoid UnknownPartition errors
    tokio::time::sleep(Duration::from_secs(5)).await;
}
