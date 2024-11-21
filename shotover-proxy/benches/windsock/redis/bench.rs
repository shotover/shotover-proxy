use crate::{
    cloud::{
        CloudResources, CloudResourcesRequired, Ec2InstanceWithDocker, Ec2InstanceWithShotover,
        RunningShotover,
    },
    common::{self, Shotover},
    profilers::{self, CloudProfilerRunner, ProfilerRunner},
    shotover::shotover_process_custom_topology,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_throwaway::Ec2Instance;
use fred::{
    prelude::*,
    rustls::{ClientConfig, RootCertStore},
};
use itertools::Itertools;
use pretty_assertions::assert_eq;
use rustls_pemfile::Item;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use shotover::{
    config::chain::TransformChainConfig,
    sources::SourceConfig,
    tls::{TlsAcceptorConfig, TlsConnectorConfig},
    transforms::{
        debug::force_parse::DebugForceEncodeConfig,
        redis::{sink_cluster::RedisSinkClusterConfig, sink_single::RedisSinkSingleConfig},
        TransformConfig,
    },
};
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use test_helpers::{
    docker_compose::docker_compose,
    shotover_process::{Count, EventMatcher, Level},
};
use tokio::sync::mpsc::UnboundedSender;
use windsock::{Bench, BenchParameters, BenchTask, Profiling, Report};

#[derive(Clone, Copy)]
pub enum RedisOperation {
    Set,
    Get,
}

#[derive(Clone, Copy)]
pub enum RedisTopology {
    Single,
    Cluster3,
}

#[derive(Clone, Copy)]
pub enum Encryption {
    None,
    Tls,
}

pub struct RedisBench {
    topology: RedisTopology,
    shotover: Shotover,
    operation: RedisOperation,
    encryption: Encryption,
}

impl RedisBench {
    pub fn new(
        topology: RedisTopology,
        shotover: Shotover,
        operation: RedisOperation,
        encryption: Encryption,
    ) -> Self {
        RedisBench {
            topology,
            shotover,
            operation,
            encryption,
        }
    }

    fn generate_topology_yaml(&self, host_address: String, redis_address: String) -> String {
        let certs = "tests/test-configs/redis/tls/certs";
        let tls_connector = match self.encryption {
            Encryption::Tls => Some(TlsConnectorConfig {
                certificate_authority_path: format!("{certs}/localhost_CA.crt"),
                certificate_path: Some(format!("{certs}/localhost.crt")),
                private_key_path: Some(format!("{certs}/localhost.key")),
                verify_hostname: true,
            }),
            Encryption::None => None,
        };
        let tls_acceptor = match self.encryption {
            Encryption::Tls => Some(TlsAcceptorConfig {
                certificate_path: format!("{certs}/localhost.crt"),
                private_key_path: format!("{certs}/localhost.key"),
                certificate_authority_path: None,
            }),
            Encryption::None => None,
        };

        let mut transforms = vec![];
        if let Shotover::ForcedMessageParsed = self.shotover {
            transforms.push(Box::new(DebugForceEncodeConfig {
                encode_requests: true,
                encode_responses: true,
            }) as Box<dyn TransformConfig>);
        }

        match self.topology {
            RedisTopology::Cluster3 => {
                transforms.push(Box::new(RedisSinkClusterConfig {
                    first_contact_points: vec![redis_address],
                    direct_destination: None,
                    tls: tls_connector,
                    connection_count: None,
                    connect_timeout_ms: 3000,
                }));
            }
            RedisTopology::Single => {
                transforms.push(Box::new(RedisSinkSingleConfig {
                    address: redis_address,
                    tls: tls_connector,
                    connect_timeout_ms: 3000,
                }));
            }
        }

        common::generate_topology(SourceConfig::Valkey(
            shotover::sources::redis::ValkeyConfig {
                name: "redis".to_owned(),
                listen_addr: host_address,
                connection_limit: None,
                hard_connection_limit: None,
                tls: tls_acceptor,
                timeout: None,
                chain: TransformChainConfig(transforms),
            },
        ))
    }

    async fn run_aws_shotover(
        &self,
        instance: Option<Arc<Ec2InstanceWithShotover>>,
        redis_ip: String,
    ) -> Option<RunningShotover> {
        if let Some(instance) = instance {
            let ip = instance.instance.private_ip().to_string();
            let topology =
                self.generate_topology_yaml(format!("{ip}:6379"), format!("{redis_ip}:6379"));
            Some(instance.run_shotover(&topology).await)
        } else {
            None
        }
    }
}

#[async_trait]
impl Bench for RedisBench {
    type CloudResourcesRequired = CloudResourcesRequired;
    type CloudResources = CloudResources;

    fn tags(&self) -> HashMap<String, String> {
        [
            ("db".to_owned(), "redis".to_owned()),
            (
                "topology".to_owned(),
                match &self.topology {
                    RedisTopology::Single => "single".to_owned(),
                    RedisTopology::Cluster3 => "cluster3".to_owned(),
                },
            ),
            (
                "operation".to_owned(),
                match &self.operation {
                    RedisOperation::Set => "set".to_owned(),
                    RedisOperation::Get => "get".to_owned(),
                },
            ),
            (
                "encryption".to_owned(),
                match &self.encryption {
                    Encryption::None => "none".to_owned(),
                    Encryption::Tls => "tls".to_owned(),
                },
            ),
            self.shotover.to_tag(),
        ]
        .into_iter()
        .collect()
    }

    fn supported_profilers(&self) -> Vec<String> {
        profilers::supported_profilers(self.shotover)
    }

    fn cores_required(&self) -> usize {
        2
    }

    fn required_cloud_resources(&self) -> Self::CloudResourcesRequired {
        let shotover_instance_count =
            if let Shotover::Standard | Shotover::ForcedMessageParsed = self.shotover {
                1
            } else {
                0
            };
        let docker_instance_count = match self.topology {
            RedisTopology::Single => 1,
            RedisTopology::Cluster3 => 7,
        };
        CloudResourcesRequired {
            shotover_instance_count,
            docker_instance_count,
            include_shotover_in_docker_instance: false,
        }
    }

    async fn orchestrate_cloud(
        &self,
        mut cloud_resources: CloudResources,
        _running_in_release: bool,
        profiling: Profiling,
        parameters: BenchParameters,
    ) -> Result<()> {
        let bench_instance = cloud_resources.bencher.unwrap();
        let shotover_instance = cloud_resources.shotover.pop();
        let redis_instances = RedisCluster::create(cloud_resources.docker, self.topology);

        let mut profiler_instances: HashMap<String, &Ec2Instance> =
            [("bencher".to_owned(), &bench_instance.instance)].into();
        if let Shotover::ForcedMessageParsed | Shotover::Standard = self.shotover {
            profiler_instances.insert(
                "shotover".to_owned(),
                &shotover_instance.as_ref().unwrap().instance,
            );
        }
        match &redis_instances {
            RedisCluster::Cluster3 { instances, .. } => {
                for (i, instance) in instances.iter().enumerate() {
                    profiler_instances.insert(format!("redis{i}"), &instance.instance);
                }
            }
            RedisCluster::Single(instance) => {
                profiler_instances.insert("redis".to_owned(), &instance.instance);
            }
        }

        let redis_ip = redis_instances.private_ips()[0].to_string();
        let shotover_ip = shotover_instance
            .as_ref()
            .map(|x| x.instance.private_ip().to_string());
        let shotover_connect_ip = shotover_instance
            .as_ref()
            .map(|x| x.instance.connect_ip().to_string());

        let mut profiler = CloudProfilerRunner::new(
            self.name(),
            profiling,
            profiler_instances,
            &shotover_connect_ip,
        )
        .await;

        let (_, running_shotover) = futures::join!(
            redis_instances.run(self.encryption),
            self.run_aws_shotover(shotover_instance.clone(), redis_ip.clone())
        );

        let destination_ip = if let Some(shotover_ip) = shotover_ip {
            format!("redis://{shotover_ip}")
        } else {
            match self.topology {
                RedisTopology::Single => format!("redis://{redis_ip}"),
                RedisTopology::Cluster3 => format!("redis-cluster://{redis_ip}"),
            }
        };

        bench_instance
            .run_bencher(&self.run_args(&destination_ip, &parameters), &self.name())
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
        test_helpers::cert::generate_redis_test_certs();

        // rediss:// url is not needed to enable TLS because we overwrite the TLS config later on
        let client_url = match (self.topology, self.shotover) {
            (RedisTopology::Single, Shotover::None) => "redis://127.0.0.1:1111",
            (RedisTopology::Cluster3, Shotover::None) => "redis-cluster://172.16.1.2:6379",
            (
                RedisTopology::Single | RedisTopology::Cluster3,
                Shotover::Standard | Shotover::ForcedMessageParsed,
            ) => "redis://127.0.0.1:6379",
        };
        let redis_address = match self.topology {
            RedisTopology::Single => "127.0.0.1:1111",
            RedisTopology::Cluster3 => "172.16.1.2:6379",
        };
        let config_dir = match (self.topology, self.encryption) {
            (RedisTopology::Single, Encryption::None) => "tests/test-configs/redis/passthrough",
            (RedisTopology::Cluster3, Encryption::None) => {
                "tests/test-configs/redis/cluster-hiding"
            }
            (RedisTopology::Single, Encryption::Tls) => "tests/test-configs/redis/tls",
            (RedisTopology::Cluster3, Encryption::Tls) => "tests/test-configs/redis/cluster-tls",
        };
        let _compose = docker_compose(&format!("{config_dir}/docker-compose.yaml"));
        let mut profiler = ProfilerRunner::new(self.name(), profiling);
        let shotover = match self.shotover {
            Shotover::Standard | Shotover::ForcedMessageParsed => {
                let topology_yaml = self
                    .generate_topology_yaml("127.0.0.1:6379".to_owned(), redis_address.to_owned());
                Some(shotover_process_custom_topology(&topology_yaml, &profiler).await)
            }
            Shotover::None => None,
        };
        profiler.run(&shotover).await;

        self.execute_run(client_url, &parameters).await;

        if let Some(shotover) = shotover {
            shotover
                .shutdown_and_then_consume_events(&[EventMatcher::new()
                    .with_level(Level::Error)
                    .with_message("encountered error in redis stream: Io(Kind(UnexpectedEof))")
                    .with_target("shotover::transforms::redis::sink_single")
                    .with_count(Count::Any)])
                .await;
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
        let address = resources;

        let mut config = RedisConfig::from_url(address).unwrap();
        if let Encryption::Tls = self.encryption {
            let private_key =
                load_private_key("tests/test-configs/redis/tls/certs/localhost.key").unwrap();
            let certs = load_certs("tests/test-configs/redis/tls/certs/localhost.crt").unwrap();
            config.tls = Some(
                ClientConfig::builder()
                    .with_root_certificates(
                        load_ca("tests/test-configs/redis/tls/certs/localhost_CA.crt").unwrap(),
                    )
                    .with_client_auth_cert(certs, private_key)
                    .unwrap()
                    .into(),
            );
        }
        let client = Arc::new(RedisClient::new(config, None, None, None));

        // connect to the server, returning a handle to the task that drives the connection
        let shutdown_handle = client.connect();
        client.wait_for_connect().await.unwrap();

        if let RedisOperation::Get = self.operation {
            let _: () = client.set("foo", 42, None, None, false).await.unwrap();
        }

        let tasks = BenchTaskRedis {
            client: client.clone(),
            operation: self.operation,
        }
        .spawn_tasks(reporter.clone(), parameters.operations_per_second)
        .await;

        // warm up and then start
        tokio::time::sleep(Duration::from_secs(1)).await;
        reporter.send(Report::Start).unwrap();
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

        client.quit().await.unwrap();
        shutdown_handle.await.unwrap().unwrap();
    }
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut BufReader::new(File::open(path)?))
        .collect::<Result<Vec<_>, _>>()
        .context("Error while parsing PEM")
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    for key in rustls_pemfile::read_all(&mut BufReader::new(File::open(path)?)) {
        match key.context("Error while parsing PEM")? {
            Item::Pkcs8Key(x) => return Ok(x.into()),
            Item::Pkcs1Key(x) => return Ok(x.into()),
            _ => {}
        }
    }
    Err(anyhow!("No suitable keys found in PEM"))
}

fn load_ca(path: &str) -> Result<RootCertStore> {
    let mut pem = BufReader::new(File::open(path)?);
    let mut root_cert_store = RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut pem) {
        root_cert_store
            .add(cert.context("Error while parsing PEM")?)
            .context("Failed to add cert to cert store")?;
    }
    Ok(root_cert_store)
}

#[derive(Clone)]
struct BenchTaskRedis {
    client: Arc<RedisClient>,
    operation: RedisOperation,
}

#[async_trait]
impl BenchTask for BenchTaskRedis {
    async fn run_one_operation(&self) -> Result<(), String> {
        match self.operation {
            RedisOperation::Set => {
                let _: () = self
                    .client
                    .set("foo", "bar", None, None, false)
                    .await
                    .map_err(|err| format!("{err}"))?;
            }
            RedisOperation::Get => {
                let result: u32 = self
                    .client
                    .get("foo")
                    .await
                    .map_err(|err| format!("{err}"))?;
                assert_eq!(result, 42);
            }
        }
        Ok(())
    }
}

enum RedisCluster {
    Single(Arc<Ec2InstanceWithDocker>),
    Cluster3 {
        instances: [Arc<Ec2InstanceWithDocker>; 6],
        cluster_creator: Arc<Ec2InstanceWithDocker>,
    },
}

impl RedisCluster {
    fn create(mut instances: Vec<Arc<Ec2InstanceWithDocker>>, topology: RedisTopology) -> Self {
        match topology {
            RedisTopology::Single => RedisCluster::Single(instances.pop().unwrap()),
            RedisTopology::Cluster3 => RedisCluster::Cluster3 {
                cluster_creator: instances.pop().unwrap(),
                instances: [
                    instances.pop().unwrap(),
                    instances.pop().unwrap(),
                    instances.pop().unwrap(),
                    instances.pop().unwrap(),
                    instances.pop().unwrap(),
                    instances.pop().unwrap(),
                ],
            },
        }
    }

    async fn run(&self, encryption: Encryption) {
        match self {
            RedisCluster::Single(instance) => match encryption {
                Encryption::None => instance.run_container("library/redis:5.0.9", &[]).await,
                Encryption::Tls => todo!(),
            },
            RedisCluster::Cluster3 {
                instances,
                cluster_creator,
            } => {
                if let Encryption::Tls = encryption {
                    todo!()
                }
                let mut wait_for = vec![];
                for instance in instances {
                    let node_addresses = self.private_ips();
                    let instance = instance.clone();
                    wait_for.push(tokio::spawn(async move {
                        instance
                            .run_container(
                                "bitnami/redis-cluster:6.2.12-debian-11-r26",
                                &[
                                    ("ALLOW_EMPTY_PASSWORD".to_owned(), "yes".to_owned()),
                                    (
                                        "REDIS_NODES".to_owned(),
                                        node_addresses.iter().map(|x| x.to_string()).join(" "),
                                    ),
                                ],
                            )
                            .await
                    }));
                }

                let node_addresses = self.private_ips();
                cluster_creator
                    .run_container(
                        "bitnami/redis-cluster:6.2.12-debian-11-r26",
                        &[
                            ("ALLOW_EMPTY_PASSWORD".to_owned(), "yes".to_owned()),
                            (
                                "REDIS_NODES".to_owned(),
                                node_addresses.iter().map(|x| x.to_string()).join(" "),
                            ),
                            ("REDIS_CLUSTER_REPLICAS".to_owned(), "1".to_owned()),
                            ("REDIS_CLUSTER_CREATOR".to_owned(), "yes".to_owned()),
                        ],
                    )
                    .await;

                for wait_for in wait_for {
                    wait_for.await.unwrap();
                }
            }
        }
    }

    fn private_ips(&self) -> Vec<IpAddr> {
        match self {
            RedisCluster::Single(instance) => vec![instance.instance.private_ip()],
            RedisCluster::Cluster3 { instances, .. } => {
                instances.iter().map(|x| x.instance.private_ip()).collect()
            }
        }
    }
}
