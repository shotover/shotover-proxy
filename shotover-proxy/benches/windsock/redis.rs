use crate::{
    aws::{Ec2InstanceWithDocker, Ec2InstanceWithShotover, RunningShotover, WindsockAws},
    common::{rewritten_file, Shotover},
    profilers::{self, CloudProfilerRunner, ProfilerRunner},
    shotover::shotover_process,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_throwaway::Ec2Instance;
use fred::{
    prelude::*,
    rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore},
};
use itertools::Itertools;
use rustls_pemfile::{certs, Item};
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    net::IpAddr,
    path::Path,
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
}

#[async_trait]
impl Bench for RedisBench {
    fn tags(&self) -> HashMap<String, String> {
        [
            ("name".to_owned(), "redis".to_owned()),
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

    async fn orchestrate_cloud(
        &self,
        _running_in_release: bool,
        profiling: Profiling,
        parameters: BenchParameters,
    ) -> Result<()> {
        let aws = WindsockAws::get().await;

        let (redis_instances, bench_instance, shotover_instance) = futures::join!(
            RedisCluster::create(aws, self.topology),
            aws.create_bencher_instance(),
            aws.create_shotover_instance()
        );

        let mut profiler_instances: HashMap<String, &Ec2Instance> =
            [("bencher".to_owned(), &bench_instance.instance)].into();
        if let Shotover::ForcedMessageParsed | Shotover::Standard = self.shotover {
            profiler_instances.insert("shotover".to_owned(), &shotover_instance.instance);
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
        let mut profiler =
            CloudProfilerRunner::new(self.name(), profiling, profiler_instances).await;

        let redis_ip = redis_instances.private_ips()[0].to_string();
        let shotover_ip = shotover_instance.instance.private_ip().to_string();

        redis_instances.run(self.encryption).await;
        // unlike other sinks, redis cluster sink needs the redis instance to be already up
        let running_shotover = run_aws_shotover(
            shotover_instance.clone(),
            self.shotover,
            redis_ip.clone(),
            self.topology,
        )
        .await;

        let destination_ip = if running_shotover.is_some() {
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
        let address = match (self.topology, self.shotover) {
            (RedisTopology::Single, Shotover::None) => "redis://127.0.0.1:1111",
            (RedisTopology::Cluster3, Shotover::None) => "redis-cluster://172.16.1.2:6379",
            (
                RedisTopology::Single | RedisTopology::Cluster3,
                Shotover::Standard | Shotover::ForcedMessageParsed,
            ) => "redis://127.0.0.1:6379",
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
            Shotover::Standard => {
                Some(shotover_process(&format!("{config_dir}/topology.yaml"), &profiler).await)
            }
            Shotover::ForcedMessageParsed => Some(
                shotover_process(&format!("{config_dir}/topology-encode.yaml"), &profiler).await,
            ),
            Shotover::None => None,
        };
        profiler.run(&shotover);

        self.execute_run(address, &parameters).await;

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
            let private_key = load_private_key("tests/test-configs/redis/tls/certs/localhost.key");
            let certs = load_certs("tests/test-configs/redis/tls/certs/localhost.crt");
            config.tls = Some(
                ClientConfig::builder()
                    .with_safe_defaults()
                    .with_root_certificates(load_ca(
                        "tests/test-configs/redis/tls/certs/localhost_CA.crt",
                    ))
                    .with_client_auth_cert(certs, private_key)
                    .unwrap()
                    .into(),
            );
        }
        let client = Arc::new(RedisClient::new(config, None, None));

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

fn load_certs(path: &str) -> Vec<Certificate> {
    load_certs_inner(path)
        .with_context(|| format!("Failed to read certs at {path:?}"))
        .unwrap()
}
fn load_certs_inner(path: &str) -> Result<Vec<Certificate>> {
    certs(&mut BufReader::new(File::open(path)?))
        .context("Error while parsing PEM")
        .map(|certs| certs.into_iter().map(Certificate).collect())
}

fn load_private_key(path: &str) -> PrivateKey {
    load_private_key_inner(path)
        .with_context(|| format!("Failed to read private key at {path:?}"))
        .unwrap()
}
fn load_private_key_inner(path: &str) -> Result<PrivateKey> {
    let keys = rustls_pemfile::read_all(&mut BufReader::new(File::open(path)?))
        .context("Error while parsing PEM")?;
    keys.into_iter()
        .find_map(|item| match item {
            Item::RSAKey(x) | Item::PKCS8Key(x) => Some(PrivateKey(x)),
            _ => None,
        })
        .ok_or_else(|| anyhow!("No suitable keys found in PEM"))
}

fn load_ca(path: &str) -> RootCertStore {
    load_ca_inner(path)
        .with_context(|| format!("Failed to load CA at {path:?}"))
        .unwrap()
}
fn load_ca_inner(path: &str) -> Result<RootCertStore> {
    let mut pem = BufReader::new(File::open(path)?);
    let certs = rustls_pemfile::certs(&mut pem).context("Error while parsing PEM")?;

    let mut root_cert_store = RootCertStore::empty();
    for cert in certs {
        root_cert_store
            .add(&Certificate(cert))
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

async fn run_aws_shotover(
    instance: Arc<Ec2InstanceWithShotover>,
    shotover: Shotover,
    redis_ip: String,
    topology: RedisTopology,
) -> Option<RunningShotover> {
    let config_dir = "tests/test-configs/redis/bench";
    let ip = instance.instance.private_ip().to_string();
    match shotover {
        Shotover::Standard | Shotover::ForcedMessageParsed => {
            let clustered = match topology {
                RedisTopology::Single => "",
                RedisTopology::Cluster3 => "-cluster",
            };
            let encoded = match shotover {
                Shotover::Standard => "",
                Shotover::ForcedMessageParsed => "-encode",
                Shotover::None => unreachable!(),
            };
            let topology = rewritten_file(
                Path::new(&format!(
                    "{config_dir}/topology{clustered}{encoded}-cloud.yaml"
                )),
                &[("HOST_ADDRESS", &ip), ("REDIS_ADDRESS", &redis_ip)],
            )
            .await;
            Some(instance.run_shotover(&topology).await)
        }
        Shotover::None => None,
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
    async fn create(aws: &'static WindsockAws, topology: RedisTopology) -> Self {
        match topology {
            RedisTopology::Single => RedisCluster::Single(aws.create_docker_instance().await),
            RedisTopology::Cluster3 => RedisCluster::Cluster3 {
                instances: [
                    aws.create_docker_instance().await,
                    aws.create_docker_instance().await,
                    aws.create_docker_instance().await,
                    aws.create_docker_instance().await,
                    aws.create_docker_instance().await,
                    aws.create_docker_instance().await,
                ],
                cluster_creator: aws.create_docker_instance().await,
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
