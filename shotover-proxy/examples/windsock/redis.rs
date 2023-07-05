use crate::{common::Shotover, profilers::ProfilerRunner};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use fred::{
    prelude::*,
    rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore},
};
use rustls_pemfile::{certs, Item};
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    sync::Arc,
    time::{Duration, Instant},
};
use test_helpers::{
    docker_compose::docker_compose,
    shotover_process::{Count, EventMatcher, Level, ShotoverProcessBuilder},
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
        ProfilerRunner::supported_profilers(self.shotover)
    }

    fn cores_required(&self) -> usize {
        2
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
            (RedisTopology::Single, Encryption::None) => "tests/test-configs/redis-passthrough",
            (RedisTopology::Cluster3, Encryption::None) => {
                "tests/test-configs/redis-cluster-hiding"
            }
            (RedisTopology::Single, Encryption::Tls) => "tests/test-configs/redis-tls",
            (RedisTopology::Cluster3, Encryption::Tls) => "tests/test-configs/redis-cluster-tls",
        };
        let _compose = docker_compose(&format!("{config_dir}/docker-compose.yaml"));
        let mut profiler = ProfilerRunner::new(profiling);
        let shotover = match self.shotover {
            Shotover::Standard => Some(
                ShotoverProcessBuilder::new_with_topology(&format!("{config_dir}/topology.yaml"))
                    .with_profile(profiler.shotover_profile())
                    .start()
                    .await,
            ),
            Shotover::ForcedMessageParsed => Some(
                ShotoverProcessBuilder::new_with_topology(&format!(
                    "{config_dir}/topology-encode.yaml"
                ))
                .with_profile(profiler.shotover_profile())
                .start()
                .await,
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
            let private_key = load_private_key("tests/test-configs/redis-tls/certs/localhost.key");
            let certs = load_certs("tests/test-configs/redis-tls/certs/localhost.crt");
            config.tls = Some(
                ClientConfig::builder()
                    .with_safe_defaults()
                    .with_root_certificates(load_ca(
                        "tests/test-configs/redis-tls/certs/localhost_CA.crt",
                    ))
                    .with_single_cert(certs, private_key)
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
        .map_err(|err| anyhow!(err).context(format!("Failed to read certs at {path:?}")))
        .unwrap()
}
fn load_certs_inner(path: &str) -> Result<Vec<Certificate>> {
    certs(&mut BufReader::new(File::open(path)?))
        .context("Error while parsing PEM")
        .map(|certs| certs.into_iter().map(Certificate).collect())
}

fn load_private_key(path: &str) -> PrivateKey {
    load_private_key_inner(path)
        .map_err(|err| anyhow!(err).context(format!("Failed to read private key at {path:?}")))
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
        .map_err(|e| e.context(format!("Failed to load CA at {path:?}")))
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
    async fn run_one_operation(&self) {
        match self.operation {
            RedisOperation::Set => {
                let _: () = self
                    .client
                    .set("foo", "bar", None, None, false)
                    .await
                    .unwrap();
            }
            RedisOperation::Get => {
                let result: u32 = self.client.get("foo").await.unwrap();
                assert_eq!(result, 42);
            }
        }
    }
}
