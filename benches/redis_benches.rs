use anyhow::{anyhow, Result};
use redis;
use redis::Commands;
use tracing::info;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use shotover_proxy::config::topology::Topology;
use std::collections::{BTreeMap, BTreeSet};
use std::collections::{HashMap, HashSet};
use std::io::BufReader;
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;
use std::{thread, time};
use tokio::runtime;
use tokio::task::JoinHandle;
use tracing::Level;

pub fn start_proxy(config: String) -> JoinHandle<Result<()>> {
    return tokio::spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) = Topology::from_file(config)?.run_chains().await {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok(())
    });
}

pub fn load_docker_compose(file_path: String) -> Result<()> {
    // stop_docker_compose(file_path.clone())?;
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(format!("docker-compose -f {} up -d", file_path.as_str()));

    info!("running {:#?}", command);

    let output = command
        .status()
        .expect("could not exec process docker-compose");
    thread::sleep(time::Duration::from_secs(4));

    if output.success() {
        return Ok(());
    }
    Err(anyhow!(
        "couldn't start docker compose {}",
        output.to_string()
    ))
}

pub fn stop_docker_compose(file_path: String) -> Result<()> {
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(format!("docker-compose -f {} down", file_path.as_str()));

    info!("running {:#?}", command);

    let output = command
        .status()
        .expect("could not exec process docker-compose");

    let mut command2 = Command::new("sh");
    command2.arg("-c").arg(format!(
        "docker-compose -f {} rm -f -s -v",
        file_path.as_str()
    ));

    info!("running {:#?}", command2);

    let output2 = command2
        .status()
        .expect("could not exec process docker-compose");

    thread::sleep(time::Duration::from_secs(1));

    let mut command3 = Command::new("sh");
    command3
        .arg("-c")
        .arg(format!("yes | docker network prune"));

    info!("running {:#?}", command3);

    let output3 = command3
        .status()
        .expect("could not exec process docker-compose");

    output3.success();

    if output.success() || output2.success() {
        return Ok(());
    }
    Err(anyhow!(
        "couldn't start docker compose {}",
        output.to_string()
    ))
}

fn redis_active_bench(c: &mut Criterion) {
    let compose_config = "examples/redis-multi/docker-compose.yml".to_string();
    load_docker_compose(compose_config.clone()).unwrap();

    let rt = runtime::Builder::new()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .threaded_scheduler()
        .core_threads(4)
        .build()
        .unwrap();

    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();

    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file("examples/redis-multi/config.yaml".to_string())
                .unwrap()
                .run_chains()
                .await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
    let mut con;

    let millisecond = Duration::from_millis(1);
    loop {
        match client.get_connection() {
            Err(err) => {
                if err.is_connection_refusal() {
                    sleep(millisecond);
                } else {
                    panic!("Could not connect: {}", err);
                }
            }
            Ok(x) => {
                con = x;
                break;
            }
        }
    }
    redis::cmd("FLUSHDB").execute(&mut con);

    c.bench_function("redis_speed", move |b| {
        b.iter(|| {
            redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
        })
    });

    rt.shutdown_timeout(time::Duration::from_secs(1));
}

fn redis_cluster_bench(c: &mut Criterion) {
    let compose_config = "examples/redis-cluster/docker-compose.yml".to_string();
    load_docker_compose(compose_config.clone()).unwrap();

    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();

    let rt = runtime::Builder::new()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .threaded_scheduler()
        .core_threads(4)
        .build()
        .unwrap();

    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file("examples/redis-cluster/config.yaml".to_string())
                .unwrap()
                .run_chains()
                .await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
    let mut con;

    let millisecond = Duration::from_millis(1);
    loop {
        match client.get_connection() {
            Err(err) => {
                if err.is_connection_refusal() {
                    sleep(millisecond);
                } else {
                    panic!("Could not connect: {}", err);
                }
            }
            Ok(x) => {
                con = x;
                break;
            }
        }
    }
    redis::cmd("FLUSHDB").execute(&mut con);

    c.bench_function("redis_speed", move |b| {
        b.iter(|| {
            redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
        })
    });

    rt.shutdown_timeout(time::Duration::from_secs(1));
}

fn redis_passthrough_bench(c: &mut Criterion) {
    let compose_config = "examples/redis-passthrough/docker-compose.yml".to_string();
    load_docker_compose(compose_config.clone()).unwrap();

    let rt = runtime::Builder::new()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .threaded_scheduler()
        .core_threads(4)
        .build()
        .unwrap();

    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();

    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file("examples/redis-passthrough/config.yaml".to_string())
                .unwrap()
                .run_chains()
                .await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
    let mut con;

    let millisecond = Duration::from_millis(1);
    loop {
        match client.get_connection() {
            Err(err) => {
                if err.is_connection_refusal() {
                    sleep(millisecond);
                } else {
                    panic!("Could not connect: {}", err);
                }
            }
            Ok(x) => {
                con = x;
                break;
            }
        }
    }
    redis::cmd("FLUSHDB").execute(&mut con);

    c.bench_function("redis_speed", move |b| {
        b.iter(|| {
            redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
        })
    });

    rt.shutdown_timeout(time::Duration::from_secs(1));
}

criterion_group!(
    benches,
    redis_cluster_bench,
    redis_passthrough_bench,
    redis_active_bench
);
criterion_main!(benches);
