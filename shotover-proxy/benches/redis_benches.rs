use anyhow::Result;

use criterion::{criterion_group, criterion_main, Criterion};
use shotover_proxy::config::topology::Topology;
use std::thread::sleep;
use std::time::Duration;
use std::time;
use tokio::runtime;
use tokio::task::JoinHandle;
use tracing::Level;
use test_helpers::docker_compose::DockerCompose;

pub fn start_proxy(config: String) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) = Topology::from_file(config)?.run_chains().await {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok(())
    })
}

fn redis_active_bench(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-multi/docker-compose.yml");

    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .worker_threads(4)
        .build()
        .unwrap();

    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::ERROR)
        .try_init();

    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file("examples/redis-multi/topology.yaml".to_string())
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

    c.bench_function("redis_multi_speed", move |b| {
        b.iter(|| {
            redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
        })
    });

    rt.shutdown_timeout(time::Duration::from_secs(1));
}

fn redis_cluster_bench(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-cluster/docker-compose.yml");

    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::ERROR)
        .try_init();

    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .worker_threads(4)
        .build()
        .unwrap();

    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file("examples/redis-cluster/topology.yaml".to_string())
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

    c.bench_function("redis_cluster_speed", move |b| {
        b.iter(|| {
            redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
        })
    });

    rt.shutdown_timeout(time::Duration::from_secs(1));
}

fn redis_passthrough_bench(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml");

    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .worker_threads(4)
        .build()
        .unwrap();

    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::ERROR)
        .try_init();

    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file("examples/redis-passthrough/topology.yaml".to_string())
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

    c.bench_function("redis_passthrough_speed", move |b| {
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
