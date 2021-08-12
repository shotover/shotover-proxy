use criterion::{criterion_group, criterion_main, Criterion};
use std::thread::sleep;
use std::time::Duration;
use test_helpers::docker_compose::DockerCompose;

mod helpers;
use helpers::ShotoverManager;

fn redis_active_bench(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-multi/docker-compose.yml");
    let _shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-multi/topology.yaml");

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
}

fn redis_cluster_bench(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-cluster/docker-compose.yml");
    let _shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster/topology.yaml");

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
}

fn redis_passthrough_bench(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml");
    let _shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-passthrough/topology.yaml");

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
}

criterion_group!(
    benches,
    redis_cluster_bench,
    redis_passthrough_bench,
    redis_active_bench
);
criterion_main!(benches);
