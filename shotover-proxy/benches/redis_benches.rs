use criterion::{criterion_group, criterion_main, Criterion};
use test_helpers::docker_compose::DockerCompose;

#[path = "../tests/helpers/mod.rs"]
mod helpers;
use helpers::ShotoverManager;

fn redis_active(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-multi/docker-compose.yml")
        .wait_for_n("Ready to accept connections", 3);
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-multi/topology.yaml");

    let mut connection = shotover_manager.redis_connection(6379);
    redis::cmd("FLUSHDB").execute(&mut connection);

    c.bench_function("redis_active", move |b| {
        b.iter(|| {
            redis::cmd("SET")
                .arg("foo")
                .arg(42)
                .execute(&mut connection);
        })
    });
}

fn redis_cluster(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-cluster/docker-compose.yml")
        .wait_for_n("Cluster state changed", 6);
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster/topology.yaml");

    let mut connection = shotover_manager.redis_connection(6379);
    redis::cmd("FLUSHDB").execute(&mut connection);

    c.bench_function("redis_cluster", move |b| {
        b.iter(|| {
            redis::cmd("SET")
                .arg("foo")
                .arg(42)
                .execute(&mut connection);
        })
    });
}

fn redis_passthrough(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml")
        .wait_for("Ready to accept connections");
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-passthrough/topology.yaml");

    let mut connection = shotover_manager.redis_connection(6379);
    redis::cmd("FLUSHDB").execute(&mut connection);

    c.bench_function("redis_passthrough", move |b| {
        b.iter(|| {
            redis::cmd("SET")
                .arg("foo")
                .arg(42)
                .execute(&mut connection);
        })
    });
}

fn redis_destination_tls(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-tls/docker-compose.yml")
        .wait_for("Ready to accept connections");
    let shotover_manager = ShotoverManager::from_topology_file("examples/redis-tls/topology.yaml");

    let mut connection = shotover_manager.redis_connection(6379);
    redis::cmd("FLUSHDB").execute(&mut connection);

    c.bench_function("redis_destination_tls", move |b| {
        b.iter(|| {
            redis::cmd("SET")
                .arg("foo")
                .arg(42)
                .execute(&mut connection);
        })
    });
}

fn redis_cluster_tls(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-cluster-tls/docker-compose.yml")
        .wait_for_n("Cluster state changed", 6);
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster-tls/topology.yaml");

    let mut connection = shotover_manager.redis_connection(6379);
    redis::cmd("FLUSHDB").execute(&mut connection);

    c.bench_function("redis_cluster_tls", move |b| {
        b.iter(|| {
            redis::cmd("SET")
                .arg("foo")
                .arg(42)
                .execute(&mut connection);
        })
    });
}

criterion_group!(
    benches,
    redis_cluster,
    redis_passthrough,
    redis_active,
    redis_destination_tls,
    redis_cluster_tls,
);
criterion_main!(benches);
