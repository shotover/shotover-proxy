use criterion::{criterion_group, criterion_main, Criterion};
use test_helpers::docker_compose::DockerCompose;

#[path = "../tests/helpers/mod.rs"]
mod helpers;
use helpers::ShotoverManager;

fn redis_active_bench(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-multi/docker-compose.yml");
    _compose.wait_for_n("Ready to accept connections", 3 ).unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-multi/topology.yaml");

    let mut connection = shotover_manager.redis_connection(6379);
    redis::cmd("FLUSHDB").execute(&mut connection);

    c.bench_function("redis_multi_speed", move |b| {
        b.iter(|| {
            redis::cmd("SET")
                .arg("foo")
                .arg(42)
                .execute(&mut connection);
        })
    });
}

fn redis_cluster_bench(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-cluster/docker-compose.yml");
    _compose.wait_for_n("Cluster state changed", 6).unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster/topology.yaml");

    let mut connection = shotover_manager.redis_connection(6379);
    redis::cmd("FLUSHDB").execute(&mut connection);

    c.bench_function("redis_cluster_speed", move |b| {
        b.iter(|| {
            redis::cmd("SET")
                .arg("foo")
                .arg(42)
                .execute(&mut connection);
        })
    });
}

fn redis_passthrough_bench(c: &mut Criterion) {
    let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml");
    _compose.wait_for("Ready to accept connections").unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-passthrough/topology.yaml");

    let mut connection = shotover_manager.redis_connection(6379);
    redis::cmd("FLUSHDB").execute(&mut connection);

    c.bench_function("redis_passthrough_speed", move |b| {
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
    redis_cluster_bench,
    redis_passthrough_bench,
    redis_active_bench
);
criterion_main!(benches);
