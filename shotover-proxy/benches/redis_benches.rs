use criterion::{criterion_group, criterion_main, Criterion};
use std::path::Path;
use test_helpers::docker_compose::DockerCompose;

#[path = "../tests/helpers/mod.rs"]
mod helpers;
use helpers::ShotoverManager;

fn redis(c: &mut Criterion) {
    let mut group = c.benchmark_group("redis");
    group.throughput(criterion::Throughput::Elements(1));
    group.noise_threshold(0.2);

    group.bench_with_input(
        "active",
        || {
            let compose = DockerCompose::new("example-configs/redis-multi/docker-compose.yml");
            let shotover_manager =
                ShotoverManager::from_topology_file("example-configs/redis-multi/topology.yaml");
            BenchResources::new(shotover_manager, compose)
        },
        move |b, state| {
            b.iter(|| {
                redis::cmd("SET")
                    .arg("foo")
                    .arg(42)
                    .execute(&mut state.connection);
            })
        },
    );

    group.bench_with_input(
        "cluster",
        || {
            let compose = DockerCompose::new("example-configs/redis-cluster/docker-compose.yml");
            let shotover_manager =
                ShotoverManager::from_topology_file("example-configs/redis-cluster/topology.yaml");
            BenchResources::new(shotover_manager, compose)
        },
        move |b, state| {
            b.iter(|| {
                redis::cmd("SET")
                    .arg("foo")
                    .arg(42)
                    .execute(&mut state.connection);
            })
        },
    );

    group.bench_with_input(
        "passthrough",
        || {
            let compose =
                DockerCompose::new("example-configs/redis-passthrough/docker-compose.yml");
            let shotover_manager = ShotoverManager::from_topology_file(
                "example-configs/redis-passthrough/topology.yaml",
            );
            BenchResources::new(shotover_manager, compose)
        },
        move |b, state| {
            b.iter(|| {
                redis::cmd("SET")
                    .arg("foo")
                    .arg(42)
                    .execute(&mut state.connection);
            })
        },
    );

    group.bench_with_input(
        "single_tls",
        || {
            test_helpers::cert::generate_test_certs(Path::new("example-configs/redis-tls/certs"));
            let compose = DockerCompose::new("example-configs/redis-tls/docker-compose.yml");
            let shotover_manager =
                ShotoverManager::from_topology_file("example-configs/redis-tls/topology.yaml");
            BenchResources::new(shotover_manager, compose)
        },
        move |b, state| {
            b.iter(|| {
                redis::cmd("SET")
                    .arg("foo")
                    .arg(42)
                    .execute(&mut state.connection);
            })
        },
    );

    group.bench_with_input(
        "cluster_tls",
        || {
            test_helpers::cert::generate_test_certs(Path::new("example-configs/redis-tls/certs"));
            let compose =
                DockerCompose::new("example-configs/redis-cluster-tls/docker-compose.yml");
            let shotover_manager = ShotoverManager::from_topology_file(
                "example-configs/redis-cluster-tls/topology.yaml",
            );
            BenchResources::new(shotover_manager, compose)
        },
        move |b, state| {
            b.iter(|| {
                redis::cmd("SET")
                    .arg("foo")
                    .arg(42)
                    .execute(&mut state.connection);
            })
        },
    );
}

criterion_group!(benches, redis);
criterion_main!(benches);

struct BenchResources {
    _compose: DockerCompose,
    _shotover_manager: ShotoverManager,
    connection: redis::Connection,
}

impl BenchResources {
    fn new(shotover_manager: ShotoverManager, compose: DockerCompose) -> Self {
        let mut connection = shotover_manager.redis_connection(6379);
        redis::cmd("FLUSHDB").execute(&mut connection);

        Self {
            _compose: compose,
            _shotover_manager: shotover_manager,
            connection,
        }
    }
}
