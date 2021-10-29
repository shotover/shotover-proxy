use criterion::{criterion_group, criterion_main, Criterion};
use test_helpers::docker_compose::DockerCompose;

#[path = "../tests/helpers/mod.rs"]
mod helpers;
use helpers::ShotoverManager;

fn redis(c: &mut Criterion) {
    let mut group = c.benchmark_group("redis");
    group.throughput(criterion::Throughput::Elements(1));


    {
        let mut state = None;
        group.bench_function("active", move |b| {
            b.iter(|| {
                let state = state.get_or_insert_with(|| {
                    let compose = DockerCompose::new("examples/redis-multi/docker-compose.yml")
                        .wait_for_n("Ready to accept connections", 3);
                    let shotover_manager =
                        ShotoverManager::from_topology_file("examples/redis-multi/topology.yaml");
                    BenchResources::new(shotover_manager, compose)
                });
                redis::cmd("SET")
                    .arg("foo")
                    .arg(42)
                    .execute(&mut state.connection);
            })
        });
    }

    {
        let mut state = None;
        group.bench_function("cluster", move |b| {
            b.iter(|| {
                let state = state.get_or_insert_with(|| {
                    let compose = DockerCompose::new("examples/redis-cluster/docker-compose.yml")
                        .wait_for_n("Cluster state changed", 6);
                    let shotover_manager =
                        ShotoverManager::from_topology_file("examples/redis-cluster/topology.yaml");
                    BenchResources::new(shotover_manager, compose)
                });
                redis::cmd("SET")
                    .arg("foo")
                    .arg(42)
                    .execute(&mut state.connection);
            })
        });
    }

    {
        let mut state = None;
        group.bench_function("passthrough", move |b| {
            let state = state.get_or_insert_with(|| {
                let compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml")
                    .wait_for("Ready to accept connections");
                let shotover_manager =
                    ShotoverManager::from_topology_file("examples/redis-passthrough/topology.yaml");
                BenchResources::new(shotover_manager, compose)
            });
            b.iter(|| {
                redis::cmd("SET")
                    .arg("foo")
                    .arg(42)
                    .execute(&mut state.connection);
            })
        });
    }


    {
        let mut state = None;
        group.bench_function("single_tls", move |b| {
            b.iter(|| {
                let state = state.get_or_insert_with(|| {
                    let compose = DockerCompose::new("examples/redis-tls/docker-compose.yml")
                        .wait_for("Ready to accept connections");
                    let shotover_manager =
                        ShotoverManager::from_topology_file("examples/redis-tls/topology.yaml");
                    BenchResources::new(shotover_manager, compose)
                });
                redis::cmd("SET")
                    .arg("foo")
                    .arg(42)
                    .execute(&mut state.connection);
            })
        });
    }

    {
        let mut state = None;
        group.bench_function("cluster_tls", move |b| {
            b.iter(|| {
                let state = state.get_or_insert_with(|| {
                    let compose =
                        DockerCompose::new("examples/redis-cluster-tls/docker-compose.yml")
                            .wait_for_n("Cluster state changed", 6);
                    let shotover_manager = ShotoverManager::from_topology_file(
                        "examples/redis-cluster-tls/topology.yaml",
                    );
                    BenchResources::new(shotover_manager, compose)
                });
                redis::cmd("SET")
                    .arg("foo")
                    .arg(42)
                    .execute(&mut state.connection);
            })
        });
    }
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
