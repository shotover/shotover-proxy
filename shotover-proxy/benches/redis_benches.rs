use criterion::{criterion_group, criterion_main, Criterion};
//use test_helpers::docker_compose::DockerCompose;

#[path = "./mod.rs"]
mod benches;
use benches::{BenchResources, DockerCompose, ShotoverManager};

fn redis(c: &mut Criterion) {
    let mut group = c.benchmark_group("redis");
    group.throughput(criterion::Throughput::Elements(1));
    group.noise_threshold(0.2);

    group.bench_with_input(
        "active",
        || {
            let compose = DockerCompose::new("examples/redis-multi/docker-compose.yml")
                .wait_for_n("Ready to accept connections", 3);
            let shotover_manager =
                ShotoverManager::from_topology_file("examples/redis-multi/topology.yaml");
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
            let compose = DockerCompose::new("examples/redis-cluster/docker-compose.yml")
                .wait_for_n("Cluster state changed", 6);
            let shotover_manager =
                ShotoverManager::from_topology_file("examples/redis-cluster/topology.yaml");
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
            let compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml")
                .wait_for("Ready to accept connections");
            let shotover_manager =
                ShotoverManager::from_topology_file("examples/redis-passthrough/topology.yaml");
            BenchResources::new(shotover_manager, compose)
        },
        move |b, state| {
            b.iter(|| {
                redis::cmd("SET").arg("foo").arg(42).execute(state.redis());
            })
        },
    );

    group.bench_with_input(
        "single_tls",
        || {
            let compose = DockerCompose::new("examples/redis-tls/docker-compose.yml")
                .wait_for("Ready to accept connections");
            let shotover_manager =
                ShotoverManager::from_topology_file("examples/redis-tls/topology.yaml");
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
            let compose = DockerCompose::new("examples/redis-cluster-tls/docker-compose.yml")
                .wait_for_n("Cluster state changed", 6);
            let shotover_manager =
                ShotoverManager::from_topology_file("examples/redis-cluster-tls/topology.yaml");
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
