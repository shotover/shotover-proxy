use cassandra_cpp::stmt;
use criterion::{criterion_group, criterion_main, Criterion};

#[path = "./mod.rs"]
mod benches;
use benches::{BenchResources, DockerCompose, ShotoverManager};

fn cassandra(c: &mut Criterion) {
    let mut group = c.benchmark_group("cassandra");
    group.throughput(criterion::Throughput::Elements(1));
    group.noise_threshold(2.0);

    let statement =
        stmt!("INSERT INTO benchmark_keyspace.table_1 (id, x, name) VALUES (1, 11, 'foo');");

    {
        group.bench_with_input(
            "passthrough",
            || {
                let compose =
                    DockerCompose::new("examples/cassandra-passthrough/docker-compose.yml")
                        .wait_for_n_t("Startup complete", 1, 90);
                let shotover_manager = ShotoverManager::from_topology_file(
                    "examples/cassandra-passthrough/topology.yaml",
                );
                BenchResources::new_cassandra(shotover_manager, compose)
            },
            |b, state| {
                b.iter(|| {
                    state.cassandra().execute(&statement).wait().unwrap();
                })
            },
        );
    }
}

criterion_group!(benches, cassandra);
criterion_main!(benches);
