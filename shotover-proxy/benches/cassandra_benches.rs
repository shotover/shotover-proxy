use cassandra_cpp::{stmt, Session};
use criterion::{criterion_group, criterion_main, Criterion};
use test_helpers::docker_compose::DockerCompose;

#[path = "../tests/helpers/mod.rs"]
mod helpers;
use helpers::ShotoverManager;

fn cassandra(c: &mut Criterion) {
    let mut group = c.benchmark_group("cassandra");
    group.throughput(criterion::Throughput::Elements(1));
    group.noise_threshold(0.2);

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
                BenchResources::new(shotover_manager, compose)
            },
            |b, state| {
                b.iter(|| {
                    state.connection.execute(&statement).wait().unwrap();
                })
            },
        );
    }
}

criterion_group!(benches, cassandra);
criterion_main!(benches);

pub struct BenchResources {
    _compose: DockerCompose,
    _shotover_manager: ShotoverManager,
    connection: Session,
}

impl BenchResources {
    #[allow(unused)]
    pub fn new(shotover_manager: ShotoverManager, compose: DockerCompose) -> Self {
        let connection = shotover_manager.cassandra_connection("127.0.0.1", 9042);

        let mut statement = stmt!("CREATE KEYSPACE benchmark_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        connection.execute(&statement).wait().unwrap();

        statement = stmt!(
            "CREATE TABLE benchmark_keyspace.table_1 (id int PRIMARY KEY, x int, name varchar);"
        );
        connection.execute(&statement).wait().unwrap();

        Self {
            _compose: compose,
            _shotover_manager: shotover_manager,
            connection,
        }
    }
}
