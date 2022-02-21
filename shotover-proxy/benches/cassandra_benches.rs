use cassandra_cpp::{stmt, Session, Statement};
use criterion::{criterion_group, criterion_main, Criterion};
use test_helpers::docker_compose::DockerCompose;

#[path = "../tests/helpers/mod.rs"]
mod helpers;
use helpers::ShotoverManager;

struct Query {
    name: &'static str,
    statement: Statement,
}

fn cassandra(c: &mut Criterion) {
    let mut group = c.benchmark_group("cassandra");
    group.throughput(criterion::Throughput::Elements(1));
    group.noise_threshold(0.2);

    let queries = [
        Query {
            name: "insert",
            statement: stmt!(
                "INSERT INTO benchmark_keyspace.table_1 (id, x, name) VALUES (1, 11, 'foo');"
            ),
        },
        Query {
            name: "select",
            statement: stmt!("SELECT id, x, name FROM benchmark_keyspace.table_1;"),
        },
    ];
    for query in queries {
        // Benches the case where the message does not meet the criteria for encryption
        #[cfg(feature = "alpha-transforms")]
        group.bench_with_input(
            format!("protect_local_{}_unencrypted", query.name),
            || {
                BenchResources::new(
                    "examples/cassandra-protect-local/topology.yaml",
                    "examples/cassandra-protect-local/docker-compose.yml",
                )
            },
            |b, state| {
                b.iter(|| {
                    state.connection.execute(&query.statement).wait().unwrap();
                })
            },
        );

        // Benches the case where the message does not meet the criteria for caching
        group.bench_with_input(
            format!("redis_cache_{}_uncached", query.name),
            || {
                BenchResources::new(
                    "examples/cassandra-redis-cache/topology.yaml",
                    "examples/cassandra-redis-cache/docker-compose.yml",
                )
            },
            |b, state| {
                b.iter(|| {
                    state.connection.execute(&query.statement).wait().unwrap();
                })
            },
        );

        group.bench_with_input(
            format!("passthrough_{}", query.name),
            || {
                BenchResources::new(
                    "examples/cassandra-passthrough/topology.yaml",
                    "examples/cassandra-passthrough/docker-compose.yml",
                )
            },
            |b, state| {
                b.iter(|| {
                    state.connection.execute(&query.statement).wait().unwrap();
                })
            },
        );

        group.bench_with_input(
            format!("passthrough_tls_{}", query.name),
            || {
                BenchResources::new_tls(
                    "examples/cassandra-passthrough-tls/topology.yaml",
                    "examples/cassandra-passthrough-tls/docker-compose.yml",
                )
            },
            |b, state| {
                b.iter(|| {
                    state.connection.execute(&query.statement).wait().unwrap();
                })
            },
        );
    }

    #[cfg(feature = "alpha-transforms")]
    {
        let queries = [
            Query {
                name: "insert",
                statement: stmt!("INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 42, true);"),
            },
            Query {
                name: "select",
                statement: stmt!("SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table"),
            },
        ];
        for query in queries {
            // Benches the case where the message meets the criteria for encryption
            group.bench_with_input(
                format!("protect_local_{}_encrypted", query.name),
                || {
                    let resources = BenchResources::new(
                        "examples/cassandra-protect-local/topology.yaml",
                        "examples/cassandra-protect-local/docker-compose.yml"
                    );

                    resources
                        .connection
                        .execute(&stmt!(
                            "CREATE KEYSPACE test_protect_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
                        ))
                        .wait()
                        .unwrap();
                    resources
                        .connection
                        .execute(&stmt!(
                            "CREATE TABLE test_protect_keyspace.test_table (pk varchar PRIMARY KEY, cluster varchar, col1 varchar, col2 int, col3 boolean);"
                        ))
                        .wait()
                        .unwrap();
                    resources
                        .connection
                        .execute(&stmt!(
                            "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'Initial value', 42, true);"
                        ))
                        .wait()
                        .unwrap();

                    resources
                },
                |b, state| {
                    b.iter(|| {
                        state.connection.execute(&query.statement).wait().unwrap();
                    })
                },
            );
        }
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
    pub fn new(shotover_topology: &str, compose_file: &str) -> Self {
        let compose = DockerCompose::new(compose_file).wait_for_n_t("Startup complete", 1, 90);
        let shotover_manager = ShotoverManager::from_topology_file(shotover_topology);

        let connection = shotover_manager.cassandra_connection("127.0.0.1", 9042);

        let bench_resources = Self {
            _compose: compose,
            _shotover_manager: shotover_manager,
            connection,
        };
        bench_resources.setup();
        bench_resources
    }

    #[allow(unused)]
    pub fn new_tls(shotover_topology: &str, compose_file: &str) -> Self {
        let compose = DockerCompose::new(compose_file).wait_for_n_t("Startup complete", 1, 90);
        let shotover_manager = ShotoverManager::from_topology_file(shotover_topology);

        let ca_cert = "examples/cassandra-passthrough-tls/certs/localhost_CA.crt";

        let connection = shotover_manager.cassandra_connection_tls(
            "127.0.0.1",
            9042,
            ca_cert,
            "cassandra",
            "cassandra",
        );

        let bench_resources = Self {
            _compose: compose,
            _shotover_manager: shotover_manager,
            connection,
        };
        bench_resources.setup();
        bench_resources
    }

    fn setup(&self) {
        self.connection
            .execute(&stmt!(
                "CREATE KEYSPACE benchmark_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
            ))
            .wait().unwrap();

        self.connection
            .execute(&stmt!(
                "CREATE TABLE benchmark_keyspace.table_1 (id int PRIMARY KEY, x int, name varchar);"
            ))
            .wait()
            .unwrap();

        self.connection
            .execute(&stmt!(
                "INSERT INTO benchmark_keyspace.table_1 (id, x, name) VALUES (0, 10, 'initial value');"
            ))
            .wait()
            .unwrap();
    }
}
