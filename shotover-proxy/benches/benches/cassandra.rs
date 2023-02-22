use cassandra_cpp::{stmt, Session, Statement};
use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashMap;
use test_helpers::cert::generate_cassandra_test_certs;
use test_helpers::connection::cassandra::{
    CassandraConnection, CassandraConnectionBuilder, CassandraDriver,
};
use test_helpers::docker_compose::DockerCompose;
use test_helpers::lazy::new_lazy_shared;
use test_helpers::shotover_process::{BinProcess, ShotoverProcessBuilder};
use tokio::runtime::Runtime;

struct Query {
    name: &'static str,
    statement: Statement,
}

const DRIVER: CassandraDriver = CassandraDriver::Datastax;

fn cassandra(c: &mut Criterion) {
    let mut group = c.benchmark_group("cassandra");
    group.throughput(criterion::Throughput::Elements(1));
    group.noise_threshold(0.2);

    let queries = ["insert", "select", "execute"];

    // Benches the case where the message does not meet the criteria for encryption
    {
        let resources = new_lazy_shared(|| {
            BenchResources::new(
                "example-configs/cassandra-protect-local/topology.yaml",
                "example-configs/cassandra-protect-local/docker-compose.yaml",
            )
        });
        for query in &queries {
            group.bench_with_input(
                format!("protect_local_{}_unencrypted", query),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let resources = resources.borrow();
                        let connection = resources.as_ref().unwrap().get_connection();
                        connection
                            .execute(resources.as_ref().unwrap().get(query))
                            .wait()
                            .unwrap();
                    })
                },
            );
        }
    }

    {
        let resources = new_lazy_shared(|| {
            BenchResources::new(
                "example-configs/cassandra-redis-cache/topology.yaml",
                "example-configs/cassandra-redis-cache/docker-compose.yaml",
            )
        });
        // Benches the case where the message does not meet the criteria for caching
        for query in &queries {
            group.bench_with_input(
                format!("redis_cache_{}_uncached", query),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let resources = resources.borrow();
                        let connection = resources.as_ref().unwrap().get_connection();
                        connection
                            .execute(resources.as_ref().unwrap().get(query))
                            .wait()
                            .unwrap();
                    })
                },
            );
        }
    }

    {
        let resources = new_lazy_shared(|| {
            BenchResources::new(
                "example-configs/cassandra-passthrough/topology.yaml",
                "example-configs/cassandra-passthrough/docker-compose.yaml",
            )
        });
        for query in &queries {
            group.bench_with_input(
                format!("passthrough_no_parse_{}", query),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let resources = resources.borrow();
                        let connection = resources.as_ref().unwrap().get_connection();
                        connection
                            .execute(resources.as_ref().unwrap().get(query))
                            .wait()
                            .unwrap();
                    })
                },
            );
        }
    }

    {
        let resources = new_lazy_shared(|| {
            BenchResources::new(
                "tests/test-configs/cassandra-passthrough-parse-request/topology.yaml",
                "tests/test-configs/cassandra-passthrough-parse-request/docker-compose.yaml",
            )
        });
        for query in &queries {
            group.bench_with_input(
                format!("passthrough_parse_request_{}", query),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let resources = resources.borrow();
                        let connection = resources.as_ref().unwrap().get_connection();
                        connection
                            .execute(resources.as_ref().unwrap().get(query))
                            .wait()
                            .unwrap();
                    })
                },
            );
        }
    }

    {
        let resources = new_lazy_shared(|| {
            BenchResources::new(
                "tests/test-configs/cassandra-passthrough-parse-response/topology.yaml",
                "tests/test-configs/cassandra-passthrough-parse-response/docker-compose.yaml",
            )
        });
        for query in &queries {
            group.bench_with_input(
                format!("passthrough_parse_response_{}", query),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let resources = resources.borrow();
                        let connection = resources.as_ref().unwrap().get_connection();
                        connection
                            .execute(resources.as_ref().unwrap().get(query))
                            .wait()
                            .unwrap();
                    })
                },
            );
        }
    }

    {
        let resources = new_lazy_shared(|| {
            BenchResources::new_tls(
                "example-configs/cassandra-tls/topology.yaml",
                "example-configs/cassandra-tls/docker-compose.yaml",
            )
        });
        for query in &queries {
            group.bench_with_input(format!("tls_{}", query), &resources, |b, resources| {
                b.iter(|| {
                    let resources = resources.borrow();
                    let connection = resources.as_ref().unwrap().get_connection();
                    connection
                        .execute(resources.as_ref().unwrap().get(query))
                        .wait()
                        .unwrap();
                })
            });
        }
    }

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
        let resources = new_lazy_shared(|| {
            let resources = BenchResources::new(
                "example-configs/cassandra-protect-local/topology.yaml",
                "example-configs/cassandra-protect-local/docker-compose.yaml",
            );

            resources
                .get_connection()
                .execute(&stmt!(
                    "CREATE KEYSPACE test_protect_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
                ))
                .wait()
                .unwrap();
            resources
                .get_connection()
                .execute(&stmt!(
                    "CREATE TABLE test_protect_keyspace.test_table (pk varchar PRIMARY KEY, cluster varchar, col1 blob, col2 int, col3 boolean);"
                ))
                .wait()
                .unwrap();
            resources
                .get_connection()
                .execute(&stmt!(
                    "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'Initial value', 42, true);"
                ))
                .wait()
                .unwrap();

            resources
        });
        for query in queries {
            // Benches the case where the message meets the criteria for encryption
            group.bench_with_input(
                format!("protect_local_{}_encrypted", query.name),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let resources = resources.borrow();
                        let connection = resources.as_ref().unwrap().get_connection();
                        connection.execute(&query.statement).wait().unwrap();
                    })
                },
            );
        }
    }

    {
        let resources = new_lazy_shared(|| {
            BenchResources::new(
                "example-configs/cassandra-request-throttling/topology.yaml",
                "example-configs/cassandra-request-throttling/docker-compose.yaml",
            )
        });
        for query in &queries {
            group.bench_with_input(
                format!("request_throttling_{}", query),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let resources = resources.borrow();
                        let connection = resources.as_ref().unwrap().get_connection();
                        connection
                            .execute(resources.as_ref().unwrap().get(query))
                            .wait()
                            .unwrap();
                    })
                },
            );
        }
    }
}

criterion_group!(benches, cassandra);
criterion_main!(benches);

pub struct BenchResources {
    connection: CassandraConnection,
    shotover: Option<BinProcess>,
    _compose: DockerCompose,
    queries: HashMap<String, Statement>,
    tokio: Runtime,
}

impl Drop for BenchResources {
    fn drop(&mut self) {
        self.tokio.block_on(
            self.shotover
                .take()
                .unwrap()
                .shutdown_and_then_consume_events(&[]),
        );
    }
}

impl BenchResources {
    fn new(shotover_topology: &str, compose_file: &str) -> Self {
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let compose = DockerCompose::new(compose_file);
        let shotover = Some(
            tokio.block_on(ShotoverProcessBuilder::new_with_topology(shotover_topology).start()),
        );

        let connection =
            tokio.block_on(CassandraConnectionBuilder::new("127.0.0.1", 9042, DRIVER).build());

        let mut bench_resources = Self {
            _compose: compose,
            shotover,
            connection,
            queries: HashMap::new(),
            tokio,
        };
        bench_resources.setup();
        bench_resources
    }

    pub fn get_connection(&self) -> &Session {
        self.connection.as_datastax()
    }

    fn new_tls(shotover_topology: &str, compose_file: &str) -> Self {
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        generate_cassandra_test_certs();
        let compose = DockerCompose::new(compose_file);
        let shotover = Some(
            tokio.block_on(ShotoverProcessBuilder::new_with_topology(shotover_topology).start()),
        );

        let ca_cert = "example-configs/docker-images/cassandra-tls-4.0.6/certs/localhost_CA.crt";

        let connection = tokio.block_on(
            CassandraConnectionBuilder::new("127.0.0.1", 9042, DRIVER)
                .with_tls(ca_cert)
                .build(),
        );

        let mut bench_resources = Self {
            _compose: compose,
            shotover,
            connection,
            queries: HashMap::new(),
            tokio,
        };
        bench_resources.setup();
        bench_resources
    }

    pub fn get(&self, query: &str) -> &Statement {
        self.queries.get(query).unwrap()
    }

    fn setup(&mut self) {
        let create_keyspace = stmt!(
            "CREATE KEYSPACE benchmark_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
        );

        let create_table = stmt!(
            "CREATE TABLE benchmark_keyspace.table_1 (id int PRIMARY KEY, x int, name varchar);"
        );

        let insert = stmt!(
            "INSERT INTO benchmark_keyspace.table_1 (id, x, name) VALUES (0, 10, 'initial value');"
        );

        self.connection
            .as_datastax()
            .execute(&create_keyspace)
            .wait()
            .unwrap();

        self.connection
            .as_datastax()
            .execute(&create_table)
            .wait()
            .unwrap();

        self.connection
            .as_datastax()
            .execute(&insert)
            .wait()
            .unwrap();

        let prepared_statement = self
            .connection
            .as_datastax()
            .prepare(
                "INSERT INTO benchmark_keyspace.table_1 (id, x, name) VALUES (0, 10, 'new_value');",
            )
            .unwrap()
            .wait()
            .unwrap()
            .bind();

        self.queries = [
            (
                "insert".into(),
                stmt!(
                    "INSERT INTO benchmark_keyspace.table_1 (id, x, name) VALUES (1, 11, 'foo');"
                ),
            ),
            (
                "select".into(),
                stmt!("SELECT id, x, name FROM benchmark_keyspace.table_1;"),
            ),
            ("execute".into(), prepared_statement),
        ]
        .into_iter()
        .collect();
    }
}
