use crate::helpers::cassandra::{CassandraConnection, CassandraDriver};
use crate::helpers::ShotoverManager;
use cassandra_cpp::{stmt, Session, Statement};
use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashMap;
use test_helpers::cert::generate_cassandra_test_certs;
use test_helpers::docker_compose::DockerCompose;
use test_helpers::lazy::new_lazy_shared;

#[cfg(feature = "alpha-transforms")]
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
    #[cfg(feature = "alpha-transforms")]
    {
        let resources = new_lazy_shared(|| {
            BenchResources::new(
                "example-configs/cassandra-protect-local/topology.yaml",
                "example-configs/cassandra-protect-local/docker-compose.yml",
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
                "example-configs/cassandra-redis-cache/docker-compose.yml",
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
                "example-configs/cassandra-passthrough/docker-compose.yml",
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

    #[cfg(feature = "alpha-transforms")]
    {
        let resources = new_lazy_shared(|| {
            BenchResources::new(
                "tests/test-configs/cassandra-passthrough-parse-request/topology.yaml",
                "tests/test-configs/cassandra-passthrough-parse-request/docker-compose.yml",
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

    #[cfg(feature = "alpha-transforms")]
    {
        let resources = new_lazy_shared(|| {
            BenchResources::new(
                "tests/test-configs/cassandra-passthrough-parse-response/topology.yaml",
                "tests/test-configs/cassandra-passthrough-parse-response/docker-compose.yml",
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
                "example-configs/cassandra-tls/docker-compose.yml",
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
        let resources = new_lazy_shared(|| {
            let resources = BenchResources::new(
                "example-configs/cassandra-protect-local/topology.yaml",
                "example-configs/cassandra-protect-local/docker-compose.yml",
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
                "example-configs/cassandra-request-throttling/docker-compose.yml",
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
    _compose: DockerCompose,
    _shotover_manager: ShotoverManager,
    connection: CassandraConnection,
    queries: HashMap<String, Statement>,
}

impl BenchResources {
    fn new(shotover_topology: &str, compose_file: &str) -> Self {
        let compose = DockerCompose::new(compose_file);
        let shotover_manager = ShotoverManager::from_topology_file(shotover_topology);

        let connection =
            futures::executor::block_on(CassandraConnection::new("127.0.0.1", 9042, DRIVER));

        let mut bench_resources = Self {
            _compose: compose,
            _shotover_manager: shotover_manager,
            connection,
            queries: HashMap::new(),
        };
        bench_resources.setup();
        bench_resources
    }

    pub fn get_connection(&self) -> &Session {
        self.connection.as_datastax()
    }

    fn new_tls(shotover_topology: &str, compose_file: &str) -> Self {
        generate_cassandra_test_certs();
        let compose = DockerCompose::new(compose_file);
        let shotover_manager = ShotoverManager::from_topology_file(shotover_topology);

        let ca_cert = "example-configs/docker-images/cassandra-tls-4.0.6/certs/localhost_CA.crt";

        let connection = CassandraConnection::new_tls("127.0.0.1", 9042, ca_cert, DRIVER);

        let mut bench_resources = Self {
            _compose: compose,
            _shotover_manager: shotover_manager,
            connection,
            queries: HashMap::new(),
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
