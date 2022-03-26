use cassandra_cpp::{stmt, Session, Statement};
use criterion::{criterion_group, criterion_main, Criterion};
use std::cell::RefCell;
use std::sync::Arc;
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
                format!("protect_local_{}_unencrypted", query.name),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let mut resources = resources.borrow_mut();
                        let connection = &mut resources.as_mut().unwrap().connection;
                        connection.execute(&query.statement).wait().unwrap();
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
                format!("redis_cache_{}_uncached", query.name),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let mut resources = resources.borrow_mut();
                        let connection = &mut resources.as_mut().unwrap().connection;
                        connection.execute(&query.statement).wait().unwrap();
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
                format!("passthrough_no_parse_{}", query.name),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let mut resources = resources.borrow_mut();
                        let connection = &mut resources.as_mut().unwrap().connection;
                        connection.execute(&query.statement).wait().unwrap();
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
                format!("passthrough_parse_request_{}", query.name),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let mut resources = resources.borrow_mut();
                        let connection = &mut resources.as_mut().unwrap().connection;
                        connection.execute(&query.statement).wait().unwrap();
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
                format!("passthrough_parse_response_{}", query.name),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let mut resources = resources.borrow_mut();
                        let connection = &mut resources.as_mut().unwrap().connection;
                        connection.execute(&query.statement).wait().unwrap();
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
            group.bench_with_input(format!("tls_{}", query.name), &resources, |b, resources| {
                b.iter(|| {
                    let mut resources = resources.borrow_mut();
                    let connection = &mut resources.as_mut().unwrap().connection;
                    connection.execute(&query.statement).wait().unwrap();
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
        });
        for query in queries {
            // Benches the case where the message meets the criteria for encryption
            group.bench_with_input(
                format!("protect_local_{}_encrypted", query.name),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let mut resources = resources.borrow_mut();
                        let connection = &mut resources.as_mut().unwrap().connection;
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
                format!("request_throttling_{}", query.name),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        let mut resources = resources.borrow_mut();
                        let connection = &mut resources.as_mut().unwrap().connection;
                        connection.execute(&query.statement).wait().unwrap();
                    })
                },
            );
        }
    }
}

/// The returned Fn will return the result of the provided Fn Wrapped in an `Arc<RefCell<Option<T>>>`
/// The reason for this complicated return type is simply for implementation reasons.
/// Refcell::borrow_mut can always be called on the RefCell (assuming the bench creation logic is single threaded) and the Option will always be Some.
///
/// The returned Fn may be called any amount of times but the provided Fn will only be called 0 or 1 times.
/// The provided Fn is not called until the first time the returned Fn is called.
///
/// This function can be easily pulled out and reused by other benchmarks if needed.
fn new_lazy_shared<T>(create: impl Fn() -> T) -> impl Fn() -> Arc<RefCell<Option<T>>> {
    let resources = Arc::new(RefCell::new(None));
    move || {
        if resources.borrow().is_none() {
            resources.replace(Some(create()));
        }
        resources.clone()
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
    fn new(shotover_topology: &str, compose_file: &str) -> Self {
        let compose = DockerCompose::new(compose_file);
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

    fn new_tls(shotover_topology: &str, compose_file: &str) -> Self {
        let compose = DockerCompose::new(compose_file);
        let shotover_manager = ShotoverManager::from_topology_file(shotover_topology);

        let ca_cert = "example-configs/cassandra-tls/certs/localhost_CA.crt";

        let connection = shotover_manager.cassandra_connection_tls("127.0.0.1", 9042, ca_cert);

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
