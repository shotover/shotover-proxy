use cassandra_cpp::{PreparedStatement, Session, Statement};
use criterion::{criterion_group, Criterion};
use test_helpers::connection::cassandra::{
    CassandraConnection, CassandraConnectionBuilder, CassandraDriver, ProtocolVersion,
};
use test_helpers::docker_compose::docker_compose;
use test_helpers::docker_compose_runner::DockerCompose;
use test_helpers::lazy::new_lazy_shared;
use test_helpers::shotover_process::{BinProcess, ShotoverProcessBuilder};
use tokio::runtime::Runtime;

struct Query {
    name: &'static str,
    statement: &'static str,
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
                        resources.borrow().as_ref().unwrap().run_query(query);
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
                        resources.borrow().as_ref().unwrap().run_query(query);
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
                        resources.borrow().as_ref().unwrap().run_query(query);
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

            // BenchResources::new_protocol_v5(
            //     "example-configs/cassandra-passthrough/topology.yaml",
            //     "example-configs/cassandra-passthrough/docker-compose.yaml",
            // )
        });
        for query in &queries {
            group.bench_with_input(
                format!("passthrough_no_parse_v5_{}", query),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        resources.borrow().as_ref().unwrap().run_query(query);
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
                        resources.borrow().as_ref().unwrap().run_query(query);
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

            // BenchResources::new_protocol_v5(
            //     "tests/test-configs/cassandra-passthrough-parse-request/topology.yaml",
            //     "tests/test-configs/cassandra-passthrough-parse-request/docker-compose.yaml",
            // )
        });
        for query in &queries {
            group.bench_with_input(
                format!("passthrough_parse_request_v5_{}", query),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        resources.borrow().as_ref().unwrap().run_query(query);
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
                        resources.borrow().as_ref().unwrap().run_query(query);
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

            // BenchResources::new_protocol_v5(
            //     "tests/test-configs/cassandra-passthrough-parse-response/topology.yaml",
            //     "tests/test-configs/cassandra-passthrough-parse-response/docker-compose.yaml",
            // )
        });
        for query in &queries {
            group.bench_with_input(
                format!("passthrough_parse_response_v5_{}", query),
                &resources,
                |b, resources| {
                    b.iter(|| {
                        resources.borrow().as_ref().unwrap().run_query(query);
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
                    resources.borrow().as_ref().unwrap().run_query(query);
                })
            });
        }
    }

    {
        let queries = [
            Query {
                name: "insert",
                statement: "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 42, true);",
            },
            Query {
                name: "select",
                statement: "SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table",
            },
        ];
        let resources = new_lazy_shared(|| {
            let resources = BenchResources::new(
                "example-configs/cassandra-protect-local/topology.yaml",
                "example-configs/cassandra-protect-local/docker-compose.yaml",
            );

            resources.execute("CREATE KEYSPACE test_protect_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
            resources.execute("CREATE TABLE test_protect_keyspace.test_table (pk varchar PRIMARY KEY, cluster varchar, col1 blob, col2 int, col3 boolean);");
            resources.execute("INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'Initial value', 42, true);");

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
                        let resources = resources.as_ref().unwrap();
                        let connection = resources.get_connection();
                        resources
                            .tokio
                            .block_on(connection.execute(query.statement))
                            .unwrap();
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
                        resources.borrow().as_ref().unwrap().run_query(query);
                    })
                },
            );
        }
    }
}

criterion_group!(benches, cassandra);

pub struct BenchResources {
    connection: CassandraConnection,
    shotover: Option<BinProcess>,
    _compose: DockerCompose,
    prepared_statement: Option<PreparedStatement>,
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
        let compose = docker_compose(compose_file);
        let shotover = Some(
            tokio.block_on(ShotoverProcessBuilder::new_with_topology(shotover_topology).start()),
        );

        let connection =
            tokio.block_on(CassandraConnectionBuilder::new("127.0.0.1", 9042, DRIVER).build());

        let mut bench_resources = Self {
            _compose: compose,
            shotover,
            connection,
            prepared_statement: None,
            tokio,
        };
        bench_resources.setup();
        bench_resources
    }

    fn new_tls(shotover_topology: &str, compose_file: &str) -> Self {
        test_helpers::cert::generate_cassandra_test_certs();
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let compose = docker_compose(compose_file);
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
            prepared_statement: None,
            tokio,
        };
        bench_resources.setup();
        bench_resources
    }

    #[allow(unused)]
    fn new_protocol_v5(shotover_topology: &str, compose_file: &str) -> Self {
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let compose = DockerCompose::new(compose_file);
        let shotover = Some(
            tokio.block_on(ShotoverProcessBuilder::new_with_topology(shotover_topology).start()),
        );

        let connection = tokio.block_on(
            CassandraConnectionBuilder::new("127.0.0.1", 9042, DRIVER)
                .with_protocol_version(ProtocolVersion::V5)
                .build(),
        );

        let mut bench_resources = Self {
            _compose: compose,
            shotover,
            connection,
            prepared_statement: None,
            tokio,
        };
        bench_resources.setup();
        bench_resources
    }

    pub fn get_connection(&self) -> &Session {
        self.connection.as_datastax()
    }

    fn get(&self, query: &str) -> Statement {
        let connection = self.connection.as_datastax();
        match query {
            "insert" => connection.statement(
                "INSERT INTO benchmark_keyspace.table_1 (id, x, name) VALUES (1, 11, 'foo');",
            ),
            "select" => connection.statement("SELECT id, x, name FROM benchmark_keyspace.table_1;"),
            "execute" => self.prepared_statement.as_ref().unwrap().bind(),
            _ => todo!("Unknown query specifier: {:?}", query),
        }
    }

    pub fn run_query(&self, query: &str) {
        self.tokio.block_on(self.get(query).execute()).unwrap();
    }

    fn setup(&mut self) {
        self.execute("CREATE KEYSPACE benchmark_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        self.execute(
            "CREATE TABLE benchmark_keyspace.table_1 (id int PRIMARY KEY, x int, name varchar);",
        );
        self.execute(
            "INSERT INTO benchmark_keyspace.table_1 (id, x, name) VALUES (0, 10, 'initial value');",
        );

        let connection = self.connection.as_datastax();

        self.prepared_statement = Some(self
            .tokio
            .block_on(connection.prepare(
                "INSERT INTO benchmark_keyspace.table_1 (id, x, name) VALUES (0, 10, 'new_value');",
            ))
            .unwrap());
    }

    fn execute(&self, query: &str) {
        self.tokio
            .block_on(self.connection.as_datastax().execute(query))
            .unwrap();
    }
}
