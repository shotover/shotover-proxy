use bytes::Bytes;
use cassandra_protocol::compression::Compression;
use cassandra_protocol::{consistency::Consistency, frame::Version, query::QueryParams};
use criterion::{criterion_group, BatchSize, Criterion};
use hex_literal::hex;
use shotover::codec::CodecState;
use shotover::frame::cassandra::{parse_statement_single, Tracing};
use shotover::frame::{CassandraFrame, CassandraOperation, Frame};
use shotover::frame::{MessageType, RedisFrame};
use shotover::message::{Message, MessageIdMap, QueryType};
use shotover::transforms::cassandra::peers_rewrite::CassandraPeersRewrite;
use shotover::transforms::chain::{TransformChain, TransformChainBuilder};
use shotover::transforms::debug::returner::{DebugReturner, Response};
use shotover::transforms::filter::{Filter, QueryTypeFilter};
use shotover::transforms::loopback::Loopback;
use shotover::transforms::null::NullSink;
#[cfg(feature = "alpha-transforms")]
use shotover::transforms::protect::{KeyManagerConfig, ProtectConfig};
use shotover::transforms::redis::cluster_ports_rewrite::RedisClusterPortsRewrite;
use shotover::transforms::redis::timestamp_tagging::RedisTimestampTagger;
use shotover::transforms::throttling::RequestThrottlingConfig;
use shotover::transforms::{TransformConfig, TransformContextConfig, Wrapper};

fn criterion_benchmark(c: &mut Criterion) {
    crate::init();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("transform");
    group.noise_threshold(0.2);

    // loopback is the fastest possible transform as it does not even have to drop the received requests
    {
        let chain = TransformChainBuilder::new(vec![Box::<Loopback>::default()], "bench");
        let wrapper = Wrapper::new_with_addr(
            vec![Message::from_frame(Frame::Redis(RedisFrame::Null))],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("loopback", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.build(),
                    wrapper: wrapper.clone(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChainBuilder::new(vec![Box::<NullSink>::default()], "bench");
        let wrapper = Wrapper::new_with_addr(
            vec![Message::from_frame(Frame::Redis(RedisFrame::Null))],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("nullsink", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.build(),
                    wrapper: wrapper.clone(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChainBuilder::new(
            vec![
                Box::new(QueryTypeFilter {
                    filter: Filter::DenyList(vec![QueryType::Read]),
                    filtered_requests: MessageIdMap::default(),
                }),
                Box::new(DebugReturner::new(Response::Redis("a".into()))),
            ],
            "bench",
        );
        let wrapper = Wrapper::new_with_addr(
            vec![
                Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                    RedisFrame::BulkString(Bytes::from_static(b"SET")),
                    RedisFrame::BulkString(Bytes::from_static(b"foo")),
                    RedisFrame::BulkString(Bytes::from_static(b"bar")),
                ]))),
                Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                    RedisFrame::BulkString(Bytes::from_static(b"GET")),
                    RedisFrame::BulkString(Bytes::from_static(b"foo")),
                ]))),
            ],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("redis_filter", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.build(),
                    wrapper: wrapper.clone(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChainBuilder::new(
            vec![
                Box::new(RedisTimestampTagger::new()),
                Box::new(DebugReturner::new(Response::Message(Message::from_frame(
                    Frame::Redis(RedisFrame::Array(vec![
                        RedisFrame::BulkString(Bytes::from_static(b"1")), // real frame
                        RedisFrame::BulkString(Bytes::from_static(b"1")), // timestamp
                    ])),
                )))),
            ],
            "bench",
        );
        let wrapper_set = Wrapper::new_with_addr(
            vec![Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                RedisFrame::BulkString(Bytes::from_static(b"SET")),
                RedisFrame::BulkString(Bytes::from_static(b"foo")),
                RedisFrame::BulkString(Bytes::from_static(b"bar")),
            ])))],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("redis_timestamp_tagger_untagged", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.build(),
                    wrapper: wrapper_set.clone(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });

        let wrapper_get = Wrapper::new_with_addr(
            vec![Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                RedisFrame::BulkString(Bytes::from_static(b"GET")),
                RedisFrame::BulkString(Bytes::from_static(b"foo")),
            ])))],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("redis_timestamp_tagger_tagged", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.build(),
                    wrapper: wrapper_get.clone(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChainBuilder::new(
            vec![
                Box::new(RedisClusterPortsRewrite::new(2004)),
                Box::<NullSink>::default(),
            ],
            "bench",
        );
        let wrapper = Wrapper::new_with_addr(
            vec![Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                RedisFrame::BulkString(Bytes::from_static(b"SET")),
                RedisFrame::BulkString(Bytes::from_static(b"foo")),
                RedisFrame::BulkString(Bytes::from_static(b"bar")),
            ])))],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("redis_cluster_ports_rewrite", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.build(),
                    wrapper: wrapper.clone(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChainBuilder::new(
            vec![
                rt.block_on(
                    RequestThrottlingConfig {
                        // an absurdly large value is given so that all messages will pass through
                        max_requests_per_second: std::num::NonZeroU32::new(100_000_000).unwrap(),
                    }
                    .get_builder(TransformContextConfig {
                        chain_name: "".into(),
                        protocol: MessageType::Redis,
                    }),
                )
                .unwrap(),
                Box::<NullSink>::default(),
            ],
            "bench",
        );
        let wrapper = Wrapper::new_with_addr(
            vec![Message::from_bytes(
                Bytes::from(
                    // a simple select query
                    hex!(
                        "0400000307000000350000002e53454c454354202a2046524f4d20737973
                        74656d2e6c6f63616c205748455245206b6579203d20276c6f63616c27000100"
                    )
                    .to_vec(),
                ),
                CodecState::Cassandra {
                    compression: Compression::None,
                },
            )],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("cassandra_request_throttling_unparsed", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.build(),
                    wrapper: wrapper.clone(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChainBuilder::new(
            vec![
                Box::new(CassandraPeersRewrite::new(9042)),
                Box::<NullSink>::default(),
            ],
            "bench",
        );

        let wrapper = Wrapper::new_with_addr(
            vec![Message::from_bytes(
                CassandraFrame {
                    version: Version::V4,
                    stream_id: 0,
                    tracing: Tracing::Request(false),
                    warnings: vec![],
                    operation: CassandraOperation::Query {
                        query: Box::new(parse_statement_single(
                            "INSERT INTO foo (z, v) VALUES (1, 123)",
                        )),
                        params: Box::new(QueryParams {
                            consistency: Consistency::One,
                            with_names: false,
                            values: None,
                            page_size: Some(5000),
                            paging_state: None,
                            serial_consistency: None,
                            timestamp: Some(1643855761086585),
                            keyspace: None,
                            now_in_seconds: None,
                        }),
                    },
                }
                .encode(Compression::None)
                .into(),
                CodecState::Cassandra {
                    compression: Compression::None,
                },
            )],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("cassandra_rewrite_peers_passthrough", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.build(),
                    wrapper: wrapper.clone(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    #[cfg(feature = "alpha-transforms")]
    {
        let chain = TransformChainBuilder::new(
            vec![
                rt.block_on(
                    ProtectConfig {
                        keyspace_table_columns: [(
                            "test_protect_keyspace".to_string(),
                            [("protected_table".to_string(), vec!["col1".to_string()])]
                                .into_iter()
                                .collect(),
                        )]
                        .into_iter()
                        .collect(),
                        key_manager: KeyManagerConfig::Local {
                            kek: "Ht8M1nDO/7fay+cft71M2Xy7j30EnLAsA84hSUMCm1k=".to_string(),
                            kek_id: "".to_string(),
                        },
                    }
                    .get_builder(TransformContextConfig {
                        chain_name: "".into(),
                        protocol: MessageType::Redis,
                    }),
                )
                .unwrap(),
                Box::<NullSink>::default(),
            ],
            "bench",
        );

        let wrapper = cassandra_parsed_query(
            "INSERT INTO test_protect_keyspace.unprotected_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 42, true);"
        );

        group.bench_function("cassandra_protect_unprotected", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.build(),
                    wrapper: wrapper.clone(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });

        let wrapper = cassandra_parsed_query(
            "INSERT INTO test_protect_keyspace.protected_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 42, true);"
        );

        group.bench_function("cassandra_protect_protected", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.build(),
                    wrapper: wrapper.clone(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }
}

#[cfg(feature = "alpha-transforms")]
fn cassandra_parsed_query(query: &str) -> Wrapper {
    Wrapper::new_with_addr(
        vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing: Tracing::Request(false),
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single(query)),
                params: Box::new(QueryParams {
                    consistency: Consistency::One,
                    with_names: false,
                    values: None,
                    page_size: Some(5000),
                    paging_state: None,
                    serial_consistency: None,
                    timestamp: Some(1643855761086585),
                    keyspace: None,
                    now_in_seconds: None,
                }),
            },
        }))],
        "127.0.0.1:6379".parse().unwrap(),
    )
}

struct BenchInput<'a> {
    chain: TransformChain,
    wrapper: Wrapper<'a>,
}

impl<'a> BenchInput<'a> {
    async fn bench(mut self) -> (Vec<Message>, TransformChain) {
        // Return both the chain itself and the response to avoid measuring the time to drop the values in the benchmark
        (
            self.chain.process_request(self.wrapper).await.unwrap(),
            self.chain,
        )
    }
}

criterion_group!(benches, criterion_benchmark);
