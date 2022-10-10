use bytes::Bytes;
use cassandra_protocol::{
    compression::Compression,
    consistency::Consistency,
    frame::{Flags, Version},
    query::QueryParams,
};
use criterion::{criterion_group, BatchSize, Criterion};
use hex_literal::hex;
use shotover_proxy::frame::cassandra::parse_statement_single;
use shotover_proxy::frame::RedisFrame;
use shotover_proxy::frame::{CassandraFrame, CassandraOperation, Frame, MessageType};
use shotover_proxy::message::{Message, QueryType};
use shotover_proxy::transforms::cassandra::peers_rewrite::CassandraPeersRewrite;
use shotover_proxy::transforms::chain::TransformChain;
use shotover_proxy::transforms::debug::returner::{DebugReturner, Response};
use shotover_proxy::transforms::filter::QueryTypeFilter;
use shotover_proxy::transforms::null::Null;
use shotover_proxy::transforms::protect::{KeyManagerConfig, ProtectConfig};
use shotover_proxy::transforms::redis::cluster_ports_rewrite::RedisClusterPortsRewrite;
use shotover_proxy::transforms::redis::timestamp_tagging::RedisTimestampTagger;
use shotover_proxy::transforms::throttling::RequestThrottlingConfig;
use shotover_proxy::transforms::{Transforms, Wrapper};

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("transform");
    group.noise_threshold(0.2);

    {
        let chain =
            TransformChain::new(vec![Transforms::Null(Null::default())], "bench".to_string());
        let wrapper = Wrapper::new_with_chain_name(
            vec![Message::from_frame(Frame::None)],
            chain.name.clone(),
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("null", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.clone(),
                    wrapper: wrapper.clone(),
                    client_details: "".into(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChain::new(
            vec![
                Transforms::QueryTypeFilter(QueryTypeFilter {
                    filter: QueryType::Read,
                }),
                Transforms::DebugReturner(DebugReturner::new(Response::Redis("a".into()))),
            ],
            "bench".to_string(),
        );
        let wrapper = Wrapper::new_with_chain_name(
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
            chain.name.clone(),
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("redis_filter", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.clone(),
                    wrapper: wrapper.clone(),
                    client_details: "".into(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChain::new(
            vec![
                Transforms::RedisTimestampTagger(RedisTimestampTagger::new()),
                Transforms::DebugReturner(DebugReturner::new(Response::Message(vec![
                    Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                        RedisFrame::BulkString(Bytes::from_static(b"1")), // real frame
                        RedisFrame::BulkString(Bytes::from_static(b"1")), // timestamp
                    ]))),
                ]))),
            ],
            "bench".to_string(),
        );
        let wrapper_set = Wrapper::new_with_chain_name(
            vec![Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                RedisFrame::BulkString(Bytes::from_static(b"SET")),
                RedisFrame::BulkString(Bytes::from_static(b"foo")),
                RedisFrame::BulkString(Bytes::from_static(b"bar")),
            ])))],
            chain.name.clone(),
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("redis_timestamp_tagger_untagged", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.clone(),
                    wrapper: wrapper_set.clone(),
                    client_details: "".into(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });

        let wrapper_get = Wrapper::new_with_chain_name(
            vec![Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                RedisFrame::BulkString(Bytes::from_static(b"GET")),
                RedisFrame::BulkString(Bytes::from_static(b"foo")),
            ])))],
            chain.name.clone(),
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("redis_timestamp_tagger_tagged", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.clone(),
                    wrapper: wrapper_get.clone(),
                    client_details: "".into(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChain::new(
            vec![
                Transforms::RedisClusterPortsRewrite(RedisClusterPortsRewrite::new(2004)),
                Transforms::Null(Null::default()),
            ],
            "bench".to_string(),
        );
        let wrapper = Wrapper::new_with_chain_name(
            vec![Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                RedisFrame::BulkString(Bytes::from_static(b"SET")),
                RedisFrame::BulkString(Bytes::from_static(b"foo")),
                RedisFrame::BulkString(Bytes::from_static(b"bar")),
            ])))],
            chain.name.clone(),
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("redis_cluster_ports_rewrite", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.clone(),
                    wrapper: wrapper.clone(),
                    client_details: "".into(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChain::new(
            vec![
                rt.block_on(
                    RequestThrottlingConfig {
                        // an absurdly large value is given so that all messages will pass through
                        max_requests_per_second: std::num::NonZeroU32::new(100_000_000).unwrap(),
                    }
                    .get_transform(),
                )
                .unwrap(),
                Transforms::Null(Null::default()),
            ],
            "bench".to_string(),
        );
        let wrapper = Wrapper::new_with_chain_name(
            vec![Message::from_bytes(
                Bytes::from(
                    // a simple select query
                    hex!(
                        "0400000307000000350000002e53454c454354202a2046524f4d20737973
                        74656d2e6c6f63616c205748455245206b6579203d20276c6f63616c27000100"
                    )
                    .to_vec(),
                ),
                MessageType::Cassandra,
            )],
            chain.name.clone(),
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("cassandra_request_throttling_unparsed", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.clone(),
                    wrapper: wrapper.clone(),
                    client_details: "".into(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChain::new(
            vec![
                Transforms::CassandraPeersRewrite(CassandraPeersRewrite::new(9042)),
                Transforms::Null(Null::default()),
            ],
            "bench".into(),
        );

        let wrapper = Wrapper::new_with_chain_name(
            vec![Message::from_bytes(
                CassandraFrame {
                    version: Version::V4,
                    flags: Flags::default(),
                    stream_id: 0,
                    tracing_id: None,
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
                .encode()
                .encode_with(Compression::None)
                .unwrap()
                .into(),
                MessageType::Cassandra,
            )],
            "bench".into(),
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("cassandra_rewrite_peers_passthrough", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.clone(),
                    wrapper: wrapper.clone(),
                    client_details: "".into(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChain::new(
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
                    .get_transform(),
                )
                .unwrap(),
                Transforms::Null(Null::default()),
            ],
            "bench".into(),
        );

        let wrapper = cassandra_parsed_query(
            "INSERT INTO test_protect_keyspace.unprotected_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 42, true);"
        );

        group.bench_function("cassandra_protect_unprotected", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput {
                    chain: chain.clone(),
                    wrapper: wrapper.clone(),
                    client_details: "".into(),
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
                    chain: chain.clone(),
                    wrapper: wrapper.clone(),
                    client_details: "".into(),
                },
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }
}

fn cassandra_parsed_query(query: &str) -> Wrapper {
    Wrapper::new_with_chain_name(
        vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            flags: Flags::default(),
            stream_id: 0,
            tracing_id: None,
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
        "bench".into(),
        "127.0.0.1:6379".parse().unwrap(),
    )
}

struct BenchInput<'a> {
    chain: TransformChain,
    wrapper: Wrapper<'a>,
    client_details: String,
}

impl<'a> BenchInput<'a> {
    async fn bench(mut self) {
        self.chain
            .process_request(self.wrapper, self.client_details)
            .await
            .unwrap();
    }
}

criterion_group!(benches, criterion_benchmark);
