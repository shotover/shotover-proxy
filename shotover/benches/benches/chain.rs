use bytes::Bytes;
use cassandra_protocol::compression::Compression;
use cassandra_protocol::{consistency::Consistency, frame::Version, query::QueryParams};
use criterion::{BatchSize, Criterion, criterion_group};
use hex_literal::hex;
use shotover::codec::CodecState;
use shotover::frame::cassandra::{Tracing, parse_statement_single};
use shotover::frame::{CassandraFrame, CassandraOperation, Frame};
use shotover::frame::{MessageType, ValkeyFrame};
use shotover::message::{Message, MessageIdMap, QueryType};
use shotover::transforms::cassandra::peers_rewrite::CassandraPeersRewrite;
use shotover::transforms::chain::{TransformChain, TransformChainBuilder};
use shotover::transforms::debug::returner::{DebugReturner, Response};
use shotover::transforms::filter::{Filter, QueryTypeFilter};
use shotover::transforms::loopback::Loopback;
use shotover::transforms::null::NullSink;
#[cfg(feature = "alpha-transforms")]
use shotover::transforms::protect::{KeyManagerConfig, ProtectConfig};
use shotover::transforms::query_counter::QueryCounter;
use shotover::transforms::throttling::RequestThrottlingConfig;
use shotover::transforms::valkey::cluster_ports_rewrite::ValkeyClusterPortsRewrite;
use shotover::transforms::{
    ChainState, TransformConfig, TransformContextBuilder, TransformContextConfig,
};

fn criterion_benchmark(c: &mut Criterion) {
    crate::init();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("transform");
    group.noise_threshold(0.2);

    // loopback is the fastest possible transform as it does not even have to drop the received requests
    {
        let chain = TransformChainBuilder::new(vec![Box::<Loopback>::default()], "bench");
        let chain_state = ChainState::new_with_addr(
            vec![Message::from_frame(Frame::Valkey(ValkeyFrame::Null))],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("loopback", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput::new_pre_used(&chain, &chain_state),
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChainBuilder::new(vec![Box::<NullSink>::default()], "bench");
        let chain_state = ChainState::new_with_addr(
            vec![Message::from_frame(Frame::Valkey(ValkeyFrame::Null))],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("nullsink", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput::new_pre_used(&chain, &chain_state),
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
                Box::new(DebugReturner::new(Response::Valkey("a".into()))),
            ],
            "bench",
        );
        let chain_state = ChainState::new_with_addr(
            vec![
                Message::from_frame(Frame::Valkey(ValkeyFrame::Array(vec![
                    ValkeyFrame::BulkString(Bytes::from_static(b"SET")),
                    ValkeyFrame::BulkString(Bytes::from_static(b"foo")),
                    ValkeyFrame::BulkString(Bytes::from_static(b"bar")),
                ]))),
                Message::from_frame(Frame::Valkey(ValkeyFrame::Array(vec![
                    ValkeyFrame::BulkString(Bytes::from_static(b"GET")),
                    ValkeyFrame::BulkString(Bytes::from_static(b"foo")),
                ]))),
            ],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("valkey_filter", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput::new_pre_used(&chain, &chain_state),
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChainBuilder::new(
            vec![
                Box::new(ValkeyClusterPortsRewrite::new(2004)),
                Box::<NullSink>::default(),
            ],
            "bench",
        );
        let chain_state = ChainState::new_with_addr(
            vec![Message::from_frame(Frame::Valkey(ValkeyFrame::Array(
                vec![
                    ValkeyFrame::BulkString(Bytes::from_static(b"SET")),
                    ValkeyFrame::BulkString(Bytes::from_static(b"foo")),
                    ValkeyFrame::BulkString(Bytes::from_static(b"bar")),
                ],
            )))],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("valkey_cluster_ports_rewrite", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput::new_pre_used(&chain, &chain_state),
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
                        up_chain_protocol: MessageType::Valkey,
                    }),
                )
                .unwrap(),
                Box::<NullSink>::default(),
            ],
            "bench",
        );
        let chain_state = ChainState::new_with_addr(
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
                || BenchInput::new_pre_used(&chain, &chain_state),
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

        let chain_state = ChainState::new_with_addr(
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
                || BenchInput::new_pre_used(&chain, &chain_state),
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
                        up_chain_protocol: MessageType::Valkey,
                    }),
                )
                .unwrap(),
                Box::<NullSink>::default(),
            ],
            "bench",
        );

        let chain_state = cassandra_parsed_query(
            "INSERT INTO test_protect_keyspace.unprotected_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 42, true);",
        );

        group.bench_function("cassandra_protect_unprotected", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput::new_pre_used(&chain, &chain_state),
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });

        let chain_state = cassandra_parsed_query(
            "INSERT INTO test_protect_keyspace.protected_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 42, true);",
        );

        group.bench_function("cassandra_protect_protected", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput::new_pre_used(&chain, &chain_state),
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }

    {
        let chain = TransformChainBuilder::new(
            vec![
                Box::new(QueryCounter::new("chain".to_owned())),
                Box::<Loopback>::default(),
            ],
            "bench",
        );
        let chain_state = ChainState::new_with_addr(
            vec![
                Message::from_frame(Frame::Valkey(ValkeyFrame::Array(vec![
                    ValkeyFrame::BulkString(Bytes::from_static(b"SET")),
                    ValkeyFrame::BulkString(Bytes::from_static(b"foo")),
                    ValkeyFrame::BulkString(Bytes::from_static(b"bar")),
                ]))),
                Message::from_frame(Frame::Valkey(ValkeyFrame::Array(vec![
                    ValkeyFrame::BulkString(Bytes::from_static(b"GET")),
                    ValkeyFrame::BulkString(Bytes::from_static(b"foo")),
                ]))),
            ],
            "127.0.0.1:6379".parse().unwrap(),
        );

        group.bench_function("query_counter_fresh", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput::new_fresh(&chain, &chain_state),
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });

        group.bench_function("query_counter_pre_used", |b| {
            b.to_async(&rt).iter_batched(
                || BenchInput::new_pre_used(&chain, &chain_state),
                BenchInput::bench,
                BatchSize::SmallInput,
            )
        });
    }
}

#[cfg(feature = "alpha-transforms")]
fn cassandra_parsed_query(query: &str) -> ChainState<'_> {
    ChainState::new_with_addr(
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
    chain_state: ChainState<'a>,
}

impl<'a> BenchInput<'a> {
    // Setup the bench such that the chain is completely fresh
    fn new_fresh(chain: &TransformChainBuilder, chain_state: &ChainState<'a>) -> Self {
        BenchInput {
            chain: chain.build(TransformContextBuilder::new_test()),
            chain_state: chain_state.clone(),
        }
    }

    // Setup the bench such that the chain has already had the test chain_state passed through it.
    // This ensures that any adhoc setup for that message type has been performed.
    // This is a more realistic bench for typical usage.
    fn new_pre_used(chain: &TransformChainBuilder, chain_state: &ChainState<'a>) -> Self {
        let mut chain = chain.build(TransformContextBuilder::new_test());

        // Run the chain once so we are measuring the chain once each transform has been fully initialized
        futures::executor::block_on(chain.process_request(&mut chain_state.clone())).unwrap();

        BenchInput {
            chain,
            chain_state: chain_state.clone(),
        }
    }

    async fn bench(mut self) -> (Vec<Message>, TransformChain) {
        // Return both the chain itself and the response to avoid measuring the time to drop the values in the benchmark
        let mut chain_state = self.chain_state;
        (
            self.chain.process_request(&mut chain_state).await.unwrap(),
            self.chain,
        )
    }
}

criterion_group!(benches, criterion_benchmark);
