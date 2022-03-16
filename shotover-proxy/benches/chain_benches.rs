use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use hex_literal::hex;
use shotover_proxy::frame::RedisFrame;
use shotover_proxy::frame::{Frame, MessageType};
use shotover_proxy::message::{Message, QueryType};
use shotover_proxy::transforms::chain::TransformChain;
use shotover_proxy::transforms::debug::returner::{DebugReturner, Response};
use shotover_proxy::transforms::filter::QueryTypeFilter;
use shotover_proxy::transforms::null::Null;
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
criterion_main!(benches);
