use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use shotover_proxy::message::{Message, QueryMessage, QueryType};
use shotover_proxy::protocols::RawFrame;
use shotover_proxy::transforms::chain::TransformChain;
use shotover_proxy::transforms::null::Null;
use shotover_proxy::transforms::redis::cluster_ports_rewrite::RedisClusterPortsRewrite;
use shotover_proxy::transforms::redis::timestamp_tagging::RedisTimestampTagger;
use shotover_proxy::transforms::{Transforms, Wrapper};

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("transform");

    {
        let chain = TransformChain::new_no_shared_state(
            vec![Transforms::Null(Null::default())],
            "bench".to_string(),
        );
        let wrapper = Wrapper::new_with_chain_name(
            vec![Message::new_query(
                QueryMessage {
                    query_string: "".to_string(),
                    namespace: vec![],
                    primary_key: HashMap::new(),
                    query_values: None,
                    projection: None,
                    query_type: QueryType::Write,
                    ast: None,
                },
                true,
                RawFrame::None,
            )],
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
        use redis_protocol::resp2::prelude::*;
        let chain = TransformChain::new_no_shared_state(
            vec![
                Transforms::RedisTimestampTagger(RedisTimestampTagger::new()),
                Transforms::Null(Null::default()),
            ],
            "bench".to_string(),
        );
        let wrapper = Wrapper::new_with_chain_name(
            vec![Message::new_raw(RawFrame::Redis(Frame::Array(vec![
                Frame::BulkString(b"SET".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"bar".to_vec()),
            ])))],
            chain.name.clone(),
        );

        group.bench_function("redis_timestamp_tagger", |b| {
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
        use redis_protocol::resp2::prelude::*;
        let chain = TransformChain::new_no_shared_state(
            vec![
                Transforms::RedisClusterPortsRewrite(RedisClusterPortsRewrite::new(2004)),
                Transforms::Null(Null::default()),
            ],
            "bench".to_string(),
        );
        let wrapper = Wrapper::new_with_chain_name(
            vec![Message::new_raw(RawFrame::Redis(Frame::Array(vec![
                Frame::BulkString(b"SET".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"bar".to_vec()),
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
