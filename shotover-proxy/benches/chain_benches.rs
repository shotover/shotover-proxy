use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use shotover_proxy::message::{Messages, QueryMessage, QueryType};
use shotover_proxy::protocols::RawFrame;
use shotover_proxy::transforms::chain::TransformChain;
use shotover_proxy::transforms::null::Null;
use shotover_proxy::transforms::redis_transforms::timestamp_tagging::RedisTimestampTagger;
use shotover_proxy::transforms::{Transforms, Wrapper};

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("transform");

    {
        let chain = TransformChain::new_no_shared_state(
            vec![Transforms::Null(Null::new_without_request())],
            "bench".to_string(),
        );
        let wrapper = Wrapper::new(Messages::new_single_query(
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
        ));

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
                Transforms::RedisTimeStampTagger(RedisTimestampTagger::new()),
                Transforms::Null(Null::new_without_request()),
            ],
            "bench".to_string(),
        );
        let wrapper = Wrapper::new(Messages::new_single_bypass(RawFrame::Redis(Frame::Array(
            vec![
                Frame::BulkString(b"SET".to_vec()),
                Frame::BulkString(b"foo".to_vec()),
                Frame::BulkString(b"bar".to_vec()),
            ],
        ))));

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
