use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use shotover_proxy::message::{Messages, QueryMessage, QueryType};
use shotover_proxy::protocols::RawFrame;
use shotover_proxy::transforms::chain::TransformChain;
use shotover_proxy::transforms::null::Null;
use shotover_proxy::transforms::{Transforms, Wrapper};

fn criterion_benchmark(c: &mut Criterion) {
    let transforms: Vec<Transforms> = vec![Transforms::Null(Null::new_without_request())];

    let mut chain = TransformChain::new_no_shared_state(transforms, "bench".to_string());
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

    c.bench_with_input(
        BenchmarkId::new("input_example", "Empty Message"),
        &wrapper,
        move |b, s| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                let _ = rt.block_on(chain.process_request(s.clone(), "".to_string()));
            })
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
