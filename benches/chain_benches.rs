use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use shotover_proxy::transforms::chain::TransformChain;
use shotover_proxy::transforms::lua::LuaConfig;
use shotover_proxy::transforms::null::Null;
use shotover_proxy::transforms::Transforms;
use shotover_transforms::TopicHolder;
use shotover_transforms::Wrapper;
use shotover_transforms::{Messages, QueryMessage, QueryType};
use shotover_transforms::{RawFrame, TransformsFromConfig};

fn criterion_benchmark(c: &mut Criterion) {
    let transforms: Vec<Transforms> = vec![Box::new(Null::new_without_request())];

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
        RawFrame::NONE,
    ));

    c.bench_with_input(
        BenchmarkId::new("input_example", "Empty Message"),
        &wrapper,
        move |b, s| {
            let mut rt = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                let _ = rt.block_on(chain.process_request(s.clone(), "".to_string()));
            })
        },
    );
}

fn lua_benchmark(c: &mut Criterion) {
    let t_holder = TopicHolder::get_test_holder();

    let lua_t = LuaConfig {
        function_def: "".to_string(),
        // query_filter: Some(String::from(LREQUEST_STRING)),
        // response_filter: Some(String::from(LRESPONSE_STRING)),
        function_name: "".to_string(),
    };

    let lwrapper = Wrapper::new(Messages::new_single_query(
        QueryMessage {
            query_string: "".to_string(),
            namespace: vec![String::from("keyspace"), String::from("old")],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::Read,
            ast: None,
        },
        true,
        RawFrame::NONE,
    ));

    let mut rt = tokio::runtime::Runtime::new().unwrap();

    let transform = rt.block_on(lua_t.get_source(&t_holder)).unwrap();

    let transforms: Vec<Transforms> = vec![transform, Box::new(Null::new())];

    let mut lchain = TransformChain::new_no_shared_state(transforms, String::from("test_chain"));

    c.bench_with_input(
        BenchmarkId::new("lua processing", "Empty Message"),
        &lwrapper,
        move |b, s| {
            b.iter(|| {
                let _ = rt.block_on(lchain.process_request(s.clone(), "".to_string()));
            })
        },
    );
}

criterion_group!(benches, criterion_benchmark, lua_benchmark);
criterion_main!(benches);
