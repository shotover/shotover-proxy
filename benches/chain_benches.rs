use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use instaproxy::config::topology::TopicHolder;
use instaproxy::message::{Message, QueryMessage, QueryType};
use instaproxy::protocols::RawFrame;
use instaproxy::transforms::chain::{TransformChain, Wrapper};
use instaproxy::transforms::lua::LuaConfig;
use instaproxy::transforms::null::Null;
use instaproxy::transforms::{Transforms, TransformsFromConfig};

fn criterion_benchmark(c: &mut Criterion) {
    let transforms: Vec<Transforms> = vec![Transforms::Null(Null::new_without_request())];

    let chain = TransformChain::new_no_shared_state(transforms, "bench".to_string());
    let wrapper = Wrapper::new(Message::Query(QueryMessage {
        original: RawFrame::NONE,
        query_string: "".to_string(),
        namespace: vec![],
        primary_key: HashMap::new(),
        query_values: None,
        projection: None,
        query_type: QueryType::Write,
        ast: None,
    }));

    c.bench_with_input(
        BenchmarkId::new("input_example", "Empty Message"),
        &wrapper,
        move |b, s| {
            let mut rt = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                let _ = rt.block_on(chain.process_request(s.clone()));
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

    let lwrapper = Wrapper::new(Message::Query(QueryMessage {
        original: RawFrame::NONE,
        query_string: "".to_string(),
        namespace: vec![String::from("keyspace"), String::from("old")],
        primary_key: Default::default(),
        query_values: None,
        projection: None,
        query_type: QueryType::Read,
        ast: None,
    }));

    let mut rt = tokio::runtime::Runtime::new().unwrap();

    let transform = rt.block_on(lua_t.get_source(&t_holder)).unwrap();

    let transforms: Vec<Transforms> = vec![transform, Transforms::Null(Null::new())];

    let lchain = TransformChain::new_no_shared_state(transforms, String::from("test_chain"));

    c.bench_with_input(
        BenchmarkId::new("lua processing", "Empty Message"),
        &lwrapper,
        move |b, s| {
            b.iter(|| {
                let _ = rt.block_on(lchain.process_request(s.clone()));
            })
        },
    );
}

criterion_group!(benches, criterion_benchmark, lua_benchmark);
criterion_main!(benches);
