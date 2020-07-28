use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use instaproxy::transforms::chain::{TransformChain, Wrapper};
use instaproxy::transforms::{Transforms, TransformsFromConfig};
use instaproxy::transforms::null::Null;
use instaproxy::message::{Message, QueryMessage, QueryType};
use std::collections::HashMap;

use instaproxy::config::topology::TopicHolder;
use instaproxy::transforms::python::PythonConfig;
use instaproxy::transforms::lua::LuaConfig;
use instaproxy::protocols::RawFrame;
use tokio::sync::mpsc::channel;
use tokio::runtime;
use tokio::runtime::Runtime;

const REQUEST_STRING: &str = r###"
qm.namespace = ["aaaaaaaaaa", "bbbbb"]
"###;

const RESPONSE_STRING: &str = r###"
qr.result = 42
"###;

const LREQUEST_STRING: &str = r###"
qm.namespace = {"aaaaaaaaaa", "bbbbb"}
return qm
"###;

const LRESPONSE_STRING: &str = r###"
qr.result = {Integer=42}
return qr
"###;

fn criterion_benchmark(c: &mut Criterion) {
    let transforms: Vec<Transforms> = vec![
        Transforms::Null(Null::new_without_request()),
    ];

    let chain = TransformChain::new_no_shared_state(transforms, "bench".to_string());
    let wrapper = Wrapper::new(Message::Query(QueryMessage {
        original: RawFrame::NONE,
        query_string: "".to_string(),
        namespace: vec![],
        primary_key: HashMap::new(),
        query_values: None,
        projection: None,
        query_type: QueryType::Write,
        ast: None
    }));

    c.bench_with_input(BenchmarkId::new("input_example", "Empty Message"), &wrapper,  move |b,s| {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        b.iter(|| {
            let _ = rt.block_on(chain.process_request(s.clone()));
        })
    });
}

fn python_benchmark(c: &mut Criterion) {
    let t_holder = TopicHolder::get_test_holder();

    let python_t = PythonConfig {
        query_filter: Some(String::from(REQUEST_STRING)),
        response_filter: Some(String::from(RESPONSE_STRING)),
    };

    let pywrapper = Wrapper::new(Message::Query(QueryMessage {
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

    let transform = rt.block_on(python_t.get_source(&t_holder)).unwrap();

    let transforms: Vec<Transforms> = vec![
        transform,
        Transforms::Null(Null::new()),
    ];


    let pychain = TransformChain::new_no_shared_state(transforms, String::from("test_chain"));

    c.bench_with_input(BenchmarkId::new("python processing", "Empty Message"), &pywrapper,  move |b,s| {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        b.iter(|| {
            let _ = rt.block_on(pychain.process_request(s.clone()));
        })
    });
}


fn lua_benchmark(c: &mut Criterion) {
    let t_holder = TopicHolder::get_test_holder();

    let lua_t = LuaConfig {
        function_def: "".to_string(),
        // query_filter: Some(String::from(LREQUEST_STRING)),
        // response_filter: Some(String::from(LRESPONSE_STRING)),
        function_name: "".to_string()
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


    let transforms: Vec<Transforms> = vec![
        transform,
        Transforms::Null(Null::new()),
    ];

    let lchain = TransformChain::new_no_shared_state(transforms, String::from("test_chain"));

    c.bench_with_input(BenchmarkId::new("lua processing", "Empty Message"), &lwrapper,  move |b,s| {
        b.iter(|| {
            let _ = rt.block_on(lchain.process_request(s.clone()));
        })
    });
}

criterion_group!(benches, criterion_benchmark, python_benchmark, lua_benchmark);
criterion_main!(benches);