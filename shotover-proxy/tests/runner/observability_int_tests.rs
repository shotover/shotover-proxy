use crate::helpers::ShotoverManager;
use serial_test::serial;

async fn http_request_metrics() -> String {
    let url = "http://localhost:9001/metrics";
    reqwest::get(url).await.unwrap().text().await.unwrap()
}

fn assert_array_elems(expected: &[&str], result: String) {
    for s in expected {
        if !result.contains(s) {
            panic!("{} missing from response: \n{}", s, result);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_metrics() {
    let shotover_manager = ShotoverManager::from_topology_file("examples/null-redis/topology.yaml");
    let mut connection = shotover_manager.redis_connection_async(6379).await;

    let expected = vec![
        r#"# TYPE query_count counter"#,
        r#"query_count{name="redis-chain"}"#,
        r#"# TYPE shotover_chain_total counter"#,
        r#"shotover_chain_total{chain="redis_chain"}"#,
        r#"# TYPE shotover_transform_total counter"#,
        r#"shotover_transform_total{transform="QueryCounter"}"#,
        r#"shotover_transform_total{transform="Null"}"#,
        r#"# TYPE shotover_chain_failures counter"#,
        r#"shotover_chain_failures{chain="redis_chain"}"#,
        r#"# TYPE shotover_transform_failures counter"#,
        r#"shotover_transform_failures{transform="Null"}"#,
        r#"shotover_transform_failures{transform="QueryCounter"}"#,
        r#"# TYPE shotover_available_connections gauge"#,
        r#"shotover_available_connections{source="RedisSource"}"#,
        r#"# TYPE shotover_transform_latency summary"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="0"}"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="0.5"}"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="0.9"}"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="0.95"}"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="0.99"}"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="0.999"}"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="1"}"#,
        r#"shotover_transform_latency_sum{transform="QueryCounter"}"#,
        r#"shotover_transform_latency_count{transform="QueryCounter"}"#,
        r#"shotover_transform_latency{transform="Null",quantile="0"}"#,
        r#"shotover_transform_latency{transform="Null",quantile="0.5"}"#,
        r#"shotover_transform_latency{transform="Null",quantile="0.9"}"#,
        r#"shotover_transform_latency{transform="Null",quantile="0.95"}"#,
        r#"shotover_transform_latency{transform="Null",quantile="0.99"}"#,
        r#"shotover_transform_latency{transform="Null",quantile="0.999"}"#,
        r#"shotover_transform_latency{transform="Null",quantile="1"}"#,
        r#"shotover_transform_latency_sum{transform="Null"}"#,
        r#"shotover_transform_latency_count{transform="Null"}"#,
        r#"# TYPE shotover_chain_latency summary"#,
        r#"shotover_chain_latency{chain="redis_chain",quantile="0"}"#,
        r#"shotover_chain_latency{chain="redis_chain",quantile="0.5"}"#,
        r#"shotover_chain_latency{chain="redis_chain",quantile="0.9"}"#,
        r#"shotover_chain_latency{chain="redis_chain",quantile="0.95"}"#,
        r#"shotover_chain_latency{chain="redis_chain",quantile="0.99"}"#,
        r#"shotover_chain_latency{chain="redis_chain",quantile="0.999"}"#,
        r#"shotover_chain_latency{chain="redis_chain",quantile="1"}"#,
        r#"shotover_chain_latency_sum{chain="redis_chain"}"#,
        r#"shotover_chain_latency_count{chain="redis_chain"}"#,
    ];

    // check we get the metrics on startup
    let mut body = http_request_metrics().await;

    let lines = body.lines().count();

    assert_array_elems(&expected, body);

    // Check we still get the metrics after sending a couple requests
    redis::cmd("SET")
        .arg("the_key")
        .arg(42)
        .query_async::<_, ()>(&mut connection)
        .await
        .unwrap();

    redis::cmd("SET")
        .arg("the_key")
        .arg(43)
        .query_async::<_, ()>(&mut connection)
        .await
        .unwrap();

    redis::cmd("GET")
        .arg("the_key")
        .query_async::<_, ()>(&mut connection)
        .await
        .unwrap();

    body = http_request_metrics().await;

    //TODO we dont assert the contents of metrics response yet,
    // due to the randomized order of the labels. Will need to parse the prometheus output

    let new_lines = vec![
        r#"shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="0"}"#,
        r#"shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="0.5"}"#,
        r#"shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="0.9"}"#,
        r#"shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="0.95"}"#,
        r#"shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="0.99"}"#,
        r#"shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="0.999"}"#,
        r#"shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="1"}"#,
        r#"shotover_chain_latency_sum{chain="redis_chain",client_details="127.0.0.1"}"#,
        r#"shotover_chain_latency_count{chain="redis_chain",client_details="127.0.0.1"}"#,
    ];

    let count= body.lines().count();
    // report missing rows before counting rows -- aids in debugging
    assert_array_elems(&expected, body);

    assert_eq!(lines + new_lines.len(), count);
}
