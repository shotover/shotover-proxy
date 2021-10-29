use crate::helpers::ShotoverManager;
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;

async fn http_request_metrics() -> String {
    let client = hyper::Client::new();
    let uri = "http://localhost:9001/metrics".parse().unwrap();
    let res = client.get(uri).await.unwrap();
    let body_bytes = hyper::body::to_bytes(res.into_body()).await.unwrap();
    String::from_utf8(body_bytes.to_vec()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_metrics() {
    let _compose = DockerCompose::new("examples/redis-cluster/docker-compose.yml")
        .wait_for_n("Cluster state changed", 6);
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster/topology.yaml");

    let mut connection = shotover_manager.redis_connection_async(6379).await;

    let expected = [
        // transform counter
        r#"# TYPE shotover_transform_total counter"#,
        r#"shotover_transform_total{transform="RedisSinkCluster"}"#,
        r#"shotover_transform_total{transform="QueryCounter"}"#,
        // chain counter
        r#"# TYPE shotover_chain_total counter"#,
        r#"shotover_chain_total{chain="redis_chain"}"#,
        // chain failures
        r#"# TYPE shotover_chain_failures counter"#,
        r#"shotover_chain_failures{chain="redis_chain"}"#,
        r#"shotover_transform_failures{transform="RedisSinkCluster"}"#,
        // transform failures
        r#"# TYPE shotover_transform_failures counter"#,
        r#"shotover_transform_failures{transform="RedisSinkCluster"}"#,
        // available connections
        r#"# TYPE shotover_available_connections gauge"#,
        r#"shotover_available_connections{source="Redis Source"}"#,
        // redis sink cluster  latency summary
        r#"# TYPE shotover_transform_latency summary"#,
        r#"shotover_transform_latency{transform="RedisSinkCluster",quantile="0"}"#,
        r#"shotover_transform_latency{transform="RedisSinkCluster",quantile="0.5"}"#,
        r#"shotover_transform_latency{transform="RedisSinkCluster",quantile="0.9"}"#,
        r#"shotover_transform_latency{transform="RedisSinkCluster",quantile="0.95"}"#,
        r#"shotover_transform_latency{transform="RedisSinkCluster",quantile="0.99"}"#,
        r#"shotover_transform_latency{transform="RedisSinkCluster",quantile="0.999"}"#,
        r#"shotover_transform_latency{transform="RedisSinkCluster",quantile="1"}"#,
        r#"shotover_transform_latency_sum{transform="RedisSinkCluster"}"#,
        r#"shotover_transform_latency_count{transform="RedisSinkCluster"}"#,
        // query counter latency summary
        r#"shotover_transform_latency{transform="QueryCounter",quantile="0"}"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="0.5"}"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="0.9"}"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="0.95"}"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="0.99"}"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="0.999"}"#,
        r#"shotover_transform_latency{transform="QueryCounter",quantile="1"}"#,
        r#"shotover_transform_latency_sum{transform="QueryCounter"}"#,
        r#"shotover_transform_latency_count{transform="QueryCounter"}"#,
        // chain latency
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

    for s in expected {
        assert!(body.contains(s));
    }

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

    body = http_request_metrics().await;

    for s in expected {
        assert!(body.contains(s));
    }
}
