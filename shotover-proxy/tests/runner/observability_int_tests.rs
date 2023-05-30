use serial_test::serial;
use test_helpers::connection::redis_connection;
use test_helpers::metrics::{assert_metrics_has_keys, assert_metrics_key_value};
use test_helpers::shotover_process::ShotoverProcessBuilder;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_metrics() {
    let shotover =
        ShotoverProcessBuilder::new_with_topology("tests/test-configs/null-redis/topology.yaml")
            .start()
            .await;
    let mut connection = redis_connection::new_async(6379).await;

    // Expected string looks unnatural because it is sorted in alphabetical order to make it match the sorted error output
    let expected = r#"
# TYPE query_count counter
# TYPE shotover_available_connections gauge
# TYPE shotover_chain_failures counter
# TYPE shotover_chain_total counter
# TYPE shotover_transform_failures counter
# TYPE shotover_transform_latency summary
# TYPE shotover_transform_total counter
query_count{name="redis-chain"}
shotover_available_connections{source="RedisSource"}
shotover_chain_failures{chain="redis_chain"}
shotover_chain_total{chain="redis_chain"}
shotover_transform_failures{transform="NullSink"}
shotover_transform_failures{transform="QueryCounter"}
shotover_transform_latency_count{transform="NullSink"}
shotover_transform_latency_count{transform="QueryCounter"}
shotover_transform_latency_sum{transform="NullSink"}
shotover_transform_latency_sum{transform="QueryCounter"}
shotover_transform_latency{transform="NullSink",quantile="0"}
shotover_transform_latency{transform="NullSink",quantile="0.5"}
shotover_transform_latency{transform="NullSink",quantile="0.9"}
shotover_transform_latency{transform="NullSink",quantile="0.95"}
shotover_transform_latency{transform="NullSink",quantile="0.99"}
shotover_transform_latency{transform="NullSink",quantile="0.999"}
shotover_transform_latency{transform="NullSink",quantile="1"}
shotover_transform_latency{transform="QueryCounter",quantile="0"}
shotover_transform_latency{transform="QueryCounter",quantile="0.5"}
shotover_transform_latency{transform="QueryCounter",quantile="0.9"}
shotover_transform_latency{transform="QueryCounter",quantile="0.95"}
shotover_transform_latency{transform="QueryCounter",quantile="0.99"}
shotover_transform_latency{transform="QueryCounter",quantile="0.999"}
shotover_transform_latency{transform="QueryCounter",quantile="1"}
shotover_transform_total{transform="NullSink"}
shotover_transform_total{transform="QueryCounter"}
"#;
    assert_metrics_has_keys("", expected).await;

    // Check we still get the metrics after sending a couple requests
    redis::cmd("SET")
        .arg("the_key")
        .arg(42)
        .query_async::<_, ()>(&mut connection)
        .await
        .unwrap_err();

    redis::cmd("SET")
        .arg("the_key")
        .arg(43)
        .query_async::<_, ()>(&mut connection)
        .await
        .unwrap_err();

    redis::cmd("GET")
        .arg("the_key")
        .query_async::<_, ()>(&mut connection)
        .await
        .unwrap_err();

    let expected_new = r#"
# TYPE shotover_chain_latency summary
query_count{name="redis-chain",query="GET",type="redis"}
query_count{name="redis-chain",query="SET",type="redis"}
shotover_chain_latency_count{chain="redis_chain",client_details="127.0.0.1"}
shotover_chain_latency_sum{chain="redis_chain",client_details="127.0.0.1"}
shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="0"}
shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="0.5"}
shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="0.9"}
shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="0.95"}
shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="0.99"}
shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="0.999"}
shotover_chain_latency{chain="redis_chain",client_details="127.0.0.1",quantile="1"}
"#;
    assert_metrics_has_keys(expected, expected_new).await;

    assert_metrics_key_value(
        r#"query_count{name="redis-chain",query="GET",type="redis"}"#,
        "1",
    )
    .await;
    assert_metrics_key_value(
        r#"query_count{name="redis-chain",query="SET",type="redis"}"#,
        "2",
    )
    .await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}
