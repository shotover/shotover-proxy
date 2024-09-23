use crate::shotover_process;
use test_helpers::connection::redis_connection;
use test_helpers::docker_compose::docker_compose;
use test_helpers::metrics::{assert_metrics_has_keys, assert_metrics_key_value};

#[tokio::test(flavor = "multi_thread")]
async fn test_metrics() {
    let shotover = shotover_process("tests/test-configs/null-redis/topology.yaml")
        .start()
        .await;

    // Expected string looks unnatural because it is sorted in alphabetical order to make it match the sorted error output
    let expected = r#"
# TYPE connections_opened counter
# TYPE shotover_available_connections_count gauge
# TYPE shotover_chain_failures_count counter
# TYPE shotover_chain_messages_per_batch_count summary
# TYPE shotover_chain_requests_batch_size summary
# TYPE shotover_chain_responses_batch_size summary
# TYPE shotover_chain_total_count counter
# TYPE shotover_query_count counter
# TYPE shotover_sink_to_source_latency_seconds summary
# TYPE shotover_transform_failures_count counter
# TYPE shotover_transform_latency_seconds summary
# TYPE shotover_transform_total_count counter
connections_opened{source="redis"}
shotover_available_connections_count{source="redis"}
shotover_chain_failures_count{chain="redis"}
shotover_chain_messages_per_batch_count_count{chain="redis"}
shotover_chain_messages_per_batch_count_sum{chain="redis"}
shotover_chain_messages_per_batch_count{chain="redis",quantile="0"}
shotover_chain_messages_per_batch_count{chain="redis",quantile="0.1"}
shotover_chain_messages_per_batch_count{chain="redis",quantile="0.5"}
shotover_chain_messages_per_batch_count{chain="redis",quantile="0.9"}
shotover_chain_messages_per_batch_count{chain="redis",quantile="0.95"}
shotover_chain_messages_per_batch_count{chain="redis",quantile="0.99"}
shotover_chain_messages_per_batch_count{chain="redis",quantile="0.999"}
shotover_chain_messages_per_batch_count{chain="redis",quantile="1"}
shotover_chain_requests_batch_size_count{chain="redis"}
shotover_chain_requests_batch_size_sum{chain="redis"}
shotover_chain_requests_batch_size{chain="redis",quantile="0"}
shotover_chain_requests_batch_size{chain="redis",quantile="0.1"}
shotover_chain_requests_batch_size{chain="redis",quantile="0.5"}
shotover_chain_requests_batch_size{chain="redis",quantile="0.9"}
shotover_chain_requests_batch_size{chain="redis",quantile="0.95"}
shotover_chain_requests_batch_size{chain="redis",quantile="0.99"}
shotover_chain_requests_batch_size{chain="redis",quantile="0.999"}
shotover_chain_requests_batch_size{chain="redis",quantile="1"}
shotover_chain_responses_batch_size_count{chain="redis"}
shotover_chain_responses_batch_size_sum{chain="redis"}
shotover_chain_responses_batch_size{chain="redis",quantile="0"}
shotover_chain_responses_batch_size{chain="redis",quantile="0.1"}
shotover_chain_responses_batch_size{chain="redis",quantile="0.5"}
shotover_chain_responses_batch_size{chain="redis",quantile="0.9"}
shotover_chain_responses_batch_size{chain="redis",quantile="0.95"}
shotover_chain_responses_batch_size{chain="redis",quantile="0.99"}
shotover_chain_responses_batch_size{chain="redis",quantile="0.999"}
shotover_chain_responses_batch_size{chain="redis",quantile="1"}
shotover_chain_total_count{chain="redis"}
shotover_query_count{name="redis-chain"}
shotover_sink_to_source_latency_seconds_count{source="redis"}
shotover_sink_to_source_latency_seconds_sum{source="redis"}
shotover_sink_to_source_latency_seconds{source="redis",quantile="0"}
shotover_sink_to_source_latency_seconds{source="redis",quantile="0.1"}
shotover_sink_to_source_latency_seconds{source="redis",quantile="0.5"}
shotover_sink_to_source_latency_seconds{source="redis",quantile="0.9"}
shotover_sink_to_source_latency_seconds{source="redis",quantile="0.95"}
shotover_sink_to_source_latency_seconds{source="redis",quantile="0.99"}
shotover_sink_to_source_latency_seconds{source="redis",quantile="0.999"}
shotover_sink_to_source_latency_seconds{source="redis",quantile="1"}
shotover_transform_failures_count{transform="NullSink"}
shotover_transform_failures_count{transform="QueryCounter"}
shotover_transform_latency_seconds_count{transform="NullSink"}
shotover_transform_latency_seconds_count{transform="QueryCounter"}
shotover_transform_latency_seconds_sum{transform="NullSink"}
shotover_transform_latency_seconds_sum{transform="QueryCounter"}
shotover_transform_latency_seconds{transform="NullSink",quantile="0"}
shotover_transform_latency_seconds{transform="NullSink",quantile="0.1"}
shotover_transform_latency_seconds{transform="NullSink",quantile="0.5"}
shotover_transform_latency_seconds{transform="NullSink",quantile="0.9"}
shotover_transform_latency_seconds{transform="NullSink",quantile="0.95"}
shotover_transform_latency_seconds{transform="NullSink",quantile="0.99"}
shotover_transform_latency_seconds{transform="NullSink",quantile="0.999"}
shotover_transform_latency_seconds{transform="NullSink",quantile="1"}
shotover_transform_latency_seconds{transform="QueryCounter",quantile="0"}
shotover_transform_latency_seconds{transform="QueryCounter",quantile="0.1"}
shotover_transform_latency_seconds{transform="QueryCounter",quantile="0.5"}
shotover_transform_latency_seconds{transform="QueryCounter",quantile="0.9"}
shotover_transform_latency_seconds{transform="QueryCounter",quantile="0.95"}
shotover_transform_latency_seconds{transform="QueryCounter",quantile="0.99"}
shotover_transform_latency_seconds{transform="QueryCounter",quantile="0.999"}
shotover_transform_latency_seconds{transform="QueryCounter",quantile="1"}
shotover_transform_total_count{transform="NullSink"}
shotover_transform_total_count{transform="QueryCounter"}
"#;
    assert_metrics_has_keys("", expected).await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

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
# TYPE shotover_chain_latency_seconds summary
shotover_chain_latency_seconds_count{chain="redis",client_details="127.0.0.1"}
shotover_chain_latency_seconds_sum{chain="redis",client_details="127.0.0.1"}
shotover_chain_latency_seconds{chain="redis",client_details="127.0.0.1",quantile="0"}
shotover_chain_latency_seconds{chain="redis",client_details="127.0.0.1",quantile="0.1"}
shotover_chain_latency_seconds{chain="redis",client_details="127.0.0.1",quantile="0.5"}
shotover_chain_latency_seconds{chain="redis",client_details="127.0.0.1",quantile="0.9"}
shotover_chain_latency_seconds{chain="redis",client_details="127.0.0.1",quantile="0.95"}
shotover_chain_latency_seconds{chain="redis",client_details="127.0.0.1",quantile="0.99"}
shotover_chain_latency_seconds{chain="redis",client_details="127.0.0.1",quantile="0.999"}
shotover_chain_latency_seconds{chain="redis",client_details="127.0.0.1",quantile="1"}
shotover_query_count{name="redis-chain",query="CLIENT",type="redis"}
shotover_query_count{name="redis-chain",query="GET",type="redis"}
shotover_query_count{name="redis-chain",query="SET",type="redis"}
"#;
    assert_metrics_has_keys(expected, expected_new).await;

    assert_metrics_key_value(
        r#"shotover_query_count{name="redis-chain",query="GET",type="redis"}"#,
        "1",
    )
    .await;
    assert_metrics_key_value(
        r#"shotover_query_count{name="redis-chain",query="SET",type="redis"}"#,
        "2",
    )
    .await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shotover_with_metrics_disabled() {
    let _compose = docker_compose("tests/test-configs/redis/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/redis/passthrough/topology.yaml")
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    // Verify Shotover can still process messages with metrics disabled
    redis::cmd("SET")
        .arg("the_key")
        .arg(42)
        .query_async::<_, ()>(&mut connection)
        .await
        .unwrap();

    shotover.shutdown_and_then_consume_events(&[]).await;
}
