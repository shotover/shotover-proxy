use crate::helpers::ShotoverManager;
use itertools::Itertools;
use serial_test::serial;

async fn http_request_metrics() -> String {
    let url = "http://localhost:9001/metrics";
    reqwest::get(url).await.unwrap().text().await.unwrap()
}

/// Asserts that the `expected` lines of keys are included in the metrics.
/// The `previous` lines are excluded from the assertion, allowing for better error messages when checking for added lines.
/// The keys are removed to keep the output deterministic.
async fn assert_metrics_has_keys(previous: &str, expected: &str) {
    let actual = http_request_metrics().await;

    let previous: Vec<&str> = previous.lines().filter(|x| !x.is_empty()).collect();
    let expected_sorted: Vec<&str> = expected
        .lines()
        .filter(|line| !line.is_empty())
        .sorted()
        .collect();
    let actual_sorted: Vec<&str> = actual
        .lines()
        .map(|x| {
            // Strip numbers from the end
            x.trim_end_matches(|c: char| {
                ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ' ', '.'].contains(&c)
            })
        })
        .filter(|line| {
            !line.is_empty() && previous.iter().all(|previous| !line.starts_with(previous))
        })
        .sorted()
        .collect();

    let expected_string = expected_sorted.join("\n");
    let actual_string = actual_sorted.join("\n");

    // Manually recreate assert_eq because it formats the strings poorly
    assert!(
        expected_string == actual_string,
        "expected:\n{expected_string}\nbut was:\n{actual_string}"
    );
}

/// Asserts that the metrics contains a key with the corresponding value
/// Use this to make assertions on specific keys that you know are deterministic
async fn assert_metrics_key_value(key: &str, value: &str) {
    let actual = http_request_metrics().await;

    for actual_line in actual.lines() {
        if let Some(actual_value) = actual_line.strip_prefix(key) {
            let actual_value = actual_value.trim();
            assert!(
                value == actual_value,
                "Expected metrics key {key:?} to have value {value:?} but it was instead {actual_value:?}"
            );
            return;
        }
    }
    panic!("key {key:?} was not found in metrics output:\n{actual}");
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_metrics() {
    let shotover_manager =
        ShotoverManager::from_topology_file("example-configs/null-redis/topology.yaml");
    let mut connection = shotover_manager.redis_connection_async(6379).await;

    // Expected string looks unnatural because it is sorted in alphabetical order to make it match the sorted error output
    let expected = r#"
# TYPE query_count counter
# TYPE shotover_available_connections gauge
# TYPE shotover_chain_failures counter
# TYPE shotover_chain_latency summary
# TYPE shotover_chain_total counter
# TYPE shotover_transform_failures counter
# TYPE shotover_transform_latency summary
# TYPE shotover_transform_total counter
query_count{name="redis-chain"}
shotover_available_connections{source="RedisSource"}
shotover_chain_failures{chain="redis_chain"}
shotover_chain_latency_count{chain="redis_chain"}
shotover_chain_latency_sum{chain="redis_chain"}
shotover_chain_latency{chain="redis_chain",quantile="0"}
shotover_chain_latency{chain="redis_chain",quantile="0.5"}
shotover_chain_latency{chain="redis_chain",quantile="0.9"}
shotover_chain_latency{chain="redis_chain",quantile="0.95"}
shotover_chain_latency{chain="redis_chain",quantile="0.99"}
shotover_chain_latency{chain="redis_chain",quantile="0.999"}
shotover_chain_latency{chain="redis_chain",quantile="1"}
shotover_chain_total{chain="redis_chain"}
shotover_transform_failures{transform="Null"}
shotover_transform_failures{transform="QueryCounter"}
shotover_transform_latency_count{transform="Null"}
shotover_transform_latency_count{transform="QueryCounter"}
shotover_transform_latency_sum{transform="Null"}
shotover_transform_latency_sum{transform="QueryCounter"}
shotover_transform_latency{transform="Null",quantile="0"}
shotover_transform_latency{transform="Null",quantile="0.5"}
shotover_transform_latency{transform="Null",quantile="0.9"}
shotover_transform_latency{transform="Null",quantile="0.95"}
shotover_transform_latency{transform="Null",quantile="0.99"}
shotover_transform_latency{transform="Null",quantile="0.999"}
shotover_transform_latency{transform="Null",quantile="1"}
shotover_transform_latency{transform="QueryCounter",quantile="0"}
shotover_transform_latency{transform="QueryCounter",quantile="0.5"}
shotover_transform_latency{transform="QueryCounter",quantile="0.9"}
shotover_transform_latency{transform="QueryCounter",quantile="0.95"}
shotover_transform_latency{transform="QueryCounter",quantile="0.99"}
shotover_transform_latency{transform="QueryCounter",quantile="0.999"}
shotover_transform_latency{transform="QueryCounter",quantile="1"}
shotover_transform_total{transform="Null"}
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
}
