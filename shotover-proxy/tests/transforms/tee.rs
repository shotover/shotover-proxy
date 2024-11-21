use crate::shotover_process;
use pretty_assertions::assert_eq;
use test_helpers::connection::redis_connection;
use test_helpers::docker_compose::docker_compose;
use test_helpers::shotover_process::{Count, EventMatcher, Level};

#[tokio::test(flavor = "multi_thread")]
async fn test_ignore_matches() {
    let shotover = shotover_process("tests/test-configs/tee/ignore.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ignore_with_mismatch() {
    let shotover = shotover_process("tests/test-configs/tee/ignore_with_mismatch.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_log_matches() {
    let shotover = shotover_process("tests/test-configs/tee/log.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_log_with_mismatch() {
    let shotover = shotover_process("tests/test-configs/tee/log_with_mismatch.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
    shotover
        .shutdown_and_then_consume_events(&[EventMatcher::new()
            .with_level(Level::Warn)
            .with_count(Count::Times(3))
            .with_target("shotover::transforms::tee")
            .with_message(
                r#"Tee mismatch:
result-source response: Valkey BulkString(b"42")
other response: Valkey BulkString(b"41")"#,
            )])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fail_matches() {
    let shotover = shotover_process("tests/test-configs/tee/fail.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fail_with_mismatch() {
    let shotover = shotover_process("tests/test-configs/tee/fail_with_mismatch.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let err = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap_err()
        .to_string();

    let expected = "An error was signalled by the server - ResponseError: ERR The responses from the Tee subchain and down-chain did not match and behavior is set to fail on mismatch";
    assert_eq!(expected, err);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subchain_matches() {
    let _compose = docker_compose("tests/test-configs/redis/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/tee/subchain.yaml")
        .start()
        .await;

    let mut shotover_connection = redis_connection::new_async("127.0.0.1", 6379).await;
    let mut mismatch_chain_redis = redis_connection::new_async("127.0.0.1", 1111).await;
    redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut mismatch_chain_redis)
        .await
        .unwrap();

    let mut result = redis::cmd("SET")
        .arg("key")
        .arg("notmyvalue")
        .query_async::<_, String>(&mut shotover_connection)
        .await
        .unwrap();

    assert_eq!(result, "42");

    result = redis::cmd("GET")
        .arg("key")
        .query_async::<_, String>(&mut mismatch_chain_redis)
        .await
        .unwrap();

    assert_eq!(result, "myvalue");
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subchain_with_mismatch() {
    let _compose = docker_compose("tests/test-configs/redis/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/tee/subchain_with_mismatch.yaml")
        .start()
        .await;

    let mut shotover_connection = redis_connection::new_async("127.0.0.1", 6379).await;
    let mut mismatch_chain_redis = redis_connection::new_async("127.0.0.1", 1111).await;

    // Set the value on the top level chain redis
    let mut result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut shotover_connection)
        .await
        .unwrap();

    assert_eq!(result, "42");

    // When the mismatch occurs, the value should be sent to the mismatch chain's redis
    result = redis::cmd("GET")
        .arg("key")
        .query_async::<_, String>(&mut mismatch_chain_redis)
        .await
        .unwrap();

    assert_eq!("myvalue", result);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_switch_main_chain() {
    let shotover = shotover_process("tests/test-configs/tee/switch_chain.yaml")
        .start()
        .await;

    for i in 1..=3 {
        println!("{i}");
        let redis_port = 6370 + i;
        let switch_port = 1230 + i;

        let mut connection = redis_connection::new_async("127.0.0.1", redis_port).await;

        let result = redis::cmd("SET")
            .arg("key")
            .arg("myvalue")
            .query_async::<_, String>(&mut connection)
            .await
            .unwrap();

        assert_eq!("a", result);

        let url = format!("http://localhost:{switch_port}/transform/tee/result-source");
        let client = reqwest::Client::new();
        client.put(&url).body("tee-chain").send().await.unwrap();

        let body = client.get(&url).send().await.unwrap().text().await.unwrap();
        assert_eq!("tee-chain", body);

        let result = redis::cmd("SET")
            .arg("key")
            .arg("myvalue")
            .query_async::<_, String>(&mut connection)
            .await
            .unwrap();

        assert_eq!("b", result);

        client.put(&url).body("regular-chain").send().await.unwrap();

        let result = redis::cmd("SET")
            .arg("key")
            .arg("myvalue")
            .query_async::<_, String>(&mut connection)
            .await
            .unwrap();

        assert_eq!("a", result);
    }

    shotover
        .shutdown_and_then_consume_events(&[
            EventMatcher::new()
                .with_level(Level::Warn)
                // generated by the final loop above, 2 by the requests + 2 by the redis-rs driver connection handshake
                .with_count(Count::Times(4))
                .with_target("shotover::transforms::tee")
                .with_message(
                    r#"Tee mismatch:
result-source response: Valkey BulkString(b"a")
other response: Valkey BulkString(b"b")"#,
                ),
            EventMatcher::new()
                .with_level(Level::Warn)
                // generated by the final loop above, by the request made while the result-source is flipped.
                .with_count(Count::Times(1))
                .with_target("shotover::transforms::tee")
                .with_message(
                    r#"Tee mismatch:
result-source response: Valkey BulkString(b"b")
other response: Valkey BulkString(b"a")"#,
                ),
        ])
        .await;
}
