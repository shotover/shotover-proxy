use serial_test::serial;
use test_helpers::connection::redis_connection;
use test_helpers::docker_compose::docker_compose;
use test_helpers::shotover_process::ShotoverProcessBuilder;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_ignore_matches() {
    let shotover = ShotoverProcessBuilder::new_with_topology("tests/test-configs/tee/ignore.yaml")
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
#[serial]
async fn test_ignore_with_mismatch() {
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/tee/ignore_with_mismatch.yaml",
    )
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
#[serial]
async fn test_fail_matches() {
    let shotover = ShotoverProcessBuilder::new_with_topology("tests/test-configs/tee/fail.yaml")
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
#[serial]
async fn test_fail_with_mismatch() {
    let shotover =
        ShotoverProcessBuilder::new_with_topology("tests/test-configs/tee/fail_with_mismatch.yaml")
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

    let expected = "An error was signalled by the server: ERR The responses from the Tee subchain and down-chain did not match and behavior is set to fail on mismatch";
    assert_eq!(expected, err);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_subchain_matches() {
    let _compose = docker_compose("tests/test-configs/redis-passthrough/docker-compose.yaml");
    let shotover =
        ShotoverProcessBuilder::new_with_topology("tests/test-configs/tee/subchain.yaml")
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
#[serial]
async fn test_subchain_with_mismatch() {
    let _compose = docker_compose("tests/test-configs/redis-passthrough/docker-compose.yaml");
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/tee/subchain_with_mismatch.yaml",
    )
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
