use crate::helpers::ShotoverManager;
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_ignore_matches() {
    let shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/tee/ignore.yaml");

    let mut connection = shotover_manager.redis_connection_async(6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_ignore_with_mismatch() {
    let shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/tee/ignore_with_mismatch.yaml");

    let mut connection = shotover_manager.redis_connection_async(6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_fail_matches() {
    let shotover_manager = ShotoverManager::from_topology_file("tests/test-configs/tee/fail.yaml");

    let mut connection = shotover_manager.redis_connection_async(6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_fail_with_mismatch() {
    let shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/tee/fail_with_mismatch.yaml");

    let mut connection = shotover_manager.redis_connection_async(6379).await;

    let err = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap_err()
        .to_string();

    let expected = "An error was signalled by the server: The responses from the Tee subchain and down-chain did not match and behavior is set to fail on mismatch";
    assert_eq!(expected, err);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_subchain_matches() {
    let _compose = DockerCompose::new("example-configs/redis-passthrough/docker-compose.yml");
    let shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/tee/subchain.yaml");

    let mut shotover_connection = shotover_manager.redis_connection_async(6379).await;
    let mut mismatch_chain_redis = shotover_manager.redis_connection_async(1111).await;
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
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_subchain_with_mismatch() {
    let _compose = DockerCompose::new("example-configs/redis-passthrough/docker-compose.yml");
    let shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/tee/subchain_with_mismatch.yaml");

    let mut shotover_connection = shotover_manager.redis_connection_async(6379).await;
    let mut mismatch_chain_redis = shotover_manager.redis_connection_async(1111).await;

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
}
