use basic_driver_tests::*;
use redis::aio::Connection;
use redis::Commands;
use serial_test::serial;
use shotover_process::ShotoverProcessBuilder;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;
use test_helpers::connection::redis_connection;
use test_helpers::docker_compose::docker_compose;
use test_helpers::shotover_process::{self, Count, EventMatcher, Level};

pub mod assert;
pub mod basic_driver_tests;

fn invalid_frame_event() -> EventMatcher {
    EventMatcher::new()
        .with_level(Level::Warn)
        .with_target("shotover::server")
        .with_message(
            r#"failed to decode message: Error decoding redis frame

Caused by:
    Decode Error: frame_type: Invalid frame type."#,
        )
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn passthrough() {
    let _compose = docker_compose("tests/test-configs/redis-passthrough/docker-compose.yaml");
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/redis-passthrough/topology.yaml",
    )
    .start()
    .await;
    let connection = || redis_connection::new_async("127.0.0.1", 6379);
    let mut flusher =
        Flusher::new_single_connection(redis_connection::new_async("127.0.0.1", 6379).await).await;

    run_all(&connection, &mut flusher).await;
    test_invalid_frame().await;
    shotover
        .shutdown_and_then_consume_events(&[invalid_frame_event()])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn passthrough_redis_down() {
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/redis-passthrough/topology.yaml",
    )
    .start()
    .await;
    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    test_trigger_transform_failure_driver(&mut connection).await;
    test_trigger_transform_failure_raw().await;

    test_invalid_frame().await;

    shotover
        .shutdown_and_then_consume_events(&[
            EventMatcher::new()
                .with_level(Level::Error)
                .with_target("shotover::server")
                .with_message(
                    r#"connection was unexpectedly terminated

Caused by:
    0: Chain failed to send and/or receive messages, the connection will now be closed.
    1: RedisSinkSingle transform failed
    2: Failed to connect to destination "127.0.0.1:1111"
    3: Connection refused (os error 111)"#,
                )
                .with_count(Count::Times(2)),
            invalid_frame_event(),
        ])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn tls_cluster_sink() {
    test_helpers::cert::generate_redis_test_certs();

    let _compose = docker_compose("tests/test-configs/redis-cluster-tls/docker-compose.yaml");
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/redis-cluster-tls/topology-no-source-encryption.yaml",
    )
    .start()
    .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;
    let mut flusher = Flusher::new_cluster_tls().await;

    run_all_cluster_hiding(&mut connection, &mut flusher).await;
    test_cluster_ports_rewrite_slots(&mut connection, 6379).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn tls_source_and_tls_single_sink() {
    test_helpers::cert::generate_redis_test_certs();

    {
        let _compose = docker_compose("tests/test-configs/redis-tls/docker-compose.yaml");
        let shotover =
            ShotoverProcessBuilder::new_with_topology("tests/test-configs/redis-tls/topology.yaml")
                .start()
                .await;

        let connection = || redis_connection::new_async_tls("127.0.0.1", 6379);
        let mut flusher = Flusher::new_single_connection(
            redis_connection::new_async_tls("127.0.0.1", 6379).await,
        )
        .await;

        run_all(&connection, &mut flusher).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    // Quick test to verify client authentication disabling works
    {
        let _compose =
            docker_compose("tests/test-configs/redis-tls-no-client-auth/docker-compose.yaml");
        let shotover = ShotoverProcessBuilder::new_with_topology(
            "tests/test-configs/redis-tls-no-client-auth/topology.yaml",
        )
        .start()
        .await;

        let mut connection = redis_connection::new_async_tls("127.0.0.1", 6379).await;
        test_cluster_basics(&mut connection).await;
        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    // Quick test to verify `verify-hostname: false` works
    test_helpers::cert::generate_test_certs_with_bad_san(Path::new(
        "tests/test-configs/redis-tls/certs",
    ));
    {
        let _compose =
            docker_compose("tests/test-configs/redis-tls-no-verify-hostname/docker-compose.yaml");
        let shotover = ShotoverProcessBuilder::new_with_topology(
            "tests/test-configs/redis-tls-no-verify-hostname/topology.yaml",
        )
        .start()
        .await;

        let mut connection = redis_connection::new_async_tls("127.0.0.1", 6379).await;
        test_cluster_basics(&mut connection).await;
        shotover.shutdown_and_then_consume_events(&[]).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_ports_rewrite() {
    let _compose =
        docker_compose("tests/test-configs/redis-cluster-ports-rewrite/docker-compose.yaml");
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/redis-cluster-ports-rewrite/topology.yaml",
    )
    .start()
    .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6380).await;
    let mut flusher =
        Flusher::new_single_connection(redis_connection::new_async("127.0.0.1", 6380).await).await;

    run_all_cluster_hiding(&mut connection, &mut flusher).await;

    test_cluster_ports_rewrite_slots(&mut connection, 6380).await;
    test_cluster_ports_rewrite_nodes(&mut connection, 6380).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn multi() {
    let _compose = docker_compose("tests/test-configs/redis-multi/docker-compose.yaml");
    let shotover =
        ShotoverProcessBuilder::new_with_topology("tests/test-configs/redis-multi/topology.yaml")
            .start()
            .await;
    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;
    let mut flusher =
        Flusher::new_single_connection(redis_connection::new_async("127.0.0.1", 6379).await).await;

    run_all_multi_safe(&mut connection, &mut flusher).await;

    shotover
        .shutdown_and_then_consume_events(&[EventMatcher::new()
            .with_level(Level::Error)
            .with_target("shotover::transforms::chain")
            .with_message(
                r#"Internal error in buffered chain: RedisTimestampTagger transform failed

Caused by:
    0: RedisSinkSingle transform failed
    1: Failed to receive message because RedisSinkSingle response processing task is dead"#,
            )
            .with_count(Count::Any)])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_auth() {
    let _compose = docker_compose("tests/test-configs/redis-cluster-auth/docker-compose.yaml");
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/redis-cluster-auth/topology.yaml",
    )
    .start()
    .await;
    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    test_auth(&mut connection).await;
    let connection = || redis_connection::new_async("127.0.0.1", 6379);
    test_auth_isolation(&connection).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_hiding() {
    let _compose = docker_compose("tests/test-configs/redis-cluster-hiding/docker-compose.yaml");
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/redis-cluster-hiding/topology.yaml",
    )
    .start()
    .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;
    let connection = &mut connection;
    let mut flusher = Flusher::new_cluster().await;

    run_all_cluster_hiding(connection, &mut flusher).await;
    test_cluster_ports_rewrite_slots(connection, 6379).await;
    test_cluster_ports_rewrite_nodes(connection, 6379).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_handling() {
    let _compose = docker_compose("tests/test-configs/redis-cluster-handling/docker-compose.yaml");
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/redis-cluster-handling/topology.yaml",
    )
    .start()
    .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;
    let connection = &mut connection;

    let mut flusher = Flusher::new_cluster().await;

    run_all_cluster_handling(connection, &mut flusher).await;
    test_cluster_ports_rewrite_slots(connection, 6379).await;
    test_cluster_ports_rewrite_nodes(connection, 6379).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_dr() {
    let _compose = docker_compose("tests/test-configs/redis-cluster-dr/docker-compose.yaml");

    let nodes = vec![
        "redis://127.0.0.1:2120/",
        "redis://127.0.0.1:2121/",
        "redis://127.0.0.1:2122/",
        "redis://127.0.0.1:2123/",
        "redis://127.0.0.1:2124/",
        "redis://127.0.0.1:2125/",
    ];
    let client = redis::cluster::ClusterClientBuilder::new(nodes)
        .password("shotover".to_string())
        .build()
        .unwrap();
    let mut replication_connection = client.get_connection().unwrap();

    // test coalesce sends messages on shotover shutdown
    {
        let shotover = ShotoverProcessBuilder::new_with_topology(
            "tests/test-configs/redis-cluster-dr/topology.yaml",
        )
        .start()
        .await;
        let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;
        redis::cmd("AUTH")
            .arg("default")
            .arg("shotover")
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap();

        redis::cmd("SET")
            .arg("key1")
            .arg(42)
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap();
        redis::cmd("SET")
            .arg("key2")
            .arg(358)
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap();

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    sleep(Duration::from_secs(1));
    assert_eq!(replication_connection.get::<&str, i32>("key1").unwrap(), 42);
    assert_eq!(
        replication_connection.get::<&str, i32>("key2").unwrap(),
        358
    );

    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/redis-cluster-dr/topology.yaml",
    )
    .start()
    .await;

    async fn new_connection() -> Connection {
        let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

        redis::cmd("AUTH")
            .arg("default")
            .arg("shotover")
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap();

        connection
    }
    let mut connection = new_connection().await;
    let mut flusher = Flusher::new_single_connection(new_connection().await).await;

    test_cluster_replication(&mut connection, &mut replication_connection).await;
    test_dr_auth().await;
    run_all_cluster_hiding(&mut connection, &mut flusher).await;

    shotover
        .shutdown_and_then_consume_events(&[EventMatcher::new()
            .with_level(Level::Error)
            .with_target("shotover::transforms::filter")
            .with_message("The current filter transform implementation does not obey the current transform invariants. see https://github.com/shotover/shotover-proxy/issues/499")
        ])
        .await;
}
