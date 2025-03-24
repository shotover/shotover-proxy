use crate::{CONNECTION_REFUSED_OS_ERROR, shotover_process};
use basic_driver_tests::*;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use pretty_assertions::assert_eq;
use redis::Commands;
use redis::aio::Connection;

use std::path::Path;
use std::thread::sleep;
use std::time::Duration;
use test_helpers::connection::valkey_connection;
use test_helpers::connection::valkey_connection::{ValkeyConnectionCreator, ValkeyDriver};
use test_helpers::docker_compose::docker_compose;
use test_helpers::metrics::assert_metrics_key_value;
use test_helpers::shotover_process::{Count, EventMatcher, Level};

pub mod assert;
pub mod basic_driver_tests;
mod basic_driver_tests_sync;

fn invalid_frame_event() -> EventMatcher {
    EventMatcher::new()
        .with_level(Level::Warn)
        .with_target("shotover::server")
        .with_message(
            r#"failed to decode message: Error decoding valkey frame

Caused by:
    Decode Error: frame_type: Invalid frame type."#,
        )
}

// #[tokio::test(flavor = "multi_thread")]
// async fn passthrough_standard() {
//     let _compose = docker_compose("tests/test-configs/valkey/passthrough/docker-compose.yaml");
//     let shotover = shotover_process("tests/test-configs/valkey/passthrough/topology.yaml")
//         .start()
//         .await;
//     let connection = || valkey_connection::new_async("127.0.0.1", 6379);
//     let mut flusher =
//         Flusher::new_single_connection(valkey_connection::new_async("127.0.0.1", 6379).await).await;
//
//     run_all(&connection, &mut flusher).await;
//     test_invalid_frame().await;
//     shotover
//         .shutdown_and_then_consume_events(&[invalid_frame_event()])
//         .await;
//
//     assert_failed_requests_metric_is_incremented_on_error_response().await;
// }

#[tokio::test(flavor = "multi_thread")]
async fn passthrough_valkey_down() {
    let shotover = shotover_process("tests/test-configs/valkey/passthrough/topology.yaml")
        .start()
        .await;
    let client = Client::new(Config::default(), None, None, None);

    {
        let _shutdown_handle = client.connect();
        test_trigger_transform_failure_driver(&client).await;
    }
    test_trigger_transform_failure_raw().await;

    test_invalid_frame().await;

    shotover
        .shutdown_and_then_consume_events(&[
            // Error occurs when client sends a message to shotover
            EventMatcher::new()
                .with_level(Level::Error)
                .with_target("shotover::server")
                .with_message(&format!(
                    r#"connection was unexpectedly terminated

Caused by:
    0: Chain failed to send and/or receive messages, the connection will now be closed.
    1: ValkeySinkSingle transform failed
    2: Failed to connect to destination 127.0.0.1:1111
    3: Connection refused (os error {CONNECTION_REFUSED_OS_ERROR})"#
                ))
                .with_count(Count::Times(2)),
            // This error occurs due to `test_invalid_frame`, it opens a connection, sends an invalid frame which
            // fails at the codec stage and never reaches the transform.
            // Since the transform has never been reached, the chain/transform does not fail and
            // chain flush is reached when the connection is closed by the client.
            EventMatcher::new()
                .with_level(Level::Error)
                .with_target("shotover::server")
                .with_message(&format!(
                    r#"encountered an error when flushing the chain valkey for shutdown

Caused by:
    0: ValkeySinkSingle transform failed
    1: Failed to connect to destination 127.0.0.1:1111
    2: Connection refused (os error {CONNECTION_REFUSED_OS_ERROR})"#
                ))
                .with_count(Count::Times(1)),
            invalid_frame_event(),
        ])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_cluster_sink() {
    test_helpers::cert::generate_valkey_test_certs();

    let _compose = docker_compose("tests/test-configs/valkey/cluster-tls/docker-compose.yaml");
    let shotover = shotover_process(
        "tests/test-configs/valkey/cluster-tls/topology-no-source-encryption.yaml",
    )
    .start()
    .await;

    // let mut connection = valkey_connection::new_async("127.0.0.1", 6379).await;
    let mut connection = ValkeyConnectionCreator {
        address: "127.0.0.1".into(),
        port: 6379,
        tls: false,
    }
    .new_sync();
    let mut flusher = Flusher::new_cluster_tls().await;

    run_all_cluster_hiding(&mut connection, &mut flusher).await;
    test_cluster_ports_rewrite_slots(&mut connection, 6379).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_source_and_tls_single_sink() {
    test_helpers::cert::generate_valkey_test_certs();

    {
        let _compose = docker_compose("tests/test-configs/valkey/tls/docker-compose.yaml");
        let shotover = shotover_process("tests/test-configs/valkey/tls/topology.yaml")
            .start()
            .await;

        // let connection = || valkey_connection::new_async_tls("127.0.0.1", 6379);
        let connection_creator = ValkeyConnectionCreator {
            address: "127.0.0.1".into(),
            port: 6379,
            tls: true,
        };

        let mut flusher = Flusher::new_single_connection(
            // valkey_connection::new_async_tls("127.0.0.1", 6379).await,
            ValkeyConnectionCreator {
                address: "127.0.0.1".into(),
                port: 6379,
                tls: true,
            }
            .build(ValkeyDriver::Sync)
            .await,
        )
        .await;

        run_all(&connection_creator, &mut flusher).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    // Quick test to verify client authentication disabling works
    {
        let _compose =
            docker_compose("tests/test-configs/valkey/tls-no-client-auth/docker-compose.yaml");
        let shotover =
            shotover_process("tests/test-configs/valkey/tls-no-client-auth/topology.yaml")
                .start()
                .await;

        // let mut connection = valkey_connection::new_async_tls("127.0.0.1", 6379).await;
        let mut connection = ValkeyConnectionCreator {
            address: "127.0.0.1".into(),
            port: 6379,
            tls: true,
        }
        .new_sync();
        test_cluster_basics(&mut connection).await;
        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    // Quick test to verify `verify-hostname: false` works
    test_helpers::cert::generate_test_certs_with_bad_san(Path::new(
        "tests/test-configs/valkey/tls/certs",
    ));
    {
        let _compose =
            docker_compose("tests/test-configs/valkey/tls-no-verify-hostname/docker-compose.yaml");
        let shotover =
            shotover_process("tests/test-configs/valkey/tls-no-verify-hostname/topology.yaml")
                .start()
                .await;

        // let mut connection = valkey_connection::new_async_tls("127.0.0.1", 6379).await;
        let mut connection = ValkeyConnectionCreator {
            address: "127.0.0.1".into(),
            port: 6379,
            tls: true,
        }
        .new_sync();
        test_cluster_basics(&mut connection).await;
        shotover.shutdown_and_then_consume_events(&[]).await;
    }
}

// #[tokio::test(flavor = "multi_thread")]
// async fn cluster_ports_rewrite() {
//     let _compose =
//         docker_compose("tests/test-configs/valkey/cluster-ports-rewrite/docker-compose.yaml");
//     let shotover =
//         shotover_process("tests/test-configs/valkey/cluster-ports-rewrite/topology.yaml")
//             .start()
//             .await;
//
//     let mut connection = valkey_connection::new_async("127.0.0.1", 6380).await;
//     let mut flusher =
//         Flusher::new_single_connection(valkey_connection::new_async("127.0.0.1", 6380).await).await;
//
//     run_all_cluster_hiding(&mut connection, &mut flusher).await;
//
//     test_cluster_ports_rewrite_slots(&mut connection, 6380).await;
//     test_cluster_ports_rewrite_nodes(&mut connection, 6380).await;
//
//     shotover.shutdown_and_then_consume_events(&[]).await;
// }

// #[tokio::test(flavor = "multi_thread")]
// async fn cluster_auth() {
//     let _compose = docker_compose("tests/test-configs/valkey/cluster-auth/docker-compose.yaml");
//     let shotover = shotover_process("tests/test-configs/valkey/cluster-auth/topology.yaml")
//         .start()
//         .await;
//     let mut connection = valkey_connection::new_async("127.0.0.1", 6379).await;
//
//     test_auth(&mut connection).await;
//     let connection = || valkey_connection::new_async("127.0.0.1", 6379);
//     test_auth_isolation(&connection).await;
//
//     shotover.shutdown_and_then_consume_events(&[]).await;
// }

#[tokio::test(flavor = "multi_thread")]
async fn cluster_hiding() {
    let _compose = docker_compose("tests/test-configs/valkey/cluster-hiding/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/valkey/cluster-hiding/topology.yaml")
        .start()
        .await;

    // let mut connection = valkey_connection::new_async("127.0.0.1", 6379).await;
    let mut connection = ValkeyConnectionCreator {
        address: "127.0.0.1".into(),
        port: 6379,
        tls: true,
    }
    .new_sync();
    let connection = &mut connection;
    let mut flusher = Flusher::new_cluster().await;

    run_all_cluster_hiding(connection, &mut flusher).await;
    test_cluster_ports_rewrite_slots(connection, 6379).await;
    test_cluster_ports_rewrite_nodes(connection, 6379).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_handling() {
    let _compose = docker_compose("tests/test-configs/valkey/cluster-handling/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/valkey/cluster-handling/topology.yaml")
        .start()
        .await;

    // let mut connection = valkey_connection::new_async("127.0.0.1", 6379).await;
    let mut connection = ValkeyConnectionCreator {
        address: "127.0.0.1".into(),
        port: 6379,
        tls: false,
    }
    .new_sync();
    let connection = &mut connection;

    let mut flusher = Flusher::new_cluster().await;

    run_all_cluster_handling(connection, &mut flusher).await;
    test_cluster_ports_rewrite_slots(connection, 6379).await;
    test_cluster_ports_rewrite_nodes(connection, 6379).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

// #[tokio::test(flavor = "multi_thread")]
// async fn cluster_dr() {
//     let _compose = docker_compose("tests/test-configs/valkey/cluster-dr/docker-compose.yaml");
//
//     let nodes = vec![
//         "redis://127.0.0.1:2120/",
//         "redis://127.0.0.1:2121/",
//         "redis://127.0.0.1:2122/",
//         "redis://127.0.0.1:2123/",
//         "redis://127.0.0.1:2124/",
//         "redis://127.0.0.1:2125/",
//     ];
//     let client = redis::cluster::ClusterClientBuilder::new(nodes)
//         .password("shotover".to_string())
//         .build()
//         .unwrap();
//     let mut replication_connection = client.get_connection().unwrap();
//
//     // test coalesce sends messages on shotover shutdown
//     {
//         let shotover = shotover_process("tests/test-configs/valkey/cluster-dr/topology.yaml")
//             .start()
//             .await;
//         let mut connection = valkey_connection::new_async("127.0.0.1", 6379).await;
//         redis::cmd("AUTH")
//             .arg("default")
//             .arg("shotover")
//             .query_async::<_, ()>(&mut connection)
//             .await
//             .unwrap();
//
//         redis::cmd("SET")
//             .arg("key1")
//             .arg(42)
//             .query_async::<_, ()>(&mut connection)
//             .await
//             .unwrap();
//         redis::cmd("SET")
//             .arg("key2")
//             .arg(358)
//             .query_async::<_, ()>(&mut connection)
//             .await
//             .unwrap();
//
//         shotover.shutdown_and_then_consume_events(&[]).await;
//     }
//
//     sleep(Duration::from_secs(1));
//     assert_eq!(replication_connection.get::<&str, i32>("key1").unwrap(), 42);
//     assert_eq!(
//         replication_connection.get::<&str, i32>("key2").unwrap(),
//         358
//     );
//
//     let shotover = shotover_process("tests/test-configs/valkey/cluster-dr/topology.yaml")
//         .start()
//         .await;
//
//     async fn new_connection() -> Connection {
//         let mut connection = valkey_connection::new_async("127.0.0.1", 6379).await;
//
//         redis::cmd("AUTH")
//             .arg("default")
//             .arg("shotover")
//             .query_async::<_, ()>(&mut connection)
//             .await
//             .unwrap();
//
//         connection
//     }
//     let mut connection = new_connection().await;
//     let mut flusher = Flusher::new_single_connection(new_connection().await).await;
//
//     test_cluster_replication(&mut connection, &mut replication_connection).await;
//     test_dr_auth().await;
//     run_all_cluster_hiding(&mut connection, &mut flusher).await;
//
//     shotover.shutdown_and_then_consume_events(&[]).await;
// }

pub async fn assert_failed_requests_metric_is_incremented_on_error_response() {
    let shotover = shotover_process("tests/test-configs/valkey/passthrough/topology.yaml")
        .start()
        .await;
    let mut connection = valkey_connection::new_async("127.0.0.1", 6379).await;

    redis::cmd("INVALID_COMMAND")
        .arg("foo")
        .query_async::<_, ()>(&mut connection)
        .await
        .unwrap_err();

    // Valkey client driver initialization sends 2 CLIENT SETINFO commands which trigger 2 errors
    // because those commands are not available in the currently used valkey version.
    assert_metrics_key_value(
        r#"shotover_failed_requests_count{chain="valkey",transform="ValkeySinkSingle"}"#,
        "3",
    )
    .await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}
