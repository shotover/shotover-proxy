use crate::shotover_process;
use redis::{Client, Commands};
use test_helpers::docker_compose::docker_compose;
use test_helpers::shotover_process::{EventMatcher, Level};

/// Helper function to verify comprehensive Valkey connection functionality
fn assert_valkey_connection_works(
    connection: &mut redis::Connection,
    expected_counter: Option<i32>,
    expected_data: &[(&str, &str)],
) -> Result<(), Box<dyn std::error::Error>> {
    // Test basic PING
    let pong: String = redis::cmd("PING").query(connection)?;
    assert_eq!(pong, "PONG");

    // Test basic SET/GET operation
    let test_key = "test_key";
    let test_value = "test_connection_works";
    let _: () = connection.set(test_key, test_value)?;
    let result: String = connection.get(test_key)?;
    assert_eq!(result, test_value);

    // Test counter if expected value is provided
    if let Some(expected) = expected_counter {
        let counter_value: i32 = connection.incr("counter", 1)?;
        assert_eq!(
            counter_value, expected,
            "Counter value should be {expected} but was {counter_value}"
        );
    }

    // Test data persistence for specific keys
    for (key, expected_value) in expected_data {
        let actual_value: String = connection.get(*key)?;
        assert_eq!(
            actual_value, *expected_value,
            "Key '{key}' should contain '{expected_value}' but contained '{actual_value}'"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_hotreload_basic_valkey_connection() {
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");
    let shotover_process = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con = client.get_connection().unwrap();

    assert_valkey_connection_works(&mut con, None, &[]).unwrap();

    shotover_process.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
async fn test_dual_shotover_instances_with_valkey() {
    let socket_path = "/tmp/test-hotreload-fd-transfer.sock";
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");

    let shotover_old = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_log_name("shot_old")
        .with_hotreload_socket_path(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Establish connection to old instance and store test data
    let client_old = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_old = client_old.get_connection().unwrap();

    // Store data in the old instance
    let _: () = con_old
        .set("persistent_key", "data_from_old_instance")
        .unwrap();
    let _: () = con_old.set("counter", 1).unwrap();

    // Verify data is accessible through old instance
    let stored_value: String = con_old.get("persistent_key").unwrap();
    assert_eq!(stored_value, "data_from_old_instance");

    // Now start the new instance that will request hot reload
    let shotover_new = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_log_name("shot_new")
        .with_hotreload_socket_path("/tmp/shotover-new.sock") // Different socket for new instance
        .with_hotreload_from_socket(socket_path) // Request handoff from old instance
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .expect_startup_events(vec![
            EventMatcher::new()
                .with_level(Level::Info)
                .with_target("shotover::hot_reload::client")
                .with_message("Old Shotover Instance acknowledged shutdown request"),
        ])
        .start()
        .await;

    let client_new = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_new = client_new
        .get_connection()
        .expect("Failed to connect to new instance");

    // Verify that data stored in old instance is still accessible
    let persistent_value: String = con_new.get("persistent_key").unwrap();
    assert_eq!(
        persistent_value, "data_from_old_instance",
        "Data should persist after hot reload socket handoff"
    );

    // Verify new instance can handle new operations
    let _: () = con_new.set("new_key", "data_from_new_instance").unwrap();

    // Verify basic functionality on new connection
    assert_valkey_connection_works(&mut con_new, Some(2), &[]).unwrap();

    // Test setting and getting additional keys through new connection
    let _: () = con_new
        .set("post_reload", "value_set_after_reload")
        .unwrap();

    drop(con_old);
    drop(con_new);

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let old_events = shotover_old.consume_remaining_events(&[]).await;
    old_events.assert_contains(
        &EventMatcher::new()
            .with_level(Level::Info)
            .with_target("shotover::hot_reload::server")
            .with_message("Received ShutdownOldInstance request, Triggering Shutdown"),
    );
    old_events.assert_contains(
        &EventMatcher::new()
            .with_level(Level::Info)
            .with_target("shotover::runner")
            .with_message("Shotover was shutdown cleanly."),
    );

    // Open a new connection after shutting down old shotover to verify hot reload occurred
    let client_after_shutdown = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_after_shutdown = client_after_shutdown
        .get_connection()
        .expect("Failed to connect after old shotover shutdown - hot reload may have failed");

    // Verify that all expected data persists after old shotover shutdown and test final state
    let _: () = con_after_shutdown
        .set("post_handoff_key", "post_handoff_value")
        .unwrap();

    assert_valkey_connection_works(
        &mut con_after_shutdown,
        Some(3),
        &[
            ("persistent_key", "data_from_old_instance"),
            ("new_key", "data_from_new_instance"),
            ("post_reload", "value_set_after_reload"),
            ("post_handoff_key", "post_handoff_value"),
        ],
    )
    .unwrap();

    // Final cleanup
    shotover_new.shutdown_and_then_consume_events(&[]).await;
}
