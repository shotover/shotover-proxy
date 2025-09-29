use crate::shotover_process;
use redis::{Client, Commands};
use test_helpers::docker_compose::docker_compose;

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

    let _: () = con.set("test_key", "test_value").unwrap();
    let result: String = con.get("test_key").unwrap();
    assert_eq!(result, "test_value");
    let pong: String = redis::cmd("PING").query(&mut con).unwrap();
    assert_eq!(pong, "PONG");

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

    // Test that the old connection continues to function after hot reload
    let old_connection_test: String = con_old.get("persistent_key").unwrap();
    assert_eq!(
        old_connection_test, "data_from_old_instance",
        "Old connection should continue to function after hot reload"
    );

    // Test that old connection can still perform operations
    let _: () = con_old
        .set("old_connection_key", "old_still_works")
        .unwrap();
    let old_op_result: String = con_old.get("old_connection_key").unwrap();
    assert_eq!(old_op_result, "old_still_works");

    // Tests to ensure con_old continues functioning after hot reload
    let old_pong: String = redis::cmd("PING").query(&mut con_old).unwrap();
    assert_eq!(
        old_pong, "PONG",
        "Old connection should respond to PING after hot reload"
    );

    // Test increment operation on old connection
    let old_incr: i32 = con_old.incr("counter", 10).unwrap();
    assert_eq!(
        old_incr, 11,
        "Old connection should handle increment operations after hot reload"
    );

    // Test setting and getting a new key through old connection
    let _: () = con_old
        .set("old_post_reload", "value_set_after_reload")
        .unwrap();
    let old_post_reload_value: String = con_old.get("old_post_reload").unwrap();
    assert_eq!(
        old_post_reload_value, "value_set_after_reload",
        "Old connection should handle new operations after hot reload"
    );

    // Verify new instance can handle new operations
    let _: () = con_new.set("new_key", "data_from_new_instance").unwrap();
    let new_value: String = con_new.get("new_key").unwrap();
    assert_eq!(new_value, "data_from_new_instance");

    // Test that we can increment the counter
    let counter_value: i32 = con_new.incr("counter", 1).unwrap();
    assert_eq!(
        counter_value, 12,
        "Counter should increment from value set by old instance"
    );

    // Verify ping works
    let pong: String = redis::cmd("PING").query(&mut con_new).unwrap();
    assert_eq!(pong, "PONG");

    // Shutdown old shotover instance
    shotover_old.shutdown_and_then_consume_events(&[]).await;

    // Open a new connection after shutting down old shotover to verify hot reload occurred
    let client_after_old_shutdown = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_after_old_shutdown = client_after_old_shutdown
        .get_connection()
        .expect("Failed to connect after old shotover shutdown - hot reload may have failed");

    // Verify that the new connection can access data that was originally set by the old instance
    let final_check: String = con_after_old_shutdown.get("persistent_key").unwrap();
    assert_eq!(
        final_check, "data_from_old_instance",
        "New connection after old shotover shutdown should still access persistent data"
    );

    // Verify that the new connection can access data set by the new instance
    let new_instance_check: String = con_after_old_shutdown.get("new_key").unwrap();
    assert_eq!(
        new_instance_check, "data_from_new_instance",
        "New connection should access data from new instance"
    );

    // Verify that the new connection can access data set by the old connection after hot reload
    let old_post_reload_check: String = con_after_old_shutdown.get("old_post_reload").unwrap();
    assert_eq!(
        old_post_reload_check, "value_set_after_reload",
        "New connection should access data set by old connection after hot reload"
    );

    // Test that new connection can perform operations
    let _: () = con_after_old_shutdown
        .set("post_handoff_key", "post_handoff_value")
        .unwrap();
    let post_handoff_result: String = con_after_old_shutdown.get("post_handoff_key").unwrap();
    assert_eq!(post_handoff_result, "post_handoff_value");

    // Verify ping works on the post-handoff connection
    let pong_after: String = redis::cmd("PING")
        .query(&mut con_after_old_shutdown)
        .unwrap();
    assert_eq!(pong_after, "PONG");

    // Verify the counter value reflects all operations from both old and new connections
    let final_counter: i32 = con_after_old_shutdown.get("counter").unwrap();
    assert_eq!(
        final_counter, 12,
        "Counter should reflect all increment operations from both old and new connections"
    );

    // Final cleanup
    shotover_new.shutdown_and_then_consume_events(&[]).await;

    // Clean up socket files
    let _ = std::fs::remove_file(socket_path);
    let _ = std::fs::remove_file("/tmp/shotover-new.sock");
}
