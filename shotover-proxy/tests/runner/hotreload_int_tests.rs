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

    // Verify new instance can handle new operations
    let _: () = con_new.set("new_key", "data_from_new_instance").unwrap();
    let new_value: String = con_new.get("new_key").unwrap();
    assert_eq!(new_value, "data_from_new_instance");

    // Test that we can increment the counter
    let counter_value: i32 = con_new.incr("counter", 1).unwrap();
    assert_eq!(
        counter_value, 2,
        "Counter should increment from value set by old instance"
    );

    // Verify ping works
    let pong: String = redis::cmd("PING").query(&mut con_new).unwrap();
    assert_eq!(pong, "PONG");

    // Cleanup
    shotover_old.shutdown_and_then_consume_events(&[]).await;
    shotover_new.shutdown_and_then_consume_events(&[]).await;

    // Clean up socket files
    let _ = std::fs::remove_file(socket_path);
    let _ = std::fs::remove_file("/tmp/shotover-new.sock");
}
