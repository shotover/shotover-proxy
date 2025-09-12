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
#[cfg(target_os = "linux")]
async fn test_socket_handoff_with_valkey() {
    let socket_path = "/tmp/test-hotreload-handoff.sock";
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");

    // Start the first Shotover instance with hot reload enabled
    let shotover_a = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_log_name("shot-old")
        .with_hotreload_socket_path(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Create a connection to the first instance and set some data
    let client_a = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_a = client_a.get_connection().unwrap();
    let _: () = con_a.set("existing_key", "existing_value").unwrap();

    println!("First Shotover instance started and data set");

    // Start the second Shotover instance that should take over the same port via hot reload
    // We need to create a topology that uses the same port (6380)
    let shotover_b = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload_from_socket(socket_path)
        .with_log_name("shot-new")
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    println!("Second Shotover instance started with socket handoff");

    // Give some time for the handoff to complete
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Test that existing connection still works (should be handled by first instance)
    let existing_value: String = con_a.get("existing_key").unwrap();
    assert_eq!(existing_value, "existing_value");

    // Create a new connection - this should go to the second instance
    let client_new = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_new = client_new.get_connection().unwrap();

    // Set new data via the new connection (should go to second instance)
    let _: () = con_new.set("new_key", "new_value").unwrap();

    // Both connections should be able to see all data since they hit the same backend
    let new_value_from_old_conn: String = con_a.get("new_key").unwrap();
    assert_eq!(new_value_from_old_conn, "new_value");

    let existing_value_from_new_conn: String = con_new.get("existing_key").unwrap();
    assert_eq!(existing_value_from_new_conn, "existing_value");

    println!("Socket handoff successful - both old and new connections working");

    shotover_a.shutdown_and_then_consume_events(&[]).await;
    shotover_b.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
async fn test_dual_shotover_instances_different_ports() {
    let socket_path = "/tmp/test-hotreload-dual.sock";
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");
    let shotover_a = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_log_name("shot-old")
        .with_hotreload_socket_path(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client_a = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_a = client_a.get_connection().unwrap();
    let _: () = con_a.set("key_from_a", "value_from_a").unwrap();

    //Second shotover on different port (this tests the hot reload communication, not socket handoff)
    let shotover_b = shotover_process("tests/test-configs/hotreload/topology-alt.yaml")
        .with_hotreload_from_socket(socket_path)
        .with_log_name("shot-new")
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client_b = Client::open("valkey://127.0.0.1:6381").unwrap();
    let mut con_b = client_b.get_connection().unwrap();
    let _: () = con_b.set("key_from_b", "value_from_b").unwrap();

    let value_a_from_b: String = con_b.get("key_from_a").unwrap();
    assert_eq!(value_a_from_b, "value_from_a");
    let value_b_from_a: String = con_a.get("key_from_b").unwrap();
    assert_eq!(value_b_from_a, "value_from_b");

    let final_check: String = con_a.get("key_from_a").unwrap();
    assert_eq!(final_check, "value_from_a");

    shotover_a.shutdown_and_then_consume_events(&[]).await;
    shotover_b.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
#[cfg(not(target_os = "linux"))]
async fn test_hotreload_protocol_communication() {
    // On non-Linux systems, we can't test actual socket transfer via pidfd_getfd,
    // but we can test that the hot reload protocol communication works
    let socket_path = "/tmp/test-hotreload-protocol.sock";
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");

    // Start the first Shotover instance with hot reload enabled
    let shotover_a = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_log_name("shot-old")
        .with_hotreload_socket_path(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Test that we can connect to the first instance
    let client_a = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_a = client_a.get_connection().unwrap();
    let _: () = con_a.set("test_key", "test_value").unwrap();

    // Start second instance that attempts hot reload (will get protocol response but fall back to new socket)
    let shotover_b = shotover_process("tests/test-configs/hotreload/topology-alt.yaml")
        .with_hotreload_from_socket(socket_path)
        .with_log_name("shot-new")
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    // Test that second instance is running on its configured port
    let client_b = Client::open("valkey://127.0.0.1:6381").unwrap();
    let mut con_b = client_b.get_connection().unwrap();
    let _: () = con_b.set("test_key_b", "test_value_b").unwrap();

    // Both should be able to access the same backend data
    let value_from_a: String = con_a.get("test_key_b").unwrap();
    assert_eq!(value_from_a, "test_value_b");

    let value_from_b: String = con_b.get("test_key").unwrap();
    assert_eq!(value_from_b, "test_value");

    println!("Hot reload protocol communication successful (socket transfer skipped on non-Linux)");

    shotover_a.shutdown_and_then_consume_events(&[]).await;
    shotover_b.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
async fn test_simple_hotreload_startup_no_db() {
    let socket_path = "/tmp/test-hotreload-simple.sock";

    // Start the first Shotover instance with hot reload enabled - no DB, just listen
    let shotover_a = shotover_process("tests/test-configs/hotreload/topology-simple.yaml")
        .with_hotreload(true)
        .with_log_name("shot-old")
        .with_hotreload_socket_path(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    println!("First Shotover instance started with hot reload enabled");

    // Give it time to fully start
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Start second instance that requests socket handoff (SAME PORT to test socket recreation logic)
    let shotover_b = shotover_process("tests/test-configs/hotreload/topology-simple.yaml")
        .with_hotreload_from_socket(socket_path)
        .with_log_name("shot-new")
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;

    println!("Second Shotover instance started - hot reload communication should occur");

    // Give time for hot reload communication
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    println!("Hot reload startup test completed successfully");

    // Shutdown both instances
    shotover_a.shutdown_and_then_consume_events(&[]).await;
    shotover_b.shutdown_and_then_consume_events(&[]).await;
}
