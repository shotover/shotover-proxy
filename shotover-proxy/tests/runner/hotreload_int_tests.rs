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
