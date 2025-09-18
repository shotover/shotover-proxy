/*use crate::shotover_process;
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
    let socket_path = "/tmp/test-hotreload.sock";
    let _compose = docker_compose("tests/test-configs/hotreload/docker-compose.yaml");
    let shotover_a = shotover_process("tests/test-configs/hotreload/topology.yaml")
        .with_hotreload(true)
        .with_hotreload_socket_path(socket_path)
        .with_config("tests/test-configs/shotover-config/config_metrics_disabled.yaml")
        .start()
        .await;
    let client_a = Client::open("valkey://127.0.0.1:6380").unwrap();
    let mut con_a = client_a.get_connection().unwrap();
    let _: () = con_a.set("key_from_a", "value_from_a").unwrap();

    //Second shotover
    let shotover_b = shotover_process("tests/test-configs/hotreload/topology-alt.yaml")
        .with_hotreload_from_socket(socket_path)
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
*/
