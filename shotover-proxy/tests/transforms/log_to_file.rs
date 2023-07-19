use crate::redis_int_tests::assert::assert_ok;
use crate::shotover_process;
use serial_test::serial;
use test_helpers::connection::redis_connection;
use test_helpers::docker_compose::docker_compose;

#[cfg(feature = "alpha-transforms")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn log_to_file() {
    let _compose = docker_compose("tests/test-configs/log-to-file/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/log-to-file/topology.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    assert_ok(redis::cmd("SET").arg("foo").arg(42), &mut connection).await;
    let request = std::fs::read("message-log/1/requests/message1.bin").unwrap();
    assert_eq!(
        request,
        "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$2\r\n42\r\n".as_bytes()
    );

    let response = std::fs::read("message-log/1/responses/message1.bin").unwrap();
    assert_eq!(response, "+OK\r\n".as_bytes());

    shotover.shutdown_and_then_consume_events(&[]).await;

    std::fs::remove_dir_all("message-log").unwrap();
}
