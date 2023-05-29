use crate::redis_int_tests::assert::assert_ok;
use serial_test::serial;
use test_helpers::connection::redis_connection;
use test_helpers::docker_compose::docker_compose;
use test_helpers::shotover_process::ShotoverProcessBuilder;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn log_to_file() {
    let _compose = docker_compose("tests/test-configs/log-to-file/docker-compose.yaml");
    let shotover =
        ShotoverProcessBuilder::new_with_topology("tests/test-configs/log-to-file/topology.yaml")
            .start()
            .await;

    let mut connection = redis_connection::new_async(6379).await;

    assert_ok(redis::cmd("SET").arg("foo").arg(42), &mut connection).await;
    // skip connection 1 as that comes from the redis_connection::new_async creating a dummy connection to the server to see if its awake yet
    let request = std::fs::read("message-log/2/requests/message1.bin").unwrap();
    assert_eq!(
        request,
        "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$2\r\n42\r\n".as_bytes()
    );

    let response = std::fs::read("message-log/2/responses/message1.bin").unwrap();
    assert_eq!(response, "+OK\r\n".as_bytes());

    shotover.shutdown_and_then_consume_events(&[]).await;

    std::fs::remove_dir_all("message-log").unwrap();
}
