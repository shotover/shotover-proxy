use serial_test::serial;
use test_helpers::docker_compose::docker_compose;
use test_helpers::shotover_process::ShotoverProcessBuilder;

mod test_cases;

#[tokio::test]
#[serial]
async fn passthrough_standard() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough/docker-compose.yaml");
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/kafka/passthrough/topology.yaml",
    )
    .start()
    .await;

    test_cases::basic().await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test]
#[serial]
async fn passthrough_encode() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough/docker-compose.yaml");
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/kafka/passthrough/topology-encode.yaml",
    )
    .start()
    .await;

    test_cases::basic().await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}
