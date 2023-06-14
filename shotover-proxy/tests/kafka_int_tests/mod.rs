use serial_test::serial;
use std::time::Duration;
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

    test_cases::basic("127.0.0.1:9192").await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
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

    test_cases::basic("127.0.0.1:9192").await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

// This test is a hack so that we can send messages through KafkaSinkCluster before we implement routing.
// As soon as we have implemented routing such that the other cluster tests can send messages this test should be deleted.
#[tokio::test]
#[serial]
async fn cluster_fake() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-fake/docker-compose.yaml");
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/kafka/cluster-fake/topology.yaml",
    )
    .start()
    .await;

    test_cases::basic("127.0.0.1:9192").await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[tokio::test]
#[serial]
async fn cluster_single_shotover() {
    let _docker_compose = docker_compose("tests/test-configs/kafka/cluster/docker-compose.yaml");
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/kafka/cluster/topology-single.yaml",
    )
    .start()
    .await;

    // disabled until routing is implemented
    //test_cases::basic("127.0.0.1:9192").await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[tokio::test]
#[serial]
async fn cluster_multi_shotover() {
    let _docker_compose = docker_compose("tests/test-configs/kafka/cluster/docker-compose.yaml");
    let mut shotovers = vec![];
    for i in 1..4 {
        shotovers.push(
            ShotoverProcessBuilder::new_with_topology(&format!(
                "tests/test-configs/kafka/cluster/topology{i}.yaml"
            ))
            .with_log_name(&format!("shotover{i}"))
            .with_observability_port(9000 + i)
            .start()
            .await,
        );
    }

    // disabled until routing is implemented
    //test_cases::basic("127.0.0.1:9192").await;

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}
