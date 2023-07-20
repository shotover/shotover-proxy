#[cfg(feature = "alpha-transforms")]
use crate::shotover_process;
#[cfg(feature = "alpha-transforms")]
use serial_test::serial;
#[cfg(feature = "alpha-transforms")]
use std::time::Duration;
#[cfg(feature = "alpha-transforms")]
use test_helpers::docker_compose::docker_compose;

#[cfg(feature = "alpha-transforms")]
mod test_cases;

#[cfg(feature = "alpha-transforms")]
#[tokio::test]
#[serial]
async fn passthrough_standard() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough/topology.yaml")
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

#[cfg(feature = "alpha-transforms")]
#[tokio::test]
#[serial]
async fn passthrough_encode() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough/topology-encode.yaml")
        .start()
        .await;

    test_cases::basic("127.0.0.1:9192").await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "alpha-transforms")]
#[tokio::test]
#[serial]
async fn cluster_single_shotover() {
    let _docker_compose = docker_compose("tests/test-configs/kafka/cluster/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/cluster/topology-single.yaml")
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

#[cfg(feature = "alpha-transforms")]
#[tokio::test]
#[serial]
async fn cluster_multi_shotover() {
    let _docker_compose = docker_compose("tests/test-configs/kafka/cluster/docker-compose.yaml");
    let mut shotovers = vec![];
    for i in 1..4 {
        shotovers.push(
            shotover_process(&format!(
                "tests/test-configs/kafka/cluster/topology{i}.yaml"
            ))
            .with_log_name(&format!("shotover{i}"))
            .with_observability_port(9000 + i)
            .start()
            .await,
        );
    }

    test_cases::basic("127.0.0.1:9192").await;

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}
