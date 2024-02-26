#[cfg(feature = "rdkafka-driver-tests")]
use crate::shotover_process;
#[cfg(feature = "rdkafka-driver-tests")]
use std::time::Duration;
#[cfg(feature = "rdkafka-driver-tests")]
use test_helpers::docker_compose::docker_compose;

#[cfg(feature = "rdkafka-driver-tests")]
mod test_cases;

#[cfg(feature = "rdkafka-driver-tests")]
use test_helpers::connection::kafka::KafkaConnectionBuilder;

#[cfg(feature = "rdkafka-driver-tests")]
#[tokio::test]
async fn passthrough_standard() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough/topology.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new("127.0.0.1:9192");
    test_cases::basic(connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[cfg(feature = "rdkafka-driver-tests")]
#[tokio::test]
async fn passthrough_tls() {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough-tls/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough-tls/topology.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new("127.0.0.1:9192");
    test_cases::basic(connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[cfg(feature = "rdkafka-driver-tests")]
#[tokio::test]
async fn cluster_tls() {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-tls/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/cluster-tls/topology.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new("127.0.0.1:9192");
    test_cases::basic(connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[cfg(feature = "rdkafka-driver-tests")]
#[tokio::test]
async fn passthrough_encode() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough/topology-encode.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new("127.0.0.1:9192");
    test_cases::basic(connection_builder).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "rdkafka-driver-tests")]
#[tokio::test]
async fn passthrough_sasl() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough-sasl/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough-sasl/topology.yaml")
        .start()
        .await;

    let connection_builder =
        KafkaConnectionBuilder::new("127.0.0.1:9192").use_sasl("user", "password");
    test_cases::basic(connection_builder).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "rdkafka-driver-tests")]
#[tokio::test]
async fn passthrough_sasl_encode() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough-sasl/docker-compose.yaml");
    let shotover =
        shotover_process("tests/test-configs/kafka/passthrough-sasl/topology-encode.yaml")
            .start()
            .await;

    let connection_builder =
        KafkaConnectionBuilder::new("127.0.0.1:9192").use_sasl("user", "password");
    test_cases::basic(connection_builder).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "rdkafka-driver-tests")]
#[tokio::test]
async fn cluster_1_rack_single_shotover() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-1-rack/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/cluster-1-rack/topology-single.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new("127.0.0.1:9192");
    test_cases::basic(connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[cfg(feature = "rdkafka-driver-tests")]
#[tokio::test]
async fn cluster_1_rack_multi_shotover() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-1-rack/docker-compose.yaml");
    let mut shotovers = vec![];
    for i in 1..4 {
        shotovers.push(
            shotover_process(&format!(
                "tests/test-configs/kafka/cluster-1-rack/topology{i}.yaml"
            ))
            .with_config(&format!(
                "tests/test-configs/shotover-config/config{i}.yaml"
            ))
            .with_log_name(&format!("shotover{i}"))
            .start()
            .await,
        );
    }

    let connection_builder = KafkaConnectionBuilder::new("127.0.0.1:9192");
    test_cases::basic(connection_builder).await;

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

#[cfg(feature = "rdkafka-driver-tests")]
#[tokio::test]
async fn cluster_2_racks_single_shotover() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-2-racks/docker-compose.yaml");
    let shotover =
        shotover_process("tests/test-configs/kafka/cluster-2-racks/topology-single.yaml")
            .start()
            .await;

    let connection_builder = KafkaConnectionBuilder::new("127.0.0.1:9192");
    test_cases::basic(connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[cfg(feature = "rdkafka-driver-tests")]
#[tokio::test]
async fn cluster_2_racks_multi_shotover() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-2-racks/docker-compose.yaml");

    // One shotover instance per rack
    let mut shotovers = vec![];
    for i in 1..3 {
        shotovers.push(
            shotover_process(&format!(
                "tests/test-configs/kafka/cluster-2-racks/topology-rack{i}.yaml"
            ))
            .with_config(&format!(
                "tests/test-configs/shotover-config/config{i}.yaml"
            ))
            .with_log_name(&format!("shotover{i}"))
            .start()
            .await,
        );
    }

    let connection_builder = KafkaConnectionBuilder::new("127.0.0.1:9192");
    test_cases::basic(connection_builder).await;

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}
