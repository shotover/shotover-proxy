mod test_cases;

use crate::shotover_process;
use pretty_assertions::assert_eq;
use rstest::rstest;
use std::time::Duration;
use std::time::Instant;
use test_cases::produce_consume_partitions1;
use test_cases::produce_consume_partitions3;
use test_cases::{assert_topic_creation_is_denied_due_to_acl, setup_basic_user_acls};
use test_helpers::connection::kafka::node::run_node_smoke_test_scram;
use test_helpers::connection::kafka::python::run_python_bad_auth_sasl_scram;
use test_helpers::connection::kafka::python::run_python_smoke_test_sasl_scram;
use test_helpers::connection::kafka::{KafkaConnectionBuilder, KafkaDriver};
use test_helpers::docker_compose::docker_compose;
use test_helpers::shotover_process::{Count, EventMatcher};
use tokio_bin_process::event::Level;

#[rstest]
#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn passthrough_standard(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough/topology.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");
    test_cases::standard_test_suite(&connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[tokio::test]
async fn passthrough_nodejs_and_python() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough/topology.yaml")
        .start()
        .await;

    test_helpers::connection::kafka::node::run_node_smoke_test("127.0.0.1:9192").await;
    test_helpers::connection::kafka::python::run_python_smoke_test("127.0.0.1:9192").await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[rstest]
#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn passthrough_tls(#[case] driver: KafkaDriver) {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough-tls/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough-tls/topology.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");
    test_cases::standard_test_suite(&connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[rstest]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn passthrough_mtls(#[case] driver: KafkaDriver) {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough-mtls/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough-mtls/topology.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192")
        .use_tls("tests/test-configs/kafka/tls/certs");
    test_cases::standard_test_suite(&connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[rstest]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_tls(#[case] driver: KafkaDriver) {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-tls/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/cluster-tls/topology.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192")
        .use_tls("tests/test-configs/kafka/tls/certs");
    test_cases::cluster_test_suite(&connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[rstest]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_mtls(#[case] driver: KafkaDriver) {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-mtls/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/cluster-mtls/topology.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192")
        .use_tls("tests/test-configs/kafka/tls/certs");
    test_cases::cluster_test_suite(&connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn passthrough_encode(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough/topology-encode.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");
    test_cases::standard_test_suite(&connection_builder).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn passthrough_sasl_plain(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough-sasl-plain/docker-compose.yaml");
    let shotover =
        shotover_process("tests/test-configs/kafka/passthrough-sasl-plain/topology.yaml")
            .start()
            .await;

    let connection_builder =
        KafkaConnectionBuilder::new(driver, "127.0.0.1:9192").use_sasl_plain("user", "password");
    test_cases::standard_test_suite(&connection_builder).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
#[tokio::test]
async fn passthrough_sasl_plain_python() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough-sasl-plain/docker-compose.yaml");
    let shotover =
        shotover_process("tests/test-configs/kafka/passthrough-sasl-plain/topology.yaml")
            .start()
            .await;

    test_helpers::connection::kafka::python::run_python_smoke_test_sasl_plain(
        "127.0.0.1:9192",
        "user",
        "password",
    )
    .await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn passthrough_sasl_scram_and_encode(#[case] driver: KafkaDriver) {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough-sasl-scram/docker-compose.yaml");
    let shotover =
        shotover_process("tests/test-configs/kafka/passthrough-sasl-scram/topology.yaml")
            .start()
            .await;

    let connection_builder =
        KafkaConnectionBuilder::new(driver, "127.0.0.1:9192").use_sasl_scram("user", "password");
    test_cases::standard_test_suite(&connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

/// This test demonstrates that kafka's SASL SCRAM implementation does not perform channel binding.
/// https://en.wikipedia.org/wiki/Salted_Challenge_Response_Authentication_Mechanism#Channel_binding
/// This is shown because we have the client performing SCRAM over a plaintext connection while the broker is receiving SCRAM over a TLS connection.
#[rstest]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn single_sasl_scram_plaintext_source_tls_sink(#[case] driver: KafkaDriver) {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose = docker_compose(
        "tests/test-configs/kafka/single-sasl-scram-plaintext-source-tls-sink/docker-compose.yaml",
    );
    let shotover = shotover_process(
        "tests/test-configs/kafka/single-sasl-scram-plaintext-source-tls-sink/topology.yaml",
    )
    .start()
    .await;

    let connection_builder =
        KafkaConnectionBuilder::new(driver, "127.0.0.1:9192").use_sasl_scram("user", "password");
    test_cases::standard_test_suite(&connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[rstest]
#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_1_rack_single_shotover(#[case] driver: KafkaDriver) {
    let docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-1-rack/docker-compose.yaml");

    {
        let shotover =
            shotover_process("tests/test-configs/kafka/cluster-1-rack/topology-single.yaml")
                .start()
                .await;

        let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");
        test_cases::cluster_test_suite(&connection_builder).await;
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }

    {
        let shotover =
            shotover_process("tests/test-configs/kafka/cluster-1-rack/topology-single.yaml")
                .start()
                .await;
        let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");

        test_cases::produce_consume_partitions1_kafka_node_goes_down(
            driver,
            &docker_compose,
            &connection_builder,
            "kafka_node_goes_down_test",
        )
        .await;

        // Shotover can reasonably hit many kinds of errors due to a kafka node down so ignore all of them.
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[
                EventMatcher::new()
                    .with_level(Level::Error)
                    .with_count(Count::Any),
                EventMatcher::new()
                    .with_level(Level::Warn)
                    .with_count(Count::Any),
            ]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

#[rstest]
#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_1_rack_single_shotover_broker_idle_timeout(#[case] driver: KafkaDriver) {
    let _docker_compose = docker_compose(
        "tests/test-configs/kafka/cluster-1-rack/docker-compose-short-idle-timeout.yaml",
    );

    let shotover = shotover_process("tests/test-configs/kafka/cluster-1-rack/topology-single.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");

    // We do not run the regular test suite since there is a race condition where the timeout
    // could occur between checking if the connection is live and sending a request.
    // In regular kafka usage this is acceptable, the client will just retry.
    // But for an integration test this would lead to flakey tests which is unacceptable.
    //
    // So instead we rely on a test case hits the timeout with plenty of buffer to avoid the race condition.
    test_cases::test_broker_idle_timeout(&connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[rstest]
#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_1_rack_multi_shotover(#[case] driver: KafkaDriver) {
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

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");
    test_cases::cluster_test_suite(&connection_builder).await;

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&multi_shotover_events()),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

#[rstest]
// #[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))] //CPP driver may cause flaky tests.
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_1_rack_multi_shotover_with_1_shotover_down(#[case] driver: KafkaDriver) {
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

    // Wait for check_shotover_peers to start
    tokio::time::sleep(Duration::from_secs(15)).await;

    // produce and consume messages, kill 1 shotover node and produce and consume more messages
    let connection_builder = KafkaConnectionBuilder::new(driver, "localhost:9192");
    let shotover_nodes_to_kill: Vec<_> = shotovers.drain(0..1).collect();
    test_cases::produce_consume_partitions1_shotover_nodes_go_down(
        shotover_nodes_to_kill,
        &connection_builder,
        "shotover_node_goes_down_test",
    )
    .await;

    // create a new connection and produce and consume messages
    let new_connection_builder = KafkaConnectionBuilder::new(driver, "localhost:9193");
    test_cases::cluster_test_suite_with_lost_shotover_node(&new_connection_builder).await;

    let mut expected_events = multi_shotover_events();
    // Other shotover nodes should detect the killed node at least once
    expected_events.push(
        EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::kafka::sink_cluster::shotover_node")
            .with_message(r#"Shotover peer 127.0.0.1:9191 is down"#)
            .with_count(Count::GreaterThanOrEqual(1)),
    );

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&expected_events),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

#[rstest]
// #[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))] //CPP driver may cause flaky tests.
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_3_racks_multi_shotover_with_2_shotover_down(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-3-racks/docker-compose.yaml");
    let mut shotovers = vec![];
    for i in 1..4 {
        shotovers.push(
            shotover_process(&format!(
                "tests/test-configs/kafka/cluster-3-racks/topology-rack{i}.yaml"
            ))
            .with_config(&format!(
                "tests/test-configs/shotover-config/config{i}.yaml"
            ))
            .with_log_name(&format!("shotover{i}"))
            .start()
            .await,
        );
    }

    // Wait for check_shotover_peers to start
    tokio::time::sleep(Duration::from_secs(15)).await;

    // produce and consume messages, kill 2 shotover nodes and produce and consume more messages
    let connection_builder = KafkaConnectionBuilder::new(driver, "localhost:9193");
    let shotover_nodes_to_kill: Vec<_> = shotovers.drain(0..2).collect();
    test_cases::produce_consume_partitions1_shotover_nodes_go_down(
        shotover_nodes_to_kill,
        &connection_builder,
        "shotover_nodes_go_down_test",
    )
    .await;

    // create a new connection and produce and consume messages
    let new_connection_builder = KafkaConnectionBuilder::new(driver, "localhost:9193");
    test_cases::cluster_test_suite_with_lost_shotover_node(&new_connection_builder).await;

    let mut expected_events = multi_shotover_events();
    // The UP shotover node should detect the killed nodes at least once
    for i in 1..3 {
        expected_events.push(
            EventMatcher::new()
                .with_level(Level::Warn)
                .with_target("shotover::transforms::kafka::sink_cluster::shotover_node")
                .with_message(&format!(r#"Shotover peer localhost:919{i} is down"#))
                .with_count(Count::GreaterThanOrEqual(1)),
        );
    }

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&expected_events),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

#[rstest]
#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_3_racks_multi_shotover_with_1_shotover_missing(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-3-racks/docker-compose.yaml");
    let mut shotovers = vec![];
    // Only start shotover1/2 and leave shotover3 missing
    for i in 1..3 {
        shotovers.push(
            shotover_process(&format!(
                "tests/test-configs/kafka/cluster-3-racks/topology-rack{i}.yaml"
            ))
            .with_config(&format!(
                "tests/test-configs/shotover-config/config{i}.yaml"
            ))
            .with_log_name(&format!("shotover{i}"))
            .start()
            .await,
        );
    }

    // Wait for check_shotover_peers to start
    tokio::time::sleep(Duration::from_secs(15)).await;

    // Send some produce and consume requests
    let connection_builder = KafkaConnectionBuilder::new(driver, "localhost:9192");
    test_cases::cluster_test_suite_with_lost_shotover_node(&connection_builder).await;

    let mut expected_events = multi_shotover_events();
    // Other shotover nodes should detect the missing node at least once
    expected_events.push(
        EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::kafka::sink_cluster::shotover_node")
            .with_message(r#"Shotover peer localhost:9193 is down"#)
            .with_count(Count::GreaterThanOrEqual(1)),
    );

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&expected_events),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

#[rstest]
#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_2_racks_multi_shotover(#[case] driver: KafkaDriver) {
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

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");
    test_cases::cluster_test_suite(&connection_builder).await;

    #[allow(irrefutable_let_patterns)]
    if let KafkaDriver::Java = driver {
        // describeLogDirs is only on java driver
        test_cases::describe_log_dirs(&connection_builder).await;
                
        // new consumer group protocol is only on java driver
        test_cases::produce_consume_partitions_new_consumer_group_protocol(
            &connection_builder,
            "partitions3_new_consumer_group_protocol",
        )
        .await;
    }

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&multi_shotover_events()),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

#[rstest]
//#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))] // CPP driver does not support scram
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_sasl_scram_single_shotover(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-sasl-scram/docker-compose.yaml");

    let shotover =
        shotover_process("tests/test-configs/kafka/cluster-sasl-scram/topology-single.yaml")
            .start()
            .await;

    let connection_builder =
        KafkaConnectionBuilder::new(driver, "127.0.0.1:9192").use_sasl_scram("user", "password");

    assert_eq!(
        connection_builder.assert_admin_error().await.to_string(),
        match driver {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            KafkaDriver::Cpp => panic!("CPP driver does not support SCRAM"),
            KafkaDriver::Java => "org.apache.kafka.common.errors.UnsupportedSaslMechanismException: Client SASL mechanism 'SCRAM-SHA-256' not enabled in the server, enabled mechanisms are [PLAIN]\n"
        }
    );

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[rstest]
//#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))] // CPP driver does not support scram
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_sasl_scram_over_mtls_single_shotover(#[case] driver: KafkaDriver) {
    test_helpers::cert::generate_kafka_test_certs();

    let docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-sasl-scram-over-mtls/docker-compose.yaml");

    // test concurrent connections with different access levels to ensure that:
    // * clients with bad auth are not authorized
    // * tokens are not mixed up
    // * requests are not sent to the super user connection
    {
        let shotover = shotover_process(
            "tests/test-configs/kafka/cluster-sasl-scram-over-mtls/topology-single.yaml",
        )
        .start()
        .await;

        // admin requests sent by admin user are successful
        let connection_super = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192")
            .use_sasl_scram("super_user", "super_password");
        setup_basic_user_acls(&connection_super, "basic_user").await;
        test_cases::cluster_test_suite(&connection_super).await;
        assert_connection_fails_with_incorrect_password(driver, "super_user").await;

        // admin requests sent by basic user are unsuccessful
        let connection_basic = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192")
            .use_sasl_scram("basic_user", "basic_password");
        assert_topic_creation_is_denied_due_to_acl(&connection_basic).await;
        assert_connection_fails_with_incorrect_password(driver, "basic_user").await;

        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }

    // rerun same tests as before with different ordering
    {
        let shotover = shotover_process(
            "tests/test-configs/kafka/cluster-sasl-scram-over-mtls/topology-single.yaml",
        )
        .start()
        .await;

        // admin requests sent by regular user are unsuccessful
        assert_connection_fails_with_incorrect_password(driver, "basic_user").await;
        let connection_basic = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192")
            .use_sasl_scram("basic_user", "basic_password");
        assert_topic_creation_is_denied_due_to_acl(&connection_basic).await;
        assert_connection_fails_with_incorrect_password(driver, "basic_user").await;

        // admin requests sent by admin user are successful
        // admin requests sent by regular user remain unsuccessful
        let connection_super = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192")
            .use_sasl_scram("super_user", "super_password");
        produce_consume_partitions1(&connection_super, "c3220ff0-9390-425d-a56d-9d880a339c8c")
            .await;
        assert_topic_creation_is_denied_due_to_acl(&connection_basic).await;
        assert_connection_fails_with_incorrect_password(driver, "basic_user").await;
        assert_connection_fails_with_incorrect_password(driver, "super_user").await;

        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }

    // Test handling of down kafka nodes.
    {
        let shotover = shotover_process(
            "tests/test-configs/kafka/cluster-sasl-scram-over-mtls/topology-single.yaml",
        )
        .start()
        .await;

        let connection = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192")
            .use_sasl_scram("super_user", "super_password");

        test_cases::produce_consume_partitions1_kafka_node_goes_down(
            driver,
            &docker_compose,
            &connection,
            "kafka_node_goes_down_test",
        )
        .await;

        // Shotover can reasonably hit many kinds of errors due to a kafka node down so ignore all of them.
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[
                EventMatcher::new()
                    .with_level(Level::Error)
                    .with_count(Count::Any),
                EventMatcher::new()
                    .with_level(Level::Warn)
                    .with_count(Count::Any),
            ]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

async fn assert_connection_fails_with_incorrect_password(driver: KafkaDriver, username: &str) {
    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192")
        .use_sasl_scram(username, "not_the_password");
    assert_eq!(
        connection_builder.assert_admin_error().await.to_string(),
        "org.apache.kafka.common.errors.SaslAuthenticationException: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256\n"
    );
}

#[rstest]
#[tokio::test]
async fn cluster_sasl_scram_over_mtls_nodejs_and_python() {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-sasl-scram-over-mtls/docker-compose.yaml");

    {
        let shotover = shotover_process(
            "tests/test-configs/kafka/cluster-sasl-scram-over-mtls/topology-single.yaml",
        )
        .start()
        .await;

        run_node_smoke_test_scram("127.0.0.1:9192", "super_user", "super_password").await;
        run_python_smoke_test_sasl_scram("127.0.0.1:9192", "super_user", "super_password").await;

        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }

    {
        let shotover = shotover_process(
            "tests/test-configs/kafka/cluster-sasl-scram-over-mtls/topology-single.yaml",
        )
        .start()
        .await;

        run_python_bad_auth_sasl_scram("127.0.0.1:9192", "incorrect_user", "super_password").await;
        run_python_bad_auth_sasl_scram("127.0.0.1:9192", "super_user", "incorrect_password").await;

        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[EventMatcher::new()
                .with_level(Level::Error)
                .with_target("shotover::server")
                .with_message(r#"encountered an error when flushing the chain kafka for shutdown

Caused by:
    0: KafkaSinkCluster transform failed
    1: Failed to receive responses (without sending requests)
    2: Outgoing connection had pending requests, those requests/responses are lost so connection recovery cannot be attempted.
    3: Failed to receive from ControlConnection
    4: The other side of this connection closed the connection"#)
                .with_count(Count::Times(2))]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

#[rstest]
//#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))] // CPP driver does not support scram
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_sasl_scram_over_mtls_multi_shotover(#[case] driver: KafkaDriver) {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-sasl-scram-over-mtls/docker-compose.yaml");

    let mut shotovers = vec![];
    for i in 1..4 {
        shotovers.push(
            shotover_process(&format!(
                "tests/test-configs/kafka/cluster-sasl-scram-over-mtls/topology{i}.yaml"
            ))
            .with_config(&format!(
                "tests/test-configs/shotover-config/config{i}.yaml"
            ))
            .with_log_name(&format!("shotover{i}"))
            .start()
            .await,
        );
    }

    let instant = Instant::now();
    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192")
        .use_sasl_scram("super_user", "super_password");
    test_cases::cluster_test_suite(&connection_builder).await;

    // Wait 20s since we started the initial run to ensure that we hit the 15s token lifetime limit
    tokio::time::sleep_until((instant + Duration::from_secs(20)).into()).await;
    test_cases::produce_consume_partitions1(
        &connection_builder,
        "d4f992d1-05c4-4252-b699-509102338519",
    )
    .await;

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&multi_shotover_events()),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

#[rstest]
//#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))] // CPP driver does not support scram
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_sasl_scram_over_mtls_multi_shotover_with_2_shotover_down(
    #[case] driver: KafkaDriver,
) {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-sasl-scram-over-mtls/docker-compose.yaml");

    let mut shotovers = vec![];
    for i in 1..4 {
        shotovers.push(
            shotover_process(&format!(
                "tests/test-configs/kafka/cluster-sasl-scram-over-mtls/topology{i}.yaml"
            ))
            .with_config(&format!(
                "tests/test-configs/shotover-config/config{i}.yaml"
            ))
            .with_log_name(&format!("shotover{i}"))
            .start()
            .await,
        );
    }

    // Wait for check_shotover_peers to start
    tokio::time::sleep(Duration::from_secs(15)).await;

    let instant = Instant::now();
    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9193")
        .use_sasl_scram("super_user", "super_password");
    produce_consume_partitions3(&connection_builder, "partitions3_rf3", 1, 500).await;

    // Wait 20s since we started the initial run to ensure that we hit the 15s token lifetime limit
    tokio::time::sleep_until((instant + Duration::from_secs(20)).into()).await;
    produce_consume_partitions3(&connection_builder, "partitions3_rf3", 1, 500).await;

    // Kill 2 shotover nodes
    for _ in 0..2 {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotovers.remove(0).shutdown_and_then_consume_events(&[]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }

    // Wait for the other shotover node to detect the down nodes
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Send more produce and consume requests
    produce_consume_partitions3(&connection_builder, "partitions3_rf3", 1, 500).await;

    let mut expected_events = multi_shotover_events();
    // The up shotover node should detect the killed nodes at least once
    expected_events.push(
        EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::kafka::sink_cluster::shotover_node")
            .with_message(r#"Shotover peer 127.0.0.1:9191 is down"#)
            .with_count(Count::GreaterThanOrEqual(1)),
    );
    expected_events.push(
        EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::kafka::sink_cluster::shotover_node")
            .with_message(r#"Shotover peer 127.0.0.1:9192 is down"#)
            .with_count(Count::GreaterThanOrEqual(1)),
    );

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&expected_events),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

#[rstest]
#[cfg_attr(feature = "kafka-cpp-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_sasl_plain_multi_shotover(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-sasl-plain/docker-compose.yaml");
    let mut shotovers = vec![];
    for i in 1..4 {
        shotovers.push(
            shotover_process(&format!(
                "tests/test-configs/kafka/cluster-sasl-plain/topology{i}.yaml"
            ))
            .with_config(&format!(
                "tests/test-configs/shotover-config/config{i}.yaml"
            ))
            .with_log_name(&format!("shotover{i}"))
            .start()
            .await,
        );
    }

    let connection_builder =
        KafkaConnectionBuilder::new(driver, "127.0.0.1:9192").use_sasl_plain("user", "password");
    test_cases::cluster_test_suite(&connection_builder).await;

    // Test invalid credentials
    // We perform the regular test suite first in an attempt to catch a scenario
    // where valid credentials are leaked into subsequent connections.
    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192")
        .use_sasl_plain("user", "not_the_password");
    assert_eq!(
        connection_builder.assert_admin_error().await.to_string(),
        match driver {
            #[cfg(feature = "kafka-cpp-driver-tests")]
            KafkaDriver::Cpp => "Admin operation error: OperationTimedOut (Local: Timed out)",
            KafkaDriver::Java => "org.apache.kafka.common.errors.SaslAuthenticationException: Authentication failed: Invalid username or password\n"
        }
    );

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&multi_shotover_events()),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

fn multi_shotover_events() -> Vec<EventMatcher> {
    // Shotover nodes can be detected as down during shutdown.
    // We should ignore "Shotover peer ... is down" for multi-shotover tests where shotover nodes are not killed.
    vec![
        EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::kafka::sink_cluster::shotover_node")
            .with_message(r#"Shotover peer 127.0.0.1:9191 is down"#)
            .with_count(Count::Any),
        EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::kafka::sink_cluster::shotover_node")
            .with_message(r#"Shotover peer 127.0.0.1:9192 is down"#)
            .with_count(Count::Any),
        EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::kafka::sink_cluster::shotover_node")
            .with_message(r#"Shotover peer 127.0.0.1:9193 is down"#)
            .with_count(Count::Any),
        EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::kafka::sink_cluster::shotover_node")
            .with_message(r#"Shotover peer localhost:9191 is down"#)
            .with_count(Count::Any),
        EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::kafka::sink_cluster::shotover_node")
            .with_message(r#"Shotover peer localhost:9192 is down"#)
            .with_count(Count::Any),
    ]
}
