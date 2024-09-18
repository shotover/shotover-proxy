mod test_cases;

use crate::shotover_process;
use pretty_assertions::assert_eq;
use rstest::rstest;
use std::time::Duration;
use std::time::Instant;
use test_cases::produce_consume_partitions1;
use test_cases::{assert_topic_creation_is_denied_due_to_acl, setup_basic_user_acls};
use test_helpers::connection::kafka::node::run_node_smoke_test_scram;
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
async fn passthrough_nodejs() {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough/topology.yaml")
        .start()
        .await;

    test_helpers::connection::kafka::node::run_node_smoke_test("127.0.0.1:9192").await;

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
            shotover.shutdown_and_then_consume_events(&multi_shotover_events(driver)),
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

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&multi_shotover_events(driver)),
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
async fn cluster_sasl_scram_over_mtls_nodejs() {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-sasl-scram-over-mtls/docker-compose.yaml");
    let shotover = shotover_process(
        "tests/test-configs/kafka/cluster-sasl-scram-over-mtls/topology-single.yaml",
    )
    .start()
    .await;

    run_node_smoke_test_scram("127.0.0.1:9192", "super_user", "super_password").await;

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
            shotover.shutdown_and_then_consume_events(&multi_shotover_events(driver)),
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
            shotover.shutdown_and_then_consume_events(&multi_shotover_events(driver)),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

fn multi_shotover_events(driver: KafkaDriver) -> Vec<EventMatcher> {
    #[allow(irrefutable_let_patterns)]
    if let KafkaDriver::Java = driver {
        // The java driver manages to send requests fast enough that shotover's find_coordinator_of_group method
        // gets a COORDINATOR_NOT_AVAILABLE response from kafka.
        // If the client were connecting directly to kafka it would get this same error, so it doesnt make sense for us to retry on error.
        // Instead we just let shotover pass on the NOT_COORDINATOR error to the client which triggers this warning in the process.
        // So we ignore the warning in this case.
        vec![EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::kafka::sink_cluster")
            .with_message(
                r#"no known coordinator for GroupId("some_group"), routing message to a random broker so that a NOT_COORDINATOR or similar error is returned to the client"#,
            )
            .with_count(Count::Any)]
    } else {
        vec![]
    }
}
