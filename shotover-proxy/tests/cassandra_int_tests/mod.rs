use crate::{CONNECTION_REFUSED_OS_ERROR, shotover_process};
use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType};
use cassandra_protocol::types::cassandra_type::CassandraType;
use cdrs_tokio::frame::events::{
    SchemaChange, SchemaChangeOptions, SchemaChangeTarget, SchemaChangeType, ServerEvent,
};
use fred::rustls::crypto::aws_lc_rs::default_provider;
use futures::Future;
use futures::future::join_all;
use pretty_assertions::assert_eq;
use rstest::rstest;
use rstest_reuse::{self, *};
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{
    ConnectionError, ConnectionPoolError, ConnectionSetupRequestError,
    ConnectionSetupRequestErrorKind, DbError, MetadataError, NewSessionError,
};
use std::net::SocketAddr;
#[cfg(feature = "cassandra-cpp-driver-tests")]
use test_helpers::connection::cassandra::CassandraDriver::Cpp;
use test_helpers::connection::cassandra::Compression;
#[cfg(feature = "alpha-transforms")]
use test_helpers::connection::cassandra::ProtocolVersion;
use test_helpers::connection::cassandra::{
    CassandraConnection, CassandraConnectionBuilder, CassandraDriver, CassandraDriver::Cdrs,
    CassandraDriver::Java, CassandraDriver::Scylla, CqlWsSession, ResultValue, assert_query_result,
    run_query,
};
use test_helpers::connection::valkey_connection::ValkeyConnectionCreator;
use test_helpers::docker_compose::docker_compose;
#[cfg(feature = "alpha-transforms")]
use test_helpers::docker_compose::new_moto;
use test_helpers::shotover_process::{Count, EventMatcher, Level};
use tokio::time::{Duration, timeout};

mod batch_statements;
mod cache;
mod cluster;
mod collections;
mod functions;
mod keyspace;
mod native_types;
mod prepared_statements_all;
mod prepared_statements_simple;
#[cfg(feature = "alpha-transforms")]
mod protect;
mod routing;
mod table;
mod timestamp;
mod udt;

async fn standard_test_suite<Fut>(connection_creator: impl Fn() -> Fut, driver: CassandraDriver)
where
    Fut: Future<Output = CassandraConnection>,
{
    // reuse a single connection a bunch to save time recreating connections
    let connection = connection_creator().await;

    keyspace::test(&connection).await;
    table::test(&connection).await;
    udt::test(&connection).await;
    native_types::test(&connection).await;
    collections::test(&connection, driver).await;
    functions::test(&connection).await;
    prepared_statements_simple::test(&connection, connection_creator, 1).await;
    prepared_statements_all::test(&connection, 1).await;
    batch_statements::test(&connection).await;
    timestamp::test(&connection).await;
}

async fn standard_test_suite_cassandra5<Fut>(
    connection_creator: impl Fn() -> Fut,
    driver: CassandraDriver,
) where
    Fut: Future<Output = CassandraConnection>,
{
    // reuse a single connection a bunch to save time recreating connections
    let connection = connection_creator().await;

    keyspace::test(&connection).await;
    table::test(&connection).await;
    udt::test(&connection).await;
    native_types::test(&connection).await;
    collections::test_cassandra_5(&connection, driver).await;
    functions::test(&connection).await;
    prepared_statements_simple::test(&connection, connection_creator, 1).await;
    prepared_statements_all::test(&connection, 1).await;
    batch_statements::test(&connection).await;
    timestamp::test(&connection).await;
}

async fn standard_test_suite_rf3<Fut>(connection_creator: impl Fn() -> Fut, driver: CassandraDriver)
where
    Fut: Future<Output = CassandraConnection>,
{
    // reuse a single connection a bunch to save time recreating connections
    let connection = connection_creator().await;

    keyspace::test(&connection).await;
    table::test(&connection).await;
    udt::test(&connection).await;
    native_types::test(&connection).await;
    collections::test(&connection, driver).await;
    prepared_statements_simple::test(&connection, &connection_creator, 1).await;
    prepared_statements_simple::test(&connection, &connection_creator, 3).await;
    prepared_statements_all::test(&connection, 1).await;
    prepared_statements_all::test(&connection, 3).await;
    batch_statements::test(&connection).await;
    timestamp::test(&connection).await;
}

#[template]
#[rstest]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::cpp(Cpp))]
#[case::scylla(Scylla)]
#[case::cdrs(Cdrs)]
#[case::java(Java)]
fn all_cassandra_drivers(#[case] driver: CassandraDriver) {}

#[apply(all_cassandra_drivers)]
#[tokio::test(flavor = "multi_thread")]
async fn passthrough_standard(#[case] driver: CassandraDriver) {
    let _compose = docker_compose("tests/test-configs/cassandra/passthrough/docker-compose.yaml");

    let shotover = shotover_process("tests/test-configs/cassandra/passthrough/topology.yaml")
        .start()
        .await;

    let connection = || CassandraConnectionBuilder::new("127.0.0.1", 9042, driver).build();

    standard_test_suite(&connection, driver).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn passthrough_cassandra_down() {
    let shotover = shotover_process("tests/test-configs/cassandra/passthrough/topology.yaml")
        .start()
        .await;

    let err = SessionBuilder::new()
        .known_nodes(["127.0.0.1:9042"])
        .user("cassandra", "cassandra")
        .build()
        .await
        .unwrap_err();
    match err {
        NewSessionError::MetadataError(MetadataError::ConnectionPoolError(
            ConnectionPoolError::Broken {
                last_connection_error:
                    ConnectionError::ConnectionSetupRequestError(ConnectionSetupRequestError {
                        error: ConnectionSetupRequestErrorKind::DbError(DbError::ServerError, err),
                        ..
                    }),
            },
        )) => {
            assert_eq!(
                format!("{err}"),
                format!("Internal shotover (or custom transform) bug: Chain failed to send and/or receive messages, the connection will now be closed.

Caused by:
    0: CassandraSinkSingle transform failed
    1: Failed to connect to destination 127.0.0.1:9043
    2: Connection refused (os error {CONNECTION_REFUSED_OS_ERROR})"
            ));
        }
        _ => panic!("Unexpected error, was {err:?}"),
    }

    shotover
        .shutdown_and_then_consume_events(&[EventMatcher::new()
            .with_level(Level::Error)
            .with_target("shotover::server")
            .with_message(&format!(
                r#"connection was unexpectedly terminated

Caused by:
    0: Chain failed to send and/or receive messages, the connection will now be closed.
    1: CassandraSinkSingle transform failed
    2: Failed to connect to destination 127.0.0.1:9043
    3: Connection refused (os error {CONNECTION_REFUSED_OS_ERROR})"#,
            ))
            .with_count(Count::Times(1))])
        .await;
}

#[cfg(feature = "alpha-transforms")]
#[apply(all_cassandra_drivers)]
#[tokio::test(flavor = "multi_thread")]
async fn passthrough_encode(#[case] driver: CassandraDriver) {
    let _compose = docker_compose("tests/test-configs/cassandra/passthrough/docker-compose.yaml");

    let shotover =
        shotover_process("tests/test-configs/cassandra/passthrough/topology-encode.yaml")
            .start()
            .await;

    let connection = || CassandraConnectionBuilder::new("127.0.0.1", 9042, driver).build();

    standard_test_suite(&connection, driver).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
#[case::scylla(Scylla)]
//#[case::cdrs(Cdrs)] // TODO
#[tokio::test(flavor = "multi_thread")]
async fn source_tls_and_single_tls(#[case] driver: CassandraDriver) {
    test_helpers::cert::generate_cassandra_test_certs();
    let _compose = docker_compose("tests/test-configs/cassandra/tls/docker-compose.yaml");

    let shotover = shotover_process("tests/test-configs/cassandra/tls/topology.yaml")
        .start()
        .await;

    let ca_cert = "tests/test-configs/cassandra/tls/certs/localhost_CA.crt";

    {
        // Run a quick test straight to Cassandra to check our assumptions that Shotover and Cassandra TLS are behaving exactly the same
        let direct_connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
            .with_tls(ca_cert)
            .build()
            .await;
        assert_query_result(
            &direct_connection,
            "SELECT bootstrapped FROM system.local",
            &[&[ResultValue::Varchar("COMPLETED".into())]],
        )
        .await;
    }

    let connection = || {
        CassandraConnectionBuilder::new("127.0.0.1", 9043, driver)
            .with_tls(ca_cert)
            .build()
    };

    standard_test_suite(&connection, driver).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
//#[case::cdrs(Cdrs)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::cpp(Cpp))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
async fn cluster_single_rack_v3(#[case] driver: CassandraDriver) {
    let _compose = docker_compose("tests/test-configs/cassandra/cluster-v3/docker-compose.yaml");

    {
        let shotover =
            shotover_process("tests/test-configs/cassandra/cluster-v3/topology-dummy-peers.yaml")
                .start()
                .await;

        let connection = || async {
            let mut connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
                .build()
                .await;
            connection
                .enable_schema_awaiter("172.16.1.2:9042", None)
                .await;
            connection
        };
        standard_test_suite(&connection, driver).await;
        cluster::single_rack_v3::test_dummy_peers(&connection().await).await;

        routing::test("127.0.0.1", 9042, "172.16.1.2", 9042, driver).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    cluster::single_rack_v3::test_topology_task(None).await;
}

#[rstest]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::cpp(Cpp))]
#[case::scylla(Scylla)]
#[case::cdrs(Cdrs)]
//#[case::java(Java)] // Need to create JVM in builder to avoid recreating for each connection instance.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_single_rack_v4(#[case] driver: CassandraDriver) {
    let mut compose = docker_compose("tests/test-configs/cassandra/cluster-v4/docker-compose.yaml");

    let connection = || async {
        let mut connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
            .build()
            .await;
        connection
            .enable_schema_awaiter("172.16.1.2:9044", None)
            .await;
        connection
    };
    {
        let shotover = shotover_process("tests/test-configs/cassandra/cluster-v4/topology.yaml")
            .start()
            .await;

        standard_test_suite(&connection, driver).await;
        cluster::single_rack_v4::test(&connection().await, driver).await;

        routing::test("127.0.0.1", 9042, "172.16.1.2", 9044, driver).await;

        cluster::single_rack_v4::test_node_going_down(&mut compose, driver).await;

        shotover
            .shutdown_and_then_consume_events(&[
                EventMatcher::new()
                    .with_level(Level::Warn)
                    .with_target("shotover::transforms::cassandra::sink_cluster::node_pool")
                    .with_message(
                        r#"A successful connection to a node was made but attempts to connect to these nodes failed first:
* 172.16.1.3:9044:
    - Failed to create new connection
    - destination 172.16.1.3:9044 did not respond to connection attempt within 3s"#,
                    )
                    .with_count(Count::Any),
            ])
            .await;
    }

    {
        let shotover =
            shotover_process("tests/test-configs/cassandra/cluster-v4/topology-dummy-peers.yaml")
                .start()
                .await;

        cluster::single_rack_v4::test_dummy_peers(&connection().await, driver).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    cluster::single_rack_v4::test_topology_task(None, Some(9044)).await;
}

#[rstest]
//#[case::cdrs(Cdrs)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::cpp(Cpp))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
async fn cluster_multi_rack_1_per_rack(#[case] driver: CassandraDriver) {
    let _compose =
        docker_compose("tests/test-configs/cassandra/cluster-multi-rack/docker-compose.yaml");

    {
        let shotover_rack1 =
            shotover_process("tests/test-configs/cassandra/cluster-multi-rack/topology_rack1.yaml")
                .with_log_name("Rack1")
                .with_config("tests/test-configs/shotover-config/config1.yaml")
                .start()
                .await;
        let shotover_rack2 =
            shotover_process("tests/test-configs/cassandra/cluster-multi-rack/topology_rack2.yaml")
                .with_log_name("Rack2")
                .with_config("tests/test-configs/shotover-config/config2.yaml")
                .start()
                .await;
        let shotover_rack3 =
            shotover_process("tests/test-configs/cassandra/cluster-multi-rack/topology_rack3.yaml")
                .with_log_name("Rack3")
                .with_config("tests/test-configs/shotover-config/config3.yaml")
                .start()
                .await;

        let connection = || async {
            let mut connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
                .build()
                .await;
            connection
                .enable_schema_awaiter("172.16.1.2:9042", None)
                .await;
            connection
        };
        standard_test_suite(&connection, driver).await;
        cluster::multi_rack::test(&connection().await).await;

        shotover_rack1.shutdown_and_then_consume_events(&[]).await;
        shotover_rack2.shutdown_and_then_consume_events(&[]).await;
        shotover_rack3.shutdown_and_then_consume_events(&[]).await;
    }

    let expected_nodes: Vec<(SocketAddr, &'static str)> = vec![
        ("172.16.1.2:9042".parse().unwrap(), "rack1"),
        ("172.16.1.3:9042".parse().unwrap(), "rack2"),
        ("172.16.1.4:9042".parse().unwrap(), "rack3"),
    ];
    cluster::multi_rack::test_topology_task(None, expected_nodes, 128).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_multi_rack_1_per_rack_go_smoke_test() {
    let _compose =
        docker_compose("tests/test-configs/cassandra/cluster-multi-rack/docker-compose.yaml");

    let shotover_rack1 =
        shotover_process("tests/test-configs/cassandra/cluster-multi-rack/topology_rack1.yaml")
            .with_log_name("Rack1")
            .with_config("tests/test-configs/shotover-config/config1.yaml")
            .start()
            .await;
    let shotover_rack2 =
        shotover_process("tests/test-configs/cassandra/cluster-multi-rack/topology_rack2.yaml")
            .with_log_name("Rack2")
            .with_config("tests/test-configs/shotover-config/config2.yaml")
            .start()
            .await;
    let shotover_rack3 =
        shotover_process("tests/test-configs/cassandra/cluster-multi-rack/topology_rack3.yaml")
            .with_log_name("Rack3")
            .with_config("tests/test-configs/shotover-config/config3.yaml")
            .start()
            .await;

    test_helpers::connection::cassandra::go::run_go_smoke_test().await;

    shotover_rack1.shutdown_and_then_consume_events(&[]).await;
    shotover_rack2.shutdown_and_then_consume_events(&[]).await;
    shotover_rack3.shutdown_and_then_consume_events(&[]).await;
}

// This is very slow, only test with one driver
// We previously had this at 3 per rack but this was too much for the github actions runners and resulted in intermittent failures.
#[rstest]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
async fn cluster_multi_rack_2_per_rack(#[case] driver: CassandraDriver) {
    let _compose = docker_compose(
        "tests/test-configs/cassandra/cluster-multi-rack-2-per-rack/docker-compose.yaml",
    );

    {
        let shotover_rack1 = shotover_process(
            "tests/test-configs/cassandra/cluster-multi-rack-2-per-rack/topology_rack1.yaml",
        )
        .with_config("tests/test-configs/shotover-config/config1.yaml")
        .with_log_name("Rack1")
        .start()
        .await;
        let shotover_rack2 = shotover_process(
            "tests/test-configs/cassandra/cluster-multi-rack-2-per-rack/topology_rack2.yaml",
        )
        .with_log_name("Rack2")
        .with_config("tests/test-configs/shotover-config/config2.yaml")
        .start()
        .await;
        let shotover_rack3 = shotover_process(
            "tests/test-configs/cassandra/cluster-multi-rack-2-per-rack/topology_rack3.yaml",
        )
        .with_config("tests/test-configs/shotover-config/config3.yaml")
        .with_log_name("Rack3")
        .start()
        .await;

        let connection = || async {
            let mut connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
                .build()
                .await;
            connection
                .enable_schema_awaiter("172.16.1.2:9042", None)
                .await;
            connection
        };
        standard_test_suite_rf3(&connection, driver).await;

        shotover_rack1.shutdown_and_then_consume_events(&[]).await;
        shotover_rack2.shutdown_and_then_consume_events(&[]).await;
        shotover_rack3.shutdown_and_then_consume_events(&[]).await;
    }
    let expected_nodes: Vec<(SocketAddr, &'static str)> = vec![
        ("172.16.1.2:9042".parse().unwrap(), "rack1"),
        ("172.16.1.3:9042".parse().unwrap(), "rack1"),
        ("172.16.1.5:9042".parse().unwrap(), "rack2"),
        ("172.16.1.6:9042".parse().unwrap(), "rack2"),
        ("172.16.1.8:9042".parse().unwrap(), "rack3"),
        ("172.16.1.9:9042".parse().unwrap(), "rack3"),
    ];
    cluster::multi_rack::test_topology_task(None, expected_nodes, 16).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_multi_rack_2_per_rack_go_smoke_test() {
    let _compose = docker_compose(
        "tests/test-configs/cassandra/cluster-multi-rack-2-per-rack/docker-compose.yaml",
    );

    let shotover_rack1 = shotover_process(
        "tests/test-configs/cassandra/cluster-multi-rack-2-per-rack/topology_rack1.yaml",
    )
    .with_config("tests/test-configs/shotover-config/config1.yaml")
    .with_log_name("Rack1")
    .start()
    .await;
    let shotover_rack2 = shotover_process(
        "tests/test-configs/cassandra/cluster-multi-rack-2-per-rack/topology_rack2.yaml",
    )
    .with_log_name("Rack2")
    .with_config("tests/test-configs/shotover-config/config2.yaml")
    .start()
    .await;
    let shotover_rack3 = shotover_process(
        "tests/test-configs/cassandra/cluster-multi-rack-2-per-rack/topology_rack3.yaml",
    )
    .with_config("tests/test-configs/shotover-config/config3.yaml")
    .with_log_name("Rack3")
    .start()
    .await;

    test_helpers::connection::cassandra::go::run_go_smoke_test().await;

    // gocql driver will route execute requests to its control connection during initialization which results in out of rack requests.
    // This warning is correctly triggered in that case.
    // The warning occurs only in rack1, gocql driver always picks rack 1 for its control connection
    shotover_rack1
        .shutdown_and_then_consume_events(&[EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::cassandra::sink_cluster::node_pool")
            .with_message("No suitable nodes to route to found within rack. This error only occurs in debug builds as it should never occur in an ideal integration test situation.")
            .with_count(Count::Any)
            ])
        .await;
    shotover_rack2.shutdown_and_then_consume_events(&[]).await;
    shotover_rack3.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
#[case::scylla(Scylla)]
//#[case::cdrs(Cdrs)] // TODO
#[tokio::test(flavor = "multi_thread")]
async fn source_tls_and_cluster_tls(#[case] driver: CassandraDriver) {
    test_helpers::cert::generate_cassandra_test_certs();
    let ca_cert = "tests/test-configs/cassandra/tls/certs/localhost_CA.crt";

    let _compose = docker_compose("tests/test-configs/cassandra/cluster-tls/docker-compose.yaml");
    {
        let shotover = shotover_process("tests/test-configs/cassandra/cluster-tls/topology.yaml")
            .start()
            .await;

        {
            // Run a quick test straight to Cassandra to check our assumptions that Shotover and Cassandra TLS are behaving exactly the same
            let direct_connection = CassandraConnectionBuilder::new("172.16.1.2", 9042, driver)
                .with_tls(ca_cert)
                .build()
                .await;
            assert_query_result(
                &direct_connection,
                "SELECT bootstrapped FROM system.local",
                &[&[ResultValue::Varchar("COMPLETED".into())]],
            )
            .await;
        }

        let connection = || async {
            let mut connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
                .with_tls(ca_cert)
                .build()
                .await;
            connection
                .enable_schema_awaiter("172.16.1.2:9042", Some(ca_cert))
                .await;
            connection
        };

        standard_test_suite(&connection, driver).await;
        cluster::single_rack_v4::test(&connection().await, driver).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    cluster::single_rack_v4::test_topology_task(Some(ca_cert), None).await;
}

#[apply(all_cassandra_drivers)]
#[tokio::test(flavor = "multi_thread")]
async fn cassandra_valkey_cache(#[case] driver: CassandraDriver) {
    let _compose = docker_compose("tests/test-configs/cassandra/valkey-cache/docker-compose.yaml");

    let shotover = shotover_process("tests/test-configs/cassandra/valkey-cache/topology.yaml")
        .start()
        .await;

    let mut valkey_connection = ValkeyConnectionCreator {
        address: "127.0.0.1".into(),
        port: 6379,
        tls: false,
    }
    .new_sync();
    let connection_creator = || CassandraConnectionBuilder::new("127.0.0.1", 9042, driver).build();
    let connection = connection_creator().await;

    keyspace::test(&connection).await;
    table::test(&connection).await;
    udt::test(&connection).await;
    functions::test(&connection).await;
    // collections::test // TODO: for some reason this test case fails here
    prepared_statements_simple::test(&connection, connection_creator, 1).await;
    batch_statements::test(&connection).await;
    cache::test(&connection, &mut valkey_connection).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
// #[case::cdrs(Cdrs)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::cpp(Cpp))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
async fn protect_transform_local(#[case] driver: CassandraDriver) {
    let _compose = docker_compose("tests/test-configs/cassandra/protect-local/docker-compose.yaml");

    let shotover = shotover_process("tests/test-configs/cassandra/protect-local/topology.yaml")
        .start()
        .await;

    let shotover_connection = || CassandraConnectionBuilder::new("127.0.0.1", 9042, driver).build();
    let direct_connection = CassandraConnectionBuilder::new("127.0.0.1", 9043, driver)
        .build()
        .await;

    standard_test_suite(shotover_connection, driver).await;
    protect::test(&shotover_connection().await, &direct_connection).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
//#[case::cdrs(Cdrs)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::cpp(Cpp))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
async fn protect_transform_aws(#[case] driver: CassandraDriver) {
    let _compose = docker_compose("tests/test-configs/cassandra/protect-aws/docker-compose.yaml");
    let _compose_aws = new_moto();

    let shotover = shotover_process("tests/test-configs/cassandra/protect-aws/topology.yaml")
        .start()
        .await;

    let shotover_connection = || CassandraConnectionBuilder::new("127.0.0.1", 9042, driver).build();
    let direct_connection = CassandraConnectionBuilder::new("127.0.0.1", 9043, driver)
        .build()
        .await;

    standard_test_suite(shotover_connection, driver).await;
    protect::test(&shotover_connection().await, &direct_connection).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
//#[case::cdrs(Cdrs)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::cpp(Cpp))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
async fn peers_rewrite_v4(#[case] driver: CassandraDriver) {
    let _docker_compose = docker_compose(
        "tests/test-configs/cassandra/peers-rewrite/docker-compose-4.0-cassandra.yaml",
    );

    let shotover = shotover_process("tests/test-configs/cassandra/peers-rewrite/topology.yaml")
        .start()
        .await;

    let normal_connection = CassandraConnectionBuilder::new("127.0.0.1", 9043, driver)
        .build()
        .await;
    let rewrite_port_connection = CassandraConnectionBuilder::new("127.0.0.1", 9044, driver)
        .build()
        .await;

    // run some basic tests to confirm it works as normal
    table::test(&normal_connection).await;

    {
        assert_query_result(
            &normal_connection,
            "SELECT data_center, native_port, rack FROM system.peers_v2;",
            &[&[
                ResultValue::Varchar("datacenter1".into()),
                ResultValue::Int(9042),
                ResultValue::Varchar("rack1".into()),
            ]],
        )
        .await;
        assert_query_result(
            &normal_connection,
            "SELECT native_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9042)]],
        )
        .await;

        assert_query_result(
            &normal_connection,
            "SELECT native_port as foo FROM system.peers_v2;",
            &[&[ResultValue::Int(9042)]],
        )
        .await;
    }

    {
        assert_query_result(
            &rewrite_port_connection,
            "SELECT data_center, native_port, rack FROM system.peers_v2;",
            &[&[
                ResultValue::Varchar("datacenter1".into()),
                ResultValue::Int(9044),
                ResultValue::Varchar("rack1".into()),
            ]],
        )
        .await;

        assert_query_result(
            &rewrite_port_connection,
            "SELECT native_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9044)]],
        )
        .await;

        assert_query_result(
            &rewrite_port_connection,
            "SELECT native_port as foo FROM system.peers_v2;",
            &[&[ResultValue::Int(9044)]],
        )
        .await;

        assert_query_result(
            &rewrite_port_connection,
            "SELECT native_port, native_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9044), ResultValue::Int(9044)]],
        )
        .await;

        assert_query_result(
            &rewrite_port_connection,
            "SELECT native_port, native_port as some_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9044), ResultValue::Int(9044)]],
        )
        .await;

        let result = rewrite_port_connection
            .execute("SELECT * FROM system.peers_v2;")
            .await;
        assert_eq!(result[0][5], ResultValue::Int(9044));
    }

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
//#[case::cdrs(Cdrs)] // Disabled due to intermittent failure that only occurs on v3
#[case::scylla(Scylla)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::cpp(Cpp))]
#[tokio::test(flavor = "multi_thread")]
async fn peers_rewrite_v3(#[case] driver: CassandraDriver) {
    let _docker_compose = docker_compose(
        "tests/test-configs/cassandra/peers-rewrite/docker-compose-3.11-cassandra.yaml",
    );

    let shotover = shotover_process("tests/test-configs/cassandra/peers-rewrite/topology.yaml")
        .start()
        .await;

    let connection = CassandraConnectionBuilder::new("127.0.0.1", 9044, driver)
        .build()
        .await;
    // run some basic tests to confirm it works as normal
    table::test(&connection).await;

    // Assert that the error cassandra gives because system.peers_v2 does not exist on cassandra v3
    // is passed through shotover unchanged.
    assert_eq!(
        connection
            .execute_fallible("SELECT data_center, native_port, rack FROM system.peers_v2;")
            .await,
        Err(ErrorBody {
            ty: ErrorType::Invalid,
            message: "unconfigured table peers_v2".into()
        })
    );

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
//#[case::cdrs(Cdrs)] // TODO: cdrs-tokio seems to be sending extra messages triggering the rate limiter
#[case::scylla(Scylla)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::cpp(Cpp))]
#[tokio::test(flavor = "multi_thread")]
async fn request_throttling(#[case] driver: CassandraDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/cassandra/passthrough/docker-compose.yaml");

    let shotover = shotover_process("tests/test-configs/cassandra/request-throttling.yaml")
        .start()
        .await;

    let connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
        .build()
        .await;
    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window and not trigger the rate limiter with client's startup reqeusts
    let connection_2 = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
        .build()
        .await;
    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window again

    let statement = "SELECT * FROM system.peers";

    // these should all be let through the request throttling
    {
        let mut future_list = vec![];
        for _ in 0..25 {
            future_list.push(connection.execute(statement));
            future_list.push(connection_2.execute(statement));
        }
        join_all(future_list).await;
    }

    // sleep to reset the window
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // only around half of these should be let through the request throttling
    {
        let mut future_list = vec![];
        for _ in 0..50 {
            future_list.push(connection.execute_fallible(statement));
            future_list.push(connection_2.execute_fallible(statement));
        }
        let mut results = join_all(future_list).await;
        results.retain(|result| match result {
            Ok(_) => true,
            Err(ErrorBody {
                ty: ErrorType::Overloaded,
                ..
            }) => false,
            Err(e) => panic!(
                "wrong error returned, got {:?}, expected SERVER_OVERLOADED",
                e
            ),
        });

        let len = results.len();
        // The number of requests getting through may increase because it may take longer to run
        // on some machines.
        assert!((50..=90).contains(&len), "got {len}");
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await; // sleep to reset the window

    // setup keyspace and table for the batch statement tests
    {
        run_query(&connection, "CREATE KEYSPACE test_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
        run_query(&connection, "CREATE TABLE test_keyspace.my_table (id int PRIMARY KEY, lastname text, firstname text);").await;
    }

    // this batch set should be allowed through
    {
        let mut queries: Vec<String> = vec![];
        for i in 0..25 {
            queries.push(format!("INSERT INTO test_keyspace.my_table (id, lastname, firstname) VALUES ({}, 'text', 'text')", i));
        }
        connection.execute_batch(queries).await;
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await; // sleep to reset the window

    // this batch set should not be allowed through
    {
        let mut queries: Vec<String> = vec![];
        for i in 0..60 {
            queries.push(format!("INSERT INTO test_keyspace.my_table (id, lastname, firstname) VALUES ({}, 'text', 'text')", i));
        }
        let result = connection
            .execute_batch_fallible(queries)
            .await
            .unwrap_err();
        assert_eq!(
            result,
            ErrorBody {
                ty: ErrorType::Overloaded,
                message: "Server overloaded".into()
            }
        );
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await; // sleep to reset the window

    let event_watcher_message = r#"A message was received that could never have been successfully delivered since it contains more sub messages than can ever be allowed through via the `RequestThrottling` transforms `max_requests_per_second` configuration."#;

    shotover
        .shutdown_and_then_consume_events(&[EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::throttling")
            .with_message(event_watcher_message)])
        .await;
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
async fn compression_single(#[case] driver: CassandraDriver) {
    async fn test(driver: CassandraDriver, topology_path: &str, compression: Compression) {
        let _compose =
            docker_compose("tests/test-configs/cassandra/passthrough/docker-compose.yaml");
        let shotover = shotover_process(topology_path).start().await;
        let connection = || {
            CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
                .with_compression(compression)
                .build()
        };

        standard_test_suite(connection, driver).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    // passthrough
    for topology in [
        "tests/test-configs/cassandra/passthrough/topology.yaml",
        "tests/test-configs/cassandra/passthrough/topology-encode.yaml",
    ] {
        for compression in [Compression::Lz4, Compression::Snappy] {
            test(driver, topology, compression).await;
        }
    }
}

#[rstest]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
async fn compression_cluster(#[case] driver: CassandraDriver) {
    async fn test(driver: CassandraDriver, topology_path: &str, compression: Compression) {
        let _compose =
            docker_compose("tests/test-configs/cassandra/cluster-v4/docker-compose.yaml");
        let shotover = shotover_process(topology_path).start().await;
        let connection = || async {
            let mut connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
                .with_compression(compression)
                .build()
                .await;
            connection
                .enable_schema_awaiter("172.16.1.2:9044", None)
                .await;
            connection
        };

        standard_test_suite(&connection, driver).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    test(
        driver,
        "tests/test-configs/cassandra/cluster-v4/topology.yaml",
        Compression::Snappy,
    )
    .await;
}

#[rstest]
#[case::cdrs(Cdrs)]
#[tokio::test(flavor = "multi_thread")]
async fn events_keyspace(#[case] driver: CassandraDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/cassandra/passthrough/docker-compose.yaml");

    let shotover = shotover_process("tests/test-configs/cassandra/passthrough/topology.yaml")
        .start()
        .await;

    let connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
        .build()
        .await;

    let mut event_recv = connection.as_cdrs().create_event_receiver();

    let create_ks = "CREATE KEYSPACE IF NOT EXISTS test_events_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    connection.execute(create_ks).await;

    let event = timeout(Duration::from_secs(10), event_recv.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        event,
        ServerEvent::SchemaChange(SchemaChange {
            change_type: SchemaChangeType::Created,
            target: SchemaChangeTarget::Keyspace,
            options: SchemaChangeOptions::Keyspace("test_events_ks".to_string())
        })
    );

    shotover.shutdown_and_then_consume_events(&[]).await;
}

// TODO find and fix the cause of this failing test https://github.com/shotover/shotover-proxy/issues/1096
#[cfg(feature = "alpha-transforms")]
#[rstest]
#[case::cdrs(Cdrs)]
#[tokio::test(flavor = "multi_thread")]
async fn test_protocol_v3(#[case] driver: CassandraDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/cassandra/passthrough/docker-compose.yaml");

    let shotover =
        shotover_process("tests/test-configs/cassandra/passthrough/topology-encode.yaml")
            .start()
            .await;

    let _connection = || {
        CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
            .with_protocol_version(ProtocolVersion::V3)
            .build()
    };

    // standard_test_suite(&connection, driver).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
#[case::cdrs(Cdrs)]
#[tokio::test(flavor = "multi_thread")]
async fn test_protocol_v4(#[case] driver: CassandraDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/cassandra/passthrough/docker-compose.yaml");

    let shotover =
        shotover_process("tests/test-configs/cassandra/passthrough/topology-encode.yaml")
            .start()
            .await;

    let connection = || {
        CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
            .with_protocol_version(ProtocolVersion::V4)
            .build()
    };

    standard_test_suite(&connection, driver).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
#[case::cdrs(Cdrs)]
#[tokio::test(flavor = "multi_thread")]
async fn test_protocol_v5_single(#[case] driver: CassandraDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/cassandra/passthrough/docker-compose.yaml");

    let shotover =
        shotover_process("tests/test-configs/cassandra/passthrough/topology-encode.yaml")
            .start()
            .await;

    let connection = || {
        CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
            .with_protocol_version(ProtocolVersion::V5)
            .build()
    };

    standard_test_suite(&connection, driver).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
#[case::cdrs(Cdrs)]
#[tokio::test(flavor = "multi_thread")]
async fn test_protocol_v5_compression_passthrough(#[case] driver: CassandraDriver) {
    {
        let _docker_compose =
            docker_compose("tests/test-configs/cassandra/passthrough/docker-compose.yaml");

        let shotover = shotover_process("tests/test-configs/cassandra/passthrough/topology.yaml")
            .start()
            .await;

        let connection = || {
            CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
                .with_protocol_version(ProtocolVersion::V5)
                .with_compression(Compression::Lz4)
                .build()
        };

        standard_test_suite(&connection, driver).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
#[case::cdrs(Cdrs)]
#[tokio::test(flavor = "multi_thread")]
async fn test_protocol_v5_compression_encode(#[case] driver: CassandraDriver) {
    {
        let _docker_compose =
            docker_compose("tests/test-configs/cassandra/passthrough/docker-compose.yaml");

        let shotover =
            shotover_process("tests/test-configs/cassandra/passthrough/topology-encode.yaml")
                .start()
                .await;

        let connection = || {
            CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
                .with_protocol_version(ProtocolVersion::V5)
                .with_compression(Compression::Lz4)
                .build()
        };

        standard_test_suite(&connection, driver).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn passthrough_websockets() {
    let _docker_compose =
        docker_compose("tests/test-configs/cassandra/passthrough-websocket/docker-compose.yaml");

    let shotover =
        shotover_process("tests/test-configs/cassandra/passthrough-websocket/topology.yaml")
            .start()
            .await;

    let mut session = CqlWsSession::new("ws://0.0.0.0:9042").await;
    let rows = session.query("SELECT bootstrapped FROM system.local").await;
    assert_eq!(rows, vec![vec![CassandraType::Varchar("COMPLETED".into())]]);

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "alpha-transforms")]
#[tokio::test(flavor = "multi_thread")]
async fn encode_websockets() {
    let _docker_compose =
        docker_compose("tests/test-configs/cassandra/passthrough-websocket/docker-compose.yaml");

    let shotover =
        shotover_process("tests/test-configs/cassandra/passthrough-websocket/topology-encode.yaml")
            .start()
            .await;

    let mut session = CqlWsSession::new("ws://0.0.0.0:9042").await;
    let rows = session.query("SELECT bootstrapped FROM system.local").await;
    assert_eq!(rows, vec![vec![CassandraType::Varchar("COMPLETED".into())]]);

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn passthrough_tls_websockets() {
    test_helpers::cert::generate_cassandra_test_certs();
    let _docker_compose = docker_compose(
        "tests/test-configs/cassandra/passthrough-websocket-tls/docker-compose.yaml",
    );

    let shotover =
        shotover_process("tests/test-configs/cassandra/passthrough-websocket-tls/topology.yaml")
            .start()
            .await;

    let ca_cert = "tests/test-configs/cassandra/tls/certs/localhost_CA.crt";

    default_provider().install_default().ok();
    let mut session = CqlWsSession::new_tls("wss://0.0.0.0:9042", ca_cert).await;
    let rows = session.query("SELECT bootstrapped FROM system.local").await;
    assert_eq!(rows, vec![vec![CassandraType::Varchar("COMPLETED".into())]]);

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[apply(all_cassandra_drivers)]
#[tokio::test(flavor = "multi_thread")]
async fn cassandra_5_passthrough(#[case] driver: CassandraDriver) {
    let _compose = docker_compose("tests/test-configs/cassandra/cassandra-5/docker-compose.yaml");

    let shotover = shotover_process("tests/test-configs/cassandra/cassandra-5/topology.yaml")
        .start()
        .await;

    let connection = || CassandraConnectionBuilder::new("127.0.0.1", 9042, driver).build();

    standard_test_suite_cassandra5(&connection, driver).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::cpp(Cpp))]
#[case::scylla(Scylla)]
#[case::cdrs(Cdrs)]
#[tokio::test(flavor = "multi_thread")]
async fn cassandra_5_cluster(#[case] driver: CassandraDriver) {
    let _compose = docker_compose("tests/test-configs/cassandra/cluster-v5/docker-compose.yaml");

    let shotover = shotover_process("tests/test-configs/cassandra/cluster-v5/topology.yaml")
        .start()
        .await;

    let connection = || async {
        let mut connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
            .build()
            .await;
        connection
            .enable_schema_awaiter("172.16.1.2:9044", None)
            .await;
        connection
    };

    standard_test_suite_cassandra5(&connection, driver).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}
