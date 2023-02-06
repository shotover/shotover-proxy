use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType};
use cdrs_tokio::frame::events::{
    SchemaChange, SchemaChangeOptions, SchemaChangeTarget, SchemaChangeType, ServerEvent,
};
use futures::future::join_all;
use futures::Future;
use rstest::rstest;
use serial_test::serial;
#[cfg(feature = "cassandra-cpp-driver-tests")]
use test_helpers::connection::cassandra::CassandraDriver::Datastax;
use test_helpers::connection::cassandra::Compression;
use test_helpers::connection::cassandra::{
    assert_query_result, run_query, CassandraConnection, CassandraDriver,
    CassandraDriver::CdrsTokio, CassandraDriver::Scylla, ResultValue,
};
use test_helpers::connection::redis_connection;
use test_helpers::docker_compose::DockerCompose;
use test_helpers::shotover_process::{Count, EventMatcher, Level, ShotoverProcessBuilder};
use tokio::time::{timeout, Duration};

mod batch_statements;
mod cache;
mod cluster;
mod collections;
mod functions;
mod keyspace;
mod native_types;
mod prepared_statements_all;
mod prepared_statements_simple;
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
    prepared_statements_simple::test(&connection, connection_creator).await;
    prepared_statements_all::test(&connection).await;
    batch_statements::test(&connection).await;
    timestamp::test(&connection).await;
}

#[rstest]
#[case::cdrs(CdrsTokio)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn passthrough_standard(#[case] driver: CassandraDriver) {
    let _compose = DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yaml");

    let shotover = ShotoverProcessBuilder::new_with_topology(
        "example-configs/cassandra-passthrough/topology.yaml",
    )
    .start()
    .await;

    let connection = || CassandraConnection::new("127.0.0.1", 9042, driver);

    standard_test_suite(&connection, driver).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
#[case::cdrs(CdrsTokio)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn passthrough_encode(#[case] driver: CassandraDriver) {
    let _compose = DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yaml");

    let shotover = ShotoverProcessBuilder::new_with_topology(
        "example-configs/cassandra-passthrough/topology-encode.yaml",
    )
    .start()
    .await;

    let connection = || CassandraConnection::new("127.0.0.1", 9042, driver);

    standard_test_suite(&connection, driver).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
#[case::scylla(Scylla)]
//#[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn source_tls_and_single_tls(#[case] driver: CassandraDriver) {
    test_helpers::cert::generate_cassandra_test_certs();
    let _compose = DockerCompose::new("example-configs/cassandra-tls/docker-compose.yaml");

    let shotover =
        ShotoverProcessBuilder::new_with_topology("example-configs/cassandra-tls/topology.yaml")
            .start()
            .await;

    let ca_cert = "example-configs/docker-images/cassandra-tls-4.0.6/certs/localhost_CA.crt";

    {
        // Run a quick test straight to Cassandra to check our assumptions that Shotover and Cassandra TLS are behaving exactly the same
        let direct_connection =
            CassandraConnection::new_tls("127.0.0.1", 9042, ca_cert, driver).await;
        assert_query_result(
            &direct_connection,
            "SELECT bootstrapped FROM system.local",
            &[&[ResultValue::Varchar("COMPLETED".into())]],
        )
        .await;
    }

    let connection = || CassandraConnection::new_tls("127.0.0.1", 9043, ca_cert, driver);

    standard_test_suite(&connection, driver).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
//#[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_single_rack_v3(#[case] driver: CassandraDriver) {
    let _compose = DockerCompose::new("example-configs/cassandra-cluster-v3/docker-compose.yaml");

    {
        let shotover = ShotoverProcessBuilder::new_with_topology(
            "example-configs/cassandra-cluster-v3/topology-dummy-peers.yaml",
        )
        .start()
        .await;

        let connection = || async {
            let mut connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;
            connection
                .enable_schema_awaiter("172.16.1.2:9042", None)
                .await;
            connection
        };
        standard_test_suite(&connection, driver).await;
        cluster::single_rack_v3::test_dummy_peers(&connection().await).await;

        routing::test("127.0.0.1", 9042, "172.16.1.2", 9042, driver).await;

        //Check for bugs in cross connection state
        native_types::test(&connection().await).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    cluster::single_rack_v3::test_topology_task(None).await;
}

#[rstest]
#[case::cdrs(CdrsTokio)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_single_rack_v4(#[case] driver: CassandraDriver) {
    let compose = DockerCompose::new("example-configs/cassandra-cluster-v4/docker-compose.yaml");

    let connection = || async {
        let mut connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;
        connection
            .enable_schema_awaiter("172.16.1.2:9044", None)
            .await;
        connection
    };
    {
        let shotover = ShotoverProcessBuilder::new_with_topology(
            "example-configs/cassandra-cluster-v4/topology.yaml",
        )
        .start()
        .await;

        standard_test_suite(&connection, driver).await;
        cluster::single_rack_v4::test(&connection().await, driver).await;

        routing::test("127.0.0.1", 9042, "172.16.1.2", 9044, driver).await;

        //Check for bugs in cross connection state
        let mut connection2 = CassandraConnection::new("127.0.0.1", 9042, driver).await;
        connection2
            .enable_schema_awaiter("172.16.1.2:9044", None)
            .await;
        native_types::test(&connection2).await;

        cluster::single_rack_v4::test_node_going_down(&compose, driver).await;

        shotover
            .shutdown_and_then_consume_events(&[
                EventMatcher::new()
                    .with_level(Level::Error)
                    .with_target("shotover_proxy::server")
                    .with_message(
                        r#"connection was unexpectedly terminated

Caused by:
    0: chain failed to send and/or receive messages
    1: CassandraSinkCluster transform failed
    2: Failed to create new connection
    3: destination 172.16.1.3:9044 did not respond to connection attempt within 3s"#,
                    )
                    .with_count(Count::Any),
                EventMatcher::new()
                    .with_level(Level::Error)
                    .with_target("shotover_proxy::server")
                    .with_message(
                        r#"connection was unexpectedly terminated

Caused by:
    0: chain failed to send and/or receive messages
    1: CassandraSinkCluster transform failed
    2: system.local returned unexpected cassandra operation: Error(ErrorBody { message: "Internal shotover error: Broken pipe (os error 32)", ty: Server })"#,
                    )
                    .with_count(Count::Any),
                EventMatcher::new()
                    .with_level(Level::Warn)
                    .with_target("shotover_proxy::transforms::cassandra::sink_cluster")
                    .with_message(
                        r#"A successful connection to a control node was made but attempts to connect to these nodes failed first:
* 172.16.1.3:9044:
    - Failed to create new connection
    - destination 172.16.1.3:9044 did not respond to connection attempt within 3s"#,
                    )
                    .with_count(Count::Any),
            ])
            .await;
    }

    {
        let shotover = ShotoverProcessBuilder::new_with_topology(
            "example-configs/cassandra-cluster-v4/topology-dummy-peers.yaml",
        )
        .start()
        .await;

        cluster::single_rack_v4::test_dummy_peers(&connection().await, driver).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    cluster::single_rack_v4::test_topology_task(None, Some(9044)).await;
}

#[rstest]
//#[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_multi_rack(#[case] driver: CassandraDriver) {
    let _compose =
        DockerCompose::new("example-configs/cassandra-cluster-multi-rack/docker-compose.yaml");

    {
        let shotover_rack1 = ShotoverProcessBuilder::new_with_topology(
            "example-configs/cassandra-cluster-multi-rack/topology_rack1.yaml",
        )
        .with_log_name("Rack1")
        .start()
        .await;
        let shotover_rack2 = ShotoverProcessBuilder::new_with_topology(
            "example-configs/cassandra-cluster-multi-rack/topology_rack2.yaml",
        )
        .with_log_name("Rack2")
        .start()
        .await;
        let shotover_rack3 = ShotoverProcessBuilder::new_with_topology(
            "example-configs/cassandra-cluster-multi-rack/topology_rack3.yaml",
        )
        .with_log_name("Rack3")
        .start()
        .await;

        let connection = || async {
            let mut connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;
            connection
                .enable_schema_awaiter("172.16.1.2:9042", None)
                .await;
            connection
        };
        standard_test_suite(&connection, driver).await;
        cluster::multi_rack::test(&connection().await).await;

        //Check for bugs in cross connection state
        native_types::test(&connection().await).await;

        // TODO: This can be removed by allowing the ShotoverProcessBuilder to specify the `observability_interface`.
        let metrics_port_collision = [EventMatcher::new()
            .with_level(Level::Error)
            .with_target("shotover_proxy::observability")
            .with_message(r#"Metrics HTTP server failed: Failed to bind to 0.0.0.0:9001"#)
            .with_count(Count::Any)];
        shotover_rack1
            .shutdown_and_then_consume_events(&metrics_port_collision)
            .await;
        shotover_rack2
            .shutdown_and_then_consume_events(&metrics_port_collision)
            .await;
        shotover_rack3
            .shutdown_and_then_consume_events(&metrics_port_collision)
            .await;
    }

    cluster::multi_rack::test_topology_task(None).await;
}

#[rstest]
#[case::scylla(Scylla)]
//#[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn source_tls_and_cluster_tls(#[case] driver: CassandraDriver) {
    test_helpers::cert::generate_cassandra_test_certs();
    let ca_cert = "example-configs/docker-images/cassandra-tls-4.0.6/certs/localhost_CA.crt";

    let _compose = DockerCompose::new("example-configs/cassandra-cluster-tls/docker-compose.yaml");
    {
        let shotover = ShotoverProcessBuilder::new_with_topology(
            "example-configs/cassandra-cluster-tls/topology.yaml",
        )
        .start()
        .await;

        {
            // Run a quick test straight to Cassandra to check our assumptions that Shotover and Cassandra TLS are behaving exactly the same
            let direct_connection =
                CassandraConnection::new_tls("172.16.1.2", 9042, ca_cert, driver).await;
            assert_query_result(
                &direct_connection,
                "SELECT bootstrapped FROM system.local",
                &[&[ResultValue::Varchar("COMPLETED".into())]],
            )
            .await;
        }

        let connection = || async {
            let mut connection =
                CassandraConnection::new_tls("127.0.0.1", 9042, ca_cert, driver).await;
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

#[rstest]
#[case::cdrs(CdrsTokio)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cassandra_redis_cache(#[case] driver: CassandraDriver) {
    let _compose = DockerCompose::new("example-configs/cassandra-redis-cache/docker-compose.yaml");

    let shotover = ShotoverProcessBuilder::new_with_topology(
        "example-configs/cassandra-redis-cache/topology.yaml",
    )
    .start()
    .await;

    let mut redis_connection = redis_connection::new(6379);
    let connection_creator = || CassandraConnection::new("127.0.0.1", 9042, driver);
    let connection = connection_creator().await;

    keyspace::test(&connection).await;
    table::test(&connection).await;
    udt::test(&connection).await;
    functions::test(&connection).await;
    // collections::test // TODO: for some reason this test case fails here
    prepared_statements_simple::test(&connection, connection_creator).await;
    batch_statements::test(&connection).await;
    cache::test(&connection, &mut redis_connection).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
// #[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn protect_transform_local(#[case] driver: CassandraDriver) {
    let _compose =
        DockerCompose::new("example-configs/cassandra-protect-local/docker-compose.yaml");

    let shotover = ShotoverProcessBuilder::new_with_topology(
        "example-configs/cassandra-protect-local/topology.yaml",
    )
    .start()
    .await;

    let shotover_connection = || CassandraConnection::new("127.0.0.1", 9042, driver);
    let direct_connection = CassandraConnection::new("127.0.0.1", 9043, driver).await;

    standard_test_suite(shotover_connection, driver).await;
    protect::test(&shotover_connection().await, &direct_connection).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
//#[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn protect_transform_aws(#[case] driver: CassandraDriver) {
    let _compose = DockerCompose::new("example-configs/cassandra-protect-aws/docker-compose.yaml");
    let _compose_aws = DockerCompose::new_moto();

    let shotover = ShotoverProcessBuilder::new_with_topology(
        "example-configs/cassandra-protect-aws/topology.yaml",
    )
    .start()
    .await;

    let shotover_connection = || CassandraConnection::new("127.0.0.1", 9042, driver);
    let direct_connection = CassandraConnection::new("127.0.0.1", 9043, driver).await;

    standard_test_suite(shotover_connection, driver).await;
    protect::test(&shotover_connection().await, &direct_connection).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
//#[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn peers_rewrite_v4(#[case] driver: CassandraDriver) {
    let _docker_compose = DockerCompose::new(
        "tests/test-configs/cassandra-peers-rewrite/docker-compose-4.0-cassandra.yaml",
    );

    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/cassandra-peers-rewrite/topology.yaml",
    )
    .start()
    .await;

    let normal_connection = CassandraConnection::new("127.0.0.1", 9043, driver).await;
    let rewrite_port_connection = CassandraConnection::new("127.0.0.1", 9044, driver).await;

    // run some basic tests to confirm it works as normal
    table::test(&normal_connection).await;

    {
        assert_query_result(
            &normal_connection,
            "SELECT data_center, native_port, rack FROM system.peers_v2;",
            &[&[
                ResultValue::Varchar("Mars".into()),
                ResultValue::Int(9042),
                ResultValue::Varchar("West".into()),
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
                ResultValue::Varchar("Mars".into()),
                ResultValue::Int(9044),
                ResultValue::Varchar("West".into()),
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
//#[case::cdrs(CdrsTokio)] // Disabled due to intermittent failure that only occurs on v3
#[case::scylla(Scylla)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn peers_rewrite_v3(#[case] driver: CassandraDriver) {
    let _docker_compose = DockerCompose::new(
        "tests/test-configs/cassandra-peers-rewrite/docker-compose-3.11-cassandra.yaml",
    );

    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/cassandra-peers-rewrite/topology.yaml",
    )
    .start()
    .await;

    let connection = CassandraConnection::new("127.0.0.1", 9044, driver).await;
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
//#[case::cdrs(CdrsTokio)] // TODO: cdrs-tokio seems to be sending extra messages triggering the rate limiter
#[case::scylla(Scylla)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn request_throttling(#[case] driver: CassandraDriver) {
    let _docker_compose =
        DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yaml");

    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/cassandra-request-throttling.yaml",
    )
    .start()
    .await;

    let connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;
    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window and not trigger the rate limiter with client's startup reqeusts
    let connection_2 = CassandraConnection::new("127.0.0.1", 9042, driver).await;
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
        assert!(50 < len && len <= 60, "got {len}");
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

    batch_statements::test(&connection).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
#[case::cdrs(CdrsTokio)]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn compression_single(#[case] driver: CassandraDriver) {
    async fn test(driver: CassandraDriver, topology_path: &str, compression: Compression) {
        let _compose =
            DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yaml");
        let shotover = shotover_from_topology_file(topology_path).await;
        let connection =
            || CassandraConnection::new_with_compression("127.0.0.1", 9042, driver, compression);

        standard_test_suite(connection, driver).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    // passthrough
    for topology in [
        "example-configs/cassandra-passthrough/topology.yaml",
        "example-configs/cassandra-passthrough/topology-encode.yaml",
    ] {
        for compression in [Compression::Lz4, Compression::Snappy] {
            test(driver, topology, compression).await;
        }
    }
}

#[rstest]
#[case::cdrs(CdrsTokio)]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn compression_cluster(#[case] driver: CassandraDriver) {
    async fn test(driver: CassandraDriver, topology_path: &str, compression: Compression) {
        let _compose =
            DockerCompose::new("example-configs/cassandra-cluster-v4/docker-compose.yaml");
        let shotover = shotover_from_topology_file(topology_path).await;
        let connection = || async {
            let mut connection =
                CassandraConnection::new_with_compression("127.0.0.1", 9042, driver, compression)
                    .await;
            connection
                .enable_schema_awaiter("172.16.1.2:9044", None)
                .await;
            connection
        };

        standard_test_suite(&connection, driver).await;

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    for compression in [Compression::Lz4, Compression::Snappy] {
        test(
            driver,
            "example-configs/cassandra-cluster-v4/topology.yaml",
            compression,
        )
        .await;
    }
}

#[rstest]
#[case::cdrs(CdrsTokio)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn events_keyspace(#[case] driver: CassandraDriver) {
    let _docker_compose =
        DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yaml");

    let shotover = ShotoverProcessBuilder::new_with_topology(
        "example-configs/cassandra-passthrough/topology.yaml",
    )
    .start()
    .await;

    let connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;

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
