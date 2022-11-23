use crate::helpers::cassandra::{assert_query_result, ResultValue};
#[cfg(feature = "cassandra-cpp-driver-tests")]
use crate::helpers::cassandra::{run_query, CassandraDriver::Datastax};
use crate::helpers::cassandra::{
    CassandraConnection, CassandraDriver, CassandraDriver::CdrsTokio, CassandraDriver::Scylla,
};
use crate::helpers::ShotoverManager;
#[cfg(feature = "cassandra-cpp-driver-tests")]
use cassandra_cpp::{Error, ErrorKind};
#[cfg(feature = "cassandra-cpp-driver-tests")]
use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType};
use cdrs_tokio::frame::events::{
    SchemaChange, SchemaChangeOptions, SchemaChangeTarget, SchemaChangeType, ServerEvent,
};
#[cfg(feature = "cassandra-cpp-driver-tests")]
use futures::future::join_all;
use futures::Future;
use metrics_util::debugging::DebuggingRecorder;
use rstest::rstest;
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;
use tokio::time::{sleep, timeout, Duration};

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
#[cfg(not(feature = "cassandra-cpp-driver-tests"))]
mod routing;
mod table;
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
}

#[rstest]
#[case::cdrs(CdrsTokio)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn passthrough_standard(#[case] driver: CassandraDriver) {
    let _compose = DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yaml");

    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-passthrough/topology.yaml");

    let connection = || CassandraConnection::new("127.0.0.1", 9042, driver);

    standard_test_suite(&connection, driver).await;
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
#[case::cdrs(CdrsTokio)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn passthrough_encode(#[case] driver: CassandraDriver) {
    let _compose = DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yaml");

    let _shotover_manager = ShotoverManager::from_topology_file(
        "example-configs/cassandra-passthrough/topology-encode.yaml",
    );

    let connection = || CassandraConnection::new("127.0.0.1", 9042, driver);

    standard_test_suite(&connection, driver).await;
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn source_tls_and_single_tls(#[case] driver: CassandraDriver) {
    test_helpers::cert::generate_cassandra_test_certs();
    let _compose = DockerCompose::new("example-configs/cassandra-tls/docker-compose.yaml");

    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-tls/topology.yaml");

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
        let _shotover_manager = ShotoverManager::from_topology_file(
            "example-configs/cassandra-cluster-v3/topology-dummy-peers.yaml",
        );

        let connection = || async {
            let mut connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;
            connection
                .enable_schema_awaiter("172.16.1.2:9042", None)
                .await;
            connection
        };
        standard_test_suite(&connection, driver).await;
        cluster::single_rack_v3::test_dummy_peers(&connection().await).await;

        #[cfg(not(feature = "cassandra-cpp-driver-tests"))]
        routing::test("127.0.0.1", 9042, "172.16.1.2", 9042, driver).await;

        //Check for bugs in cross connection state
        native_types::test(&connection().await).await;
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
        let _shotover_manager = ShotoverManager::from_topology_file(
            "example-configs/cassandra-cluster-v4/topology.yaml",
        );

        standard_test_suite(&connection, driver).await;
        cluster::single_rack_v4::test(&connection().await, driver).await;

        #[cfg(not(feature = "cassandra-cpp-driver-tests"))]
        routing::test("127.0.0.1", 9042, "172.16.1.2", 9044, driver).await;

        //Check for bugs in cross connection state
        let mut connection2 = CassandraConnection::new("127.0.0.1", 9042, driver).await;
        connection2
            .enable_schema_awaiter("172.16.1.2:9044", None)
            .await;
        native_types::test(&connection2).await;
    }

    {
        let _shotover_manager = ShotoverManager::from_topology_file(
            "example-configs/cassandra-cluster-v4/topology-dummy-peers.yaml",
        );

        cluster::single_rack_v4::test_dummy_peers(&connection().await, driver).await;
    }

    cluster::single_rack_v4::test_topology_task(None, Some(9044)).await;

    let shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-cluster-v4/topology.yaml");
    cluster::single_rack_v4::test_node_going_down(compose, shotover_manager, driver, false).await;
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case::cdrs(CdrsTokio)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn cluster_single_rack_node_lost(#[case] driver: CassandraDriver) {
    let compose = DockerCompose::new("example-configs/cassandra-cluster-v4/docker-compose.yaml");

    let shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-cluster-v4/topology.yaml");
    cluster::single_rack_v4::test_node_going_down(compose, shotover_manager, driver, true).await;
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
        let _shotover_manager_rack1 = ShotoverManager::from_topology_file_without_observability(
            "example-configs/cassandra-cluster-multi-rack/topology_rack1.yaml",
        );
        let _shotover_manager_rack2 = ShotoverManager::from_topology_file_without_observability(
            "example-configs/cassandra-cluster-multi-rack/topology_rack2.yaml",
        );
        let _shotover_manager_rack3 = ShotoverManager::from_topology_file_without_observability(
            "example-configs/cassandra-cluster-multi-rack/topology_rack3.yaml",
        );

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
    }

    cluster::multi_rack::test_topology_task(None).await;
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn source_tls_and_cluster_tls(#[case] driver: CassandraDriver) {
    test_helpers::cert::generate_cassandra_test_certs();
    let ca_cert = "example-configs/docker-images/cassandra-tls-4.0.6/certs/localhost_CA.crt";

    let _compose = DockerCompose::new("example-configs/cassandra-cluster-tls/docker-compose.yaml");
    {
        let _shotover_manager = ShotoverManager::from_topology_file(
            "example-configs/cassandra-cluster-tls/topology.yaml",
        );

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
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();
    let _compose = DockerCompose::new("example-configs/cassandra-redis-cache/docker-compose.yaml");

    let shotover_manager = ShotoverManager::from_topology_file_without_observability(
        "example-configs/cassandra-redis-cache/topology.yaml",
    );

    let mut redis_connection = shotover_manager.redis_connection(6379);
    let connection_creator = || CassandraConnection::new("127.0.0.1", 9042, driver);
    let connection = connection_creator().await;

    keyspace::test(&connection).await;
    table::test(&connection).await;
    udt::test(&connection).await;
    functions::test(&connection).await;
    // collections::test // TODO: for some this test case fails here
    prepared_statements_simple::test(&connection, connection_creator).await;
    batch_statements::test(&connection).await;
    cache::test(&connection, &mut redis_connection, &snapshotter).await;
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
// #[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn protect_transform_local(#[case] driver: CassandraDriver) {
    let _compose =
        DockerCompose::new("example-configs/cassandra-protect-local/docker-compose.yaml");

    let _shotover_manager = ShotoverManager::from_topology_file(
        "example-configs/cassandra-protect-local/topology.yaml",
    );

    let shotover_connection = || CassandraConnection::new("127.0.0.1", 9042, driver);
    let direct_connection = CassandraConnection::new("127.0.0.1", 9043, driver).await;

    standard_test_suite(shotover_connection, driver).await;
    protect::test(&shotover_connection().await, &direct_connection).await;
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
//#[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[case::scylla(Scylla)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn protect_transform_aws(#[case] driver: CassandraDriver) {
    let _compose = DockerCompose::new("example-configs/cassandra-protect-aws/docker-compose.yaml");
    let _compose_aws = DockerCompose::new_moto();

    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-protect-aws/topology.yaml");

    let shotover_connection = || CassandraConnection::new("127.0.0.1", 9042, driver);
    let direct_connection = CassandraConnection::new("127.0.0.1", 9043, driver).await;

    standard_test_suite(shotover_connection, driver).await;
    protect::test(&shotover_connection().await, &direct_connection).await;
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

    let _shotover_manager = ShotoverManager::from_topology_file(
        "tests/test-configs/cassandra-peers-rewrite/topology.yaml",
    );

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
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn peers_rewrite_v3(#[case] driver: CassandraDriver) {
    let _docker_compose = DockerCompose::new(
        "tests/test-configs/cassandra-peers-rewrite/docker-compose-3.11-cassandra.yaml",
    );

    let _shotover_manager = ShotoverManager::from_topology_file(
        "tests/test-configs/cassandra-peers-rewrite/topology.yaml",
    );

    let connection = CassandraConnection::new("127.0.0.1", 9044, driver).await;
    // run some basic tests to confirm it works as normal
    table::test(&connection).await;

    // Assert that the error cassandra gives because system.peers_v2 does not exist on cassandra v3
    // is passed through shotover unchanged.
    let statement = "SELECT data_center, native_port, rack FROM system.peers_v2;";
    let result = connection.execute_expect_err(statement).await;
    assert_eq!(
        result,
        ErrorBody {
            ty: ErrorType::Invalid,
            message: "unconfigured table peers_v2".into()
        }
    );
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case::cdrs(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case::datastax(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn request_throttling(#[case] driver: CassandraDriver) {
    let _docker_compose =
        DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yaml");

    let _shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/cassandra-request-throttling.yaml");

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
            Err(Error(
                ErrorKind::CassErrorResult(cassandra_cpp::CassErrorCode::SERVER_OVERLOADED, ..),
                _,
            )) => false,
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
        let result = connection.execute_batch_expect_err(queries).await;
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
}

#[rstest]
#[case::cdrs(CdrsTokio)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn events_keyspace(#[case] driver: CassandraDriver) {
    let _docker_compose =
        DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yaml");

    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-passthrough/topology.yaml");

    let connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;

    let mut event_recv = connection.as_cdrs().create_event_receiver();

    sleep(Duration::from_secs(10)).await; // let the driver finish connecting to the cluster and registering for the events

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
}
