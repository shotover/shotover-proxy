use crate::helpers::cassandra::{assert_query_result, execute_query, run_query, ResultValue};
use crate::helpers::ShotoverManager;
use cassandra_cpp::{stmt, Batch, BatchType, Error, ErrorKind};
use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
use cdrs_tokio::frame::events::ServerEvent;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use futures::future::{join_all, try_join_all};
use metrics_util::debugging::DebuggingRecorder;
use serial_test::serial;
use std::sync::Arc;
use test_helpers::docker_compose::DockerCompose;
use tokio::time::{sleep, timeout, Duration};

mod batch_statements;
mod cache;
mod collections;
mod functions;
mod keyspace;
mod native_types;
mod prepared_statements;
#[cfg(feature = "alpha-transforms")]
mod protect;
mod table;
mod udt;

#[test]
#[serial]
fn test_passthrough() {
    let _compose = DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yml");

    let shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-passthrough/topology.yaml");

    let connection = shotover_manager.cassandra_connection("127.0.0.1", 9042);

    keyspace::test(&connection);
    table::test(&connection);
    udt::test(&connection);
    native_types::test(&connection);
    collections::test(&connection);
    functions::test(&connection);
    prepared_statements::test(&connection);
    batch_statements::test(&connection);
}

#[test]
#[serial]
fn test_source_tls_and_single_tls() {
    test_helpers::cert::generate_cassandra_test_certs();
    let _compose = DockerCompose::new("example-configs/cassandra-tls/docker-compose.yml");

    let shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-tls/topology.yaml");

    let ca_cert = "example-configs/cassandra-tls/certs/localhost_CA.crt";

    {
        // Run a quick test straight to Cassandra to check our assumptions that Shotover and Cassandra TLS are behaving exactly the same
        let direct_connection =
            shotover_manager.cassandra_connection_tls("127.0.0.1", 9042, ca_cert);
        assert_query_result(
            &direct_connection,
            "SELECT bootstrapped FROM system.local",
            &[&[ResultValue::Varchar("COMPLETED".into())]],
        );
    }

    let connection = shotover_manager.cassandra_connection_tls("127.0.0.1", 9043, ca_cert);

    keyspace::test(&connection);
    table::test(&connection);
    udt::test(&connection);
    native_types::test(&connection);
    collections::test(&connection);
    functions::test(&connection);
    prepared_statements::test(&connection);
    batch_statements::test(&connection);
}

#[test]
#[serial]
fn test_cassandra_redis_cache() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();
    let _compose = DockerCompose::new("example-configs/cassandra-redis-cache/docker-compose.yml");

    let shotover_manager = ShotoverManager::from_topology_file_without_observability(
        "example-configs/cassandra-redis-cache/topology.yaml",
    );

    let mut redis_connection = shotover_manager.redis_connection(6379);
    let connection = shotover_manager.cassandra_connection("127.0.0.1", 9042);

    keyspace::test(&connection);
    table::test(&connection);
    udt::test(&connection);
    functions::test(&connection);
    cache::test(&connection, &mut redis_connection, &snapshotter);
    prepared_statements::test(&connection);
    batch_statements::test(&connection);
}

#[test]
#[serial]
#[cfg(feature = "alpha-transforms")]
fn test_cassandra_protect_transform_local() {
    let _compose = DockerCompose::new("example-configs/cassandra-protect-local/docker-compose.yml");

    let shotover_manager = ShotoverManager::from_topology_file(
        "example-configs/cassandra-protect-local/topology.yaml",
    );

    let shotover_connection = shotover_manager.cassandra_connection("127.0.0.1", 9042);
    let direct_connection = shotover_manager.cassandra_connection("127.0.0.1", 9043);

    keyspace::test(&shotover_connection);
    table::test(&shotover_connection);
    udt::test(&shotover_connection);
    native_types::test(&shotover_connection);
    collections::test(&shotover_connection);
    functions::test(&shotover_connection);
    protect::test(&shotover_connection, &direct_connection);
    batch_statements::test(&shotover_connection);
}

#[test]
#[serial]
#[cfg(feature = "alpha-transforms")]
fn test_cassandra_protect_transform_aws() {
    let _compose = DockerCompose::new("example-configs/cassandra-protect-aws/docker-compose.yml");
    let _compose_aws = DockerCompose::new_moto();

    let shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-protect-aws/topology.yaml");

    let shotover_connection = shotover_manager.cassandra_connection("127.0.0.1", 9042);
    let direct_connection = shotover_manager.cassandra_connection("127.0.0.1", 9043);

    keyspace::test(&shotover_connection);
    table::test(&shotover_connection);
    udt::test(&shotover_connection);
    native_types::test(&shotover_connection);
    collections::test(&shotover_connection);
    functions::test(&shotover_connection);
    protect::test(&shotover_connection, &direct_connection);
    batch_statements::test(&shotover_connection);
}

#[test]
#[serial]
fn test_cassandra_peers_rewrite() {
    // check it works with the newer version of Cassandra
    {
        let _docker_compose = DockerCompose::new(
            "tests/test-configs/cassandra-peers-rewrite/docker-compose-4.0-cassandra.yaml",
        );

        let shotover_manager = ShotoverManager::from_topology_file(
            "tests/test-configs/cassandra-peers-rewrite/topology.yaml",
        );

        let normal_connection = shotover_manager.cassandra_connection("127.0.0.1", 9043);

        let rewrite_port_connection = shotover_manager.cassandra_connection("127.0.0.1", 9044);
        table::test(&rewrite_port_connection); // run some basic tests to confirm it works as normal

        {
            assert_query_result(
                &normal_connection,
                "SELECT data_center, native_port, rack FROM system.peers_v2;",
                &[&[
                    ResultValue::Varchar("dc1".into()),
                    ResultValue::Int(9042),
                    ResultValue::Varchar("West".into()),
                ]],
            );
            assert_query_result(
                &normal_connection,
                "SELECT native_port FROM system.peers_v2;",
                &[&[ResultValue::Int(9042)]],
            );

            assert_query_result(
                &normal_connection,
                "SELECT native_port as foo FROM system.peers_v2;",
                &[&[ResultValue::Int(9042)]],
            );
        }

        {
            assert_query_result(
                &rewrite_port_connection,
                "SELECT data_center, native_port, rack FROM system.peers_v2;",
                &[&[
                    ResultValue::Varchar("dc1".into()),
                    ResultValue::Int(9044),
                    ResultValue::Varchar("West".into()),
                ]],
            );

            assert_query_result(
                &rewrite_port_connection,
                "SELECT native_port FROM system.peers_v2;",
                &[&[ResultValue::Int(9044)]],
            );

            assert_query_result(
                &rewrite_port_connection,
                "SELECT native_port as foo FROM system.peers_v2;",
                &[&[ResultValue::Int(9044)]],
            );

            assert_query_result(
                &rewrite_port_connection,
                "SELECT native_port, native_port FROM system.peers_v2;",
                &[&[ResultValue::Int(9044), ResultValue::Int(9044)]],
            );

            assert_query_result(
                &rewrite_port_connection,
                "SELECT native_port, native_port as some_port FROM system.peers_v2;",
                &[&[ResultValue::Int(9044), ResultValue::Int(9044)]],
            );

            let result = execute_query(&rewrite_port_connection, "SELECT * FROM system.peers_v2;");
            assert_eq!(result[0][5], ResultValue::Int(9044));
        }
    }

    // check it works with an older version of Cassandra
    {
        let _docker_compose = DockerCompose::new(
            "tests/test-configs/cassandra-peers-rewrite/docker-compose-3.11-cassandra.yaml",
        );

        let shotover_manager = ShotoverManager::from_topology_file(
            "tests/test-configs/cassandra-peers-rewrite/topology.yaml",
        );

        let connection = shotover_manager.cassandra_connection("127.0.0.1", 9044);
        table::test(&connection); // run some basic tests to confirm it works as normal

        let statement = stmt!("SELECT data_center, native_port, rack FROM system.peers_v2;");
        let result = connection.execute(&statement).wait().unwrap_err();
        assert!(matches!(
            result,
            Error(
                ErrorKind::CassErrorResult(cassandra_cpp::CassErrorCode::SERVER_INVALID_QUERY, ..),
                _
            )
        ));
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cassandra_request_throttling() {
    let _docker_compose =
        DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yml");

    let shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/cassandra-request-throttling.yaml");

    let connection = shotover_manager.cassandra_connection("127.0.0.1", 9042);
    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window and not trigger the rate limiter with client's startup reqeusts
    let connection_2 = shotover_manager.cassandra_connection("127.0.0.1", 9042);
    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window again

    let statement = stmt!("SELECT * FROM system.peers");

    // these should all be let through the request throttling
    {
        let mut futures = vec![];
        for _ in 0..25 {
            futures.push(connection.execute(&statement));
            futures.push(connection_2.execute(&statement));
        }
        try_join_all(futures).await.unwrap();
    }

    // sleep to reset the window
    std::thread::sleep(std::time::Duration::from_secs(1));

    // only around half of these should be let through the request throttling
    {
        let mut futures = vec![];
        for _ in 0..50 {
            futures.push(connection.execute(&statement));
            futures.push(connection_2.execute(&statement));
        }
        let mut results = join_all(futures).await;
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

    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window

    // setup keyspace and table for the batch statement tests
    {
        run_query(&connection, "CREATE KEYSPACE test_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        run_query(&connection, "CREATE TABLE test_keyspace.my_table (id int PRIMARY KEY, lastname text, firstname text);");
    }

    // this batch set should be allowed through
    {
        let mut batch = Batch::new(BatchType::LOGGED);
        for i in 0..25 {
            let statement = format!("INSERT INTO test_keyspace.my_table (id, lastname, firstname) VALUES ({}, 'text', 'text')", i);
            batch.add_statement(&stmt!(statement.as_str())).unwrap();
        }
        connection.execute_batch(&batch).wait().unwrap();
    }

    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window

    // this batch set should not be allowed through
    {
        let mut batch = Batch::new(BatchType::LOGGED);
        for i in 0..60 {
            let statement = format!("INSERT INTO test_keyspace.my_table (id, lastname, firstname) VALUES ({}, 'text', 'text')", i);
            batch.add_statement(&stmt!(statement.as_str())).unwrap();
        }
        let result = connection.execute_batch(&batch).wait().unwrap_err();
        assert!(matches!(
            result,
            Error(
                ErrorKind::CassErrorResult(cassandra_cpp::CassErrorCode::SERVER_OVERLOADED, ..),
                ..
            )
        ));
    }

    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window

    batch_statements::test(&connection);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_events_keyspace() {
    let _docker_compose =
        DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yml");

    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-passthrough/topology.yaml");

    let user = "cassandra";
    let password = "cassandra";
    let auth = StaticPasswordAuthenticatorProvider::new(&user, &password);
    let config = NodeTcpConfigBuilder::new()
        .with_contact_point("127.0.0.1:9042".into())
        .with_authenticator_provider(Arc::new(auth))
        .build()
        .await
        .unwrap();

    let session = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), config)
        .build()
        .unwrap();

    let mut event_recv = session.create_event_receiver();

    sleep(Duration::from_secs(3)).await; // let the driver finish connecting to the cluster and registering for the events

    let create_ks = "CREATE KEYSPACE IF NOT EXISTS test_events_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

    session.query(create_ks).await.unwrap();

    match timeout(Duration::from_secs(10), event_recv.recv()).await {
        Ok(recvd) => {
            if let Ok(event) = recvd {
                assert!(matches!(event, ServerEvent::SchemaChange { .. }));
            };
        }
        Err(err) => {
            panic!("{}", err);
        }
    }
}
