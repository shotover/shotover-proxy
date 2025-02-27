use crate::cassandra_int_tests::cluster::run_topology_task;
use cassandra_protocol::events::ServerEvent;
use cassandra_protocol::frame::events::{StatusChange, StatusChangeType};
use pretty_assertions::assert_eq;
use std::net::SocketAddr;
use std::time::Duration;
use test_helpers::connection::cassandra::{
    CassandraConnection, CassandraConnectionBuilder, CassandraDriver, ResultValue,
    assert_query_result, run_query,
};

use test_helpers::docker_compose::DockerCompose;
use tokio::sync::broadcast;
use tokio::time::timeout;

async fn test_rewrite_system_peers(connection: &CassandraConnection) {
    let all_columns = "peer, data_center, host_id, preferred_ip, rack, release_version, rpc_address, schema_version, tokens";
    assert_query_result(connection, "SELECT * FROM system.peers;", &[]).await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns} FROM system.peers;"),
        &[],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns}, {all_columns} FROM system.peers;"),
        &[],
    )
    .await;
}

async fn test_rewrite_system_peers_v2(connection: &CassandraConnection) {
    let all_columns = "peer, peer_port, data_center, host_id, native_address, native_port, preferred_ip, preferred_port, rack, release_version, schema_version, tokens";
    assert_query_result(connection, "SELECT * FROM system.peers_v2;", &[]).await;

    run_query(connection, "USE system;").await;
    assert_query_result(connection, "SELECT * FROM peers_v2;", &[]).await;

    assert_query_result(
        connection,
        &format!("SELECT {all_columns} FROM system.peers_v2;"),
        &[],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns}, {all_columns} FROM system.peers_v2;"),
        &[],
    )
    .await;
}

async fn test_rewrite_system_peers_dummy_peers(
    connection: &CassandraConnection,
    driver: CassandraDriver,
) {
    let star_results1 = [
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Varchar("datacenter1".into()),
        ResultValue::Uuid("3c3c4e2d-ba74-4f76-b52e-fb5bcee6a9f4".parse().unwrap()),
        ResultValue::Null,
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("4.0.6".into()),
        // rpc_address is non-determistic because we dont know which node this will be
        ResultValue::Any,
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        if let CassandraDriver::Cdrs = driver {
            ResultValue::List(std::iter::repeat(ResultValue::Any).take(3 * 128).collect())
        } else {
            ResultValue::Set(std::iter::repeat(ResultValue::Any).take(3 * 128).collect())
        },
    ];
    let star_results2 = [
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Varchar("datacenter1".into()),
        ResultValue::Uuid("fa74d7ec-1223-472b-97de-04a32ccdb70b".parse().unwrap()),
        ResultValue::Null,
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("4.0.6".into()),
        // rpc_address is non-determistic because we dont know which node this will be
        ResultValue::Any,
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        if let CassandraDriver::Cdrs = driver {
            ResultValue::List(std::iter::repeat(ResultValue::Any).take(3 * 128).collect())
        } else {
            ResultValue::Set(std::iter::repeat(ResultValue::Any).take(3 * 128).collect())
        },
    ];

    let all_columns = "peer, data_center, host_id, preferred_ip, rack, release_version, rpc_address, schema_version, tokens";
    assert_query_result(
        connection,
        "SELECT * FROM system.peers;",
        &[&star_results1, &star_results2],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns} FROM system.peers;"),
        &[&star_results1, &star_results2],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns}, {all_columns} FROM system.peers;"),
        &[
            &[star_results1.as_slice(), star_results1.as_slice()].concat(),
            &[star_results2.as_slice(), star_results2.as_slice()].concat(),
        ],
    )
    .await;
}

async fn test_rewrite_system_peers_v2_dummy_peers(
    connection: &CassandraConnection,
    driver: CassandraDriver,
) {
    let star_results1 = [
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Int(7000),
        ResultValue::Varchar("datacenter1".into()),
        ResultValue::Uuid("3c3c4e2d-ba74-4f76-b52e-fb5bcee6a9f4".parse().unwrap()),
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Int(9042),
        ResultValue::Null,
        ResultValue::Null,
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("4.0.6".into()),
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        if let CassandraDriver::Cdrs = driver {
            ResultValue::List(std::iter::repeat(ResultValue::Any).take(3 * 128).collect())
        } else {
            ResultValue::Set(std::iter::repeat(ResultValue::Any).take(3 * 128).collect())
        },
    ];
    let star_results2 = [
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Int(7000),
        ResultValue::Varchar("datacenter1".into()),
        ResultValue::Uuid("fa74d7ec-1223-472b-97de-04a32ccdb70b".parse().unwrap()),
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Int(9042),
        ResultValue::Null,
        ResultValue::Null,
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("4.0.6".into()),
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        if let CassandraDriver::Cdrs = driver {
            ResultValue::List(std::iter::repeat(ResultValue::Any).take(3 * 128).collect())
        } else {
            ResultValue::Set(std::iter::repeat(ResultValue::Any).take(3 * 128).collect())
        },
    ];

    let all_columns = "peer, peer_port, data_center, host_id, native_address, native_port, preferred_ip, preferred_port, rack, release_version, schema_version, tokens";
    assert_query_result(
        connection,
        "SELECT * FROM system.peers_v2;",
        &[&star_results1, &star_results2],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns} FROM system.peers_v2;"),
        &[&star_results1, &star_results2],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns}, {all_columns} FROM system.peers_v2;"),
        &[
            &[star_results1.as_slice(), star_results1.as_slice()].concat(),
            &[star_results2.as_slice(), star_results2.as_slice()].concat(),
        ],
    )
    .await;
}

async fn test_rewrite_system_local(connection: &CassandraConnection, driver: CassandraDriver) {
    let star_results = [
        ResultValue::Varchar("local".into()),
        ResultValue::Varchar("COMPLETED".into()),
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Int(7000),
        ResultValue::Varchar("TestCluster".into()),
        ResultValue::Varchar("3.4.5".into()),
        ResultValue::Varchar("datacenter1".into()),
        // gossip_generation is non deterministic cant assert on it
        ResultValue::Any,
        ResultValue::Uuid("2dd022d6-2937-4754-89d6-02d2933a8f7a".parse().unwrap()),
        ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ResultValue::Int(7000),
        ResultValue::Varchar("5".into()),
        ResultValue::Varchar("org.apache.cassandra.dht.Murmur3Partitioner".into()),
        ResultValue::Varchar("rack1".into()),
        ResultValue::Varchar("4.0.6".into()),
        // rpc_address is non deterministic so we cant assert on it
        ResultValue::Any,
        ResultValue::Int(9042),
        // schema_version is non deterministic so we cant assert on it.
        ResultValue::Any,
        // Unfortunately token generation appears to be non-deterministic but we can at least assert that
        // there are 128 tokens per node
        if let CassandraDriver::Cdrs = driver {
            ResultValue::List(std::iter::repeat(ResultValue::Any).take(3 * 128).collect())
        } else {
            ResultValue::Set(std::iter::repeat(ResultValue::Any).take(3 * 128).collect())
        },
        // truncated_at is non deterministic so we cant assert on it.
        ResultValue::Any,
    ];

    let all_columns =
        "key, bootstrapped, broadcast_address, broadcast_port, cluster_name, cql_version, data_center,
        gossip_generation, host_id, listen_address, listen_port, native_protocol_version, partitioner, rack,
        release_version, rpc_address, rpc_port, schema_version, tokens, truncated_at";

    assert_query_result(connection, "SELECT * FROM system.local;", &[&star_results]).await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns} FROM system.local;"),
        &[&star_results],
    )
    .await;
    assert_query_result(
        connection,
        &format!("SELECT {all_columns}, {all_columns} FROM system.local;"),
        &[&[star_results.as_slice(), star_results.as_slice()].concat()],
    )
    .await;
}

pub async fn test(connection: &CassandraConnection, driver: CassandraDriver) {
    test_rewrite_system_local(connection, driver).await;
    test_rewrite_system_peers(connection).await;
    test_rewrite_system_peers_v2(connection).await;
}

pub async fn test_dummy_peers(connection: &CassandraConnection, driver: CassandraDriver) {
    test_rewrite_system_local(connection, driver).await;
    test_rewrite_system_peers_dummy_peers(connection, driver).await;
    test_rewrite_system_peers_v2_dummy_peers(connection, driver).await;
}

pub async fn test_topology_task(ca_path: Option<&str>, cassandra_port: Option<u32>) {
    let nodes = run_topology_task(ca_path, cassandra_port).await;
    let port = cassandra_port.unwrap_or(9042);

    assert_eq!(nodes.len(), 3);
    let mut possible_addresses: Vec<SocketAddr> = vec![
        format!("172.16.1.2:{port}").parse().unwrap(),
        format!("172.16.1.3:{port}").parse().unwrap(),
        format!("172.16.1.4:{port}").parse().unwrap(),
    ];
    for node in &nodes {
        let address_index = possible_addresses
            .iter()
            .position(|x| *x == node.address)
            .expect("Node did not contain a unique expected address");
        possible_addresses.remove(address_index);

        assert_eq!(node.rack, "rack1");
        assert_eq!(node.tokens.len(), 128);
        assert!(node.is_up);
    }
}

pub async fn test_node_going_down(compose: &mut DockerCompose, driver: CassandraDriver) {
    let mut connection_shotover = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
        .build()
        .await;
    connection_shotover
        .enable_schema_awaiter("172.16.1.2:9044", None)
        .await;
    // Use Replication 2 in case it ends up on the node that we kill
    run_query(&connection_shotover, "CREATE KEYSPACE cluster_single_rack_node_going_down WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };").await;
    run_query(&connection_shotover, "CREATE TABLE cluster_single_rack_node_going_down.test_table (pk varchar PRIMARY KEY, col1 int, col2 boolean);").await;

    // setup data to read
    run_query(&connection_shotover, "INSERT INTO cluster_single_rack_node_going_down.test_table (pk, col1, col2) VALUES ('pk1', 42, true);").await;
    run_query(&connection_shotover, "INSERT INTO cluster_single_rack_node_going_down.test_table (pk, col1, col2) VALUES ('pk2', 413, false);").await;

    let mut event_connections = EventConnections::new().await;

    {
        // stop one of the containers to trigger a status change event.
        // event_connections.direct is connecting to cassandra-one, so make sure to instead kill caassandra-two.
        compose.stop_service("cassandra-two");
        assert_down_event(&mut event_connections).await;

        let new_connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
            .build()
            .await;

        // test that shotover handles connections created before and after node goes down
        test_connection_handles_node_down(&new_connection, driver).await;
        test_connection_handles_node_down(&connection_shotover, driver).await;

        compose.start_service("cassandra-two");
        assert_up_event(&mut event_connections).await;

        // test that shotover handles connections created before and after the nodes goes up
        let new_new_connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
            .build()
            .await;
        test_connection_handles_node_down(&new_new_connection, driver).await;
        test_connection_handles_node_down(&new_connection, driver).await;
        test_connection_handles_node_down(&connection_shotover, driver).await;
    }
    {
        // Kill the service this time instead of stopping it
        compose.kill_service("cassandra-two");
        assert_down_event(&mut event_connections).await;

        let new_connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
            .build()
            .await;

        test_connection_handles_node_down(&new_connection, driver).await;
        test_connection_handles_node_down(&connection_shotover, driver).await;

        compose.start_service("cassandra-two");
        assert_up_event(&mut event_connections).await;

        let new_new_connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
            .build()
            .await;
        test_connection_handles_node_down(&new_new_connection, driver).await;
        test_connection_handles_node_down(&new_connection, driver).await;
        test_connection_handles_node_down(&connection_shotover, driver).await;
    }
    {
        compose.stop_service("cassandra-two");
        assert_down_event(&mut event_connections).await;

        // Test the case where connection_shotover does not receive a message while the node is down,
        // This ensures we handle the case where the outgoing connection is dead but the per connection state never observed the node go down

        compose.start_service("cassandra-two");
        assert_up_event(&mut event_connections).await;

        let new_connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
            .build()
            .await;
        test_connection_handles_node_down_with_one_retry(&new_connection).await;
        if connection_shotover.is(&[CassandraDriver::Cdrs, CassandraDriver::Scylla]) {
            test_connection_handles_node_down_with_one_retry(&connection_shotover).await;
        }
    }
    {
        // Same again but with kill instead of stop
        compose.kill_service("cassandra-two");
        assert_down_event(&mut event_connections).await;

        compose.start_service("cassandra-two");
        assert_up_event(&mut event_connections).await;

        let new_connection = CassandraConnectionBuilder::new("127.0.0.1", 9042, driver)
            .build()
            .await;
        test_connection_handles_node_down_with_one_retry(&new_connection).await;
        if connection_shotover.is(&[CassandraDriver::Cdrs, CassandraDriver::Scylla]) {
            test_connection_handles_node_down_with_one_retry(&connection_shotover).await;
        }
    }
}

struct EventConnections {
    _direct: CassandraConnection,
    recv_direct: broadcast::Receiver<ServerEvent>,
    _shotover: CassandraConnection,
    recv_shotover: broadcast::Receiver<ServerEvent>,
}

impl EventConnections {
    async fn new() -> Self {
        let direct = CassandraConnectionBuilder::new("172.16.1.2", 9044, CassandraDriver::Cdrs)
            .build()
            .await;
        let recv_direct = direct.as_cdrs().create_event_receiver();

        let shotover = CassandraConnectionBuilder::new("127.0.0.1", 9042, CassandraDriver::Cdrs)
            .build()
            .await;
        let recv_shotover = shotover.as_cdrs().create_event_receiver();

        EventConnections {
            _direct: direct,
            recv_direct,
            _shotover: shotover,
            recv_shotover,
        }
    }
}

async fn assert_down_event(event_connections: &mut EventConnections) {
    loop {
        // The direct connection should allow all events to pass through
        let event = timeout(
            Duration::from_secs(120),
            event_connections.recv_direct.recv(),
        )
        .await
        .unwrap()
        .unwrap();

        // Sometimes we get up status events if we connect early enough.
        // I assume these are just due to the nodes initially joining the cluster.
        // If we hit one skip it and continue searching for our expected down status event
        if matches!(
            event,
            ServerEvent::StatusChange(StatusChange {
                change_type: StatusChangeType::Up,
                ..
            })
        ) {
            continue;
        }

        assert_eq!(
            event,
            ServerEvent::StatusChange(StatusChange {
                change_type: StatusChangeType::Down,
                addr: "172.16.1.3:9044".parse().unwrap()
            })
        );
        break;
    }

    // we have already received an event directly from the cassandra instance so its reasonable to
    // expect shotover to have processed that event within 10 seconds if it was ever going to
    timeout(
        Duration::from_secs(10),
        event_connections.recv_shotover.recv(),
    )
    .await
    .expect_err("CassandraSinkCluster must filter out this event");
}

async fn assert_up_event(event_connections: &mut EventConnections) {
    // The direct connection should allow all events to pass through
    let event = timeout(
        Duration::from_secs(120),
        event_connections.recv_direct.recv(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        event,
        ServerEvent::StatusChange(StatusChange {
            change_type: StatusChangeType::Up,
            addr: "172.16.1.3:9044".parse().unwrap()
        })
    );
    // we have already received an event directly from the cassandra instance so its reasonable to
    // expect shotover to have processed that event within 10 seconds if it was ever going to
    timeout(
        Duration::from_secs(10),
        event_connections.recv_shotover.recv(),
    )
    .await
    .expect_err("CassandraSinkCluster must filter out this event");
}

async fn test_connection_handles_node_down(
    connection: &CassandraConnection,
    driver: CassandraDriver,
) {
    // test a query that hits the control node and performs rewriting
    test_rewrite_system_local(connection, driver).await;

    // run this a few times to make sure we arent getting lucky with the routing
    for _ in 0..10 {
        assert_query_result(
            connection,
            "SELECT pk, col1, col2 FROM cluster_single_rack_node_going_down.test_table;",
            &[
                &[
                    ResultValue::Varchar("pk1".into()),
                    ResultValue::Int(42),
                    ResultValue::Boolean(true),
                ],
                &[
                    ResultValue::Varchar("pk2".into()),
                    ResultValue::Int(413),
                    ResultValue::Boolean(false),
                ],
            ],
        )
        .await;
    }
}

async fn test_connection_handles_node_down_with_one_retry(connection: &CassandraConnection) {
    // run this a few times to make sure we arent getting lucky with the routing
    let mut fail_count = 0;
    for _ in 0..50 {
        if connection
            .execute_fallible(
                "SELECT pk, col1, col2 FROM cluster_single_rack_node_going_down.test_table;",
            )
            .await
            .is_err()
        {
            fail_count += 1;
        }
    }
    assert!(
        fail_count == 0 || fail_count == 1,
        "must never fail or fail only once. The case where it fails once indicates that the connection to cassandra-two was dead but recreated allowing the next query to succeed"
    )
}
