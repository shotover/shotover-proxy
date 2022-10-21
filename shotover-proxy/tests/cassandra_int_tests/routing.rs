use crate::helpers::cassandra::{run_query, CassandraConnection, CassandraDriver};

pub async fn create_keyspace(connection: &mut CassandraConnection) {
    let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_routing_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    run_query(connection, create_ks).await;
}

pub async fn create_table(connection: &mut CassandraConnection) {
    let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_routing_ks.my_test_table (key int PRIMARY KEY, name text);";
    run_query(connection, create_table_cql).await;
}

pub async fn test(
    shotover_contact_points: &str,
    shotover_port: u16,
    cassandra_contact_points: &str,
    cassandra_port: u16,
) {
    let mut shotover = CassandraConnection::new(
        shotover_contact_points,
        shotover_port,
        CassandraDriver::Scylla,
    )
    .await;
    shotover
        .enable_schema_awaiter(
            &format!(
                "{}:{}",
                cassandra_contact_points.split(',').next().unwrap(),
                cassandra_port
            ),
            None,
        )
        .await;
    let mut cassandra = CassandraConnection::new(
        cassandra_contact_points,
        cassandra_port,
        CassandraDriver::Scylla,
    )
    .await;

    create_keyspace(&mut shotover).await;
    create_table(&mut cassandra).await;

    let insert_cql = "INSERT INTO test_routing_ks.my_test_table (key, name) VALUES (?, 'my_name')";
    let prepared_insert = shotover.prepare(insert_cql).await;

    let select_cql = "SELECT name FROM test_routing_ks.my_test_table WHERE key = ?;";
    let prepared_select = shotover.prepare(select_cql).await;

    let update_cql = "UPDATE test_routing_ks.my_test_table SET name = 'not_my_name' WHERE key = ?";
    let prepared_update = cassandra.prepare(update_cql).await;

    let delete_cql = "DELETE FROM test_routing_ks.my_test_table WHERE key = ?;";
    let prepared_delete = cassandra.prepare(delete_cql).await;

    for key in 0..100 {
        let shotover_hit = shotover
            .execute_prepared_with_tracing(&prepared_insert, key)
            .await;
        let cassandra_hit = cassandra
            .execute_prepared_with_tracing(&prepared_insert, key)
            .await;
        assert_eq!(shotover_hit, cassandra_hit);
    }

    for key in 0..100 {
        let shotover_hit = shotover
            .execute_prepared_with_tracing(&prepared_select, key)
            .await;
        let cassandra_hit = cassandra
            .execute_prepared_with_tracing(&prepared_select, key)
            .await;
        assert_eq!(shotover_hit, cassandra_hit);
    }

    for key in 0..100 {
        let shotover_hit = shotover
            .execute_prepared_with_tracing(&prepared_update, key)
            .await;
        let cassandra_hit = cassandra
            .execute_prepared_with_tracing(&prepared_update, key)
            .await;
        assert_eq!(shotover_hit, cassandra_hit);
    }

    for key in 0..100 {
        let shotover_hit = shotover
            .execute_prepared_with_tracing(&prepared_delete, key)
            .await;
        let cassandra_hit = cassandra
            .execute_prepared_with_tracing(&prepared_delete, key)
            .await;
        assert_eq!(shotover_hit, cassandra_hit);
    }
}
