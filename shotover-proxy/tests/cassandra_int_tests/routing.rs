use crate::helpers::cassandra::{CassandraConnection, CassandraDriver};

mod single_key {
    use crate::helpers::cassandra::{run_query, CassandraConnection, ResultValue};

    pub async fn create_keyspace(connection: &CassandraConnection) {
        let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_routing_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
        run_query(connection, create_ks).await;
    }

    pub async fn create_table(connection: &CassandraConnection) {
        let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_routing_ks.my_test_table_single (key int PRIMARY KEY, name text);";
        run_query(connection, create_table_cql).await;
    }

    pub async fn test(shotover: &CassandraConnection, cassandra: &CassandraConnection) {
        create_keyspace(shotover).await;
        create_table(shotover).await;

        let insert_cql =
            "INSERT INTO test_routing_ks.my_test_table_single (key, name) VALUES (?, 'my_name')";
        let prepared_insert = shotover.prepare(insert_cql).await;

        let select_cql = "SELECT name FROM test_routing_ks.my_test_table_single WHERE key = ?;";
        let prepared_select = shotover.prepare(select_cql).await;

        let update_cql =
            "UPDATE test_routing_ks.my_test_table_single SET name = 'not_my_name' WHERE key = ?";
        let prepared_update = cassandra.prepare(update_cql).await;

        let delete_cql = "DELETE FROM test_routing_ks.my_test_table_single WHERE key = ?;";
        let prepared_delete = cassandra.prepare(delete_cql).await;

        for key in 0..10 {
            let shotover_hit = shotover
                .execute_prepared_coordinator_node(&prepared_insert, &[ResultValue::Int(key)])
                .await;
            let cassandra_hit = cassandra
                .execute_prepared_coordinator_node(&prepared_insert, &[ResultValue::Int(key)])
                .await;
            assert_eq!(shotover_hit, cassandra_hit);
        }

        for key in 0..10 {
            let shotover_hit = shotover
                .execute_prepared_coordinator_node(&prepared_select, &[ResultValue::Int(key)])
                .await;
            let cassandra_hit = cassandra
                .execute_prepared_coordinator_node(&prepared_select, &[ResultValue::Int(key)])
                .await;
            assert_eq!(shotover_hit, cassandra_hit);
        }

        for key in 0..10 {
            let shotover_hit = shotover
                .execute_prepared_coordinator_node(&prepared_update, &[ResultValue::Int(key)])
                .await;
            let cassandra_hit = cassandra
                .execute_prepared_coordinator_node(&prepared_update, &[ResultValue::Int(key)])
                .await;
            assert_eq!(shotover_hit, cassandra_hit);
        }

        for key in 0..10 {
            let shotover_hit = shotover
                .execute_prepared_coordinator_node(&prepared_delete, &[ResultValue::Int(key)])
                .await;
            let cassandra_hit = cassandra
                .execute_prepared_coordinator_node(&prepared_delete, &[ResultValue::Int(key)])
                .await;
            assert_eq!(shotover_hit, cassandra_hit);
        }
    }
}

mod compound_key {
    use crate::helpers::cassandra::{run_query, CassandraConnection, ResultValue};

    async fn create_keyspace(connection: &CassandraConnection) {
        let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_routing_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
        run_query(connection, create_ks).await;
    }

    async fn create_table(connection: &CassandraConnection) {
        let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_routing_ks.my_test_table_compound (key int, name text, age int, blah text, PRIMARY KEY (key, age)) WITH CLUSTERING ORDER BY (age DESC);";
        run_query(connection, create_table_cql).await;
    }

    pub async fn test(shotover: &CassandraConnection, cassandra: &CassandraConnection) {
        create_keyspace(shotover).await;
        create_table(shotover).await;

        let insert_cql =
            "INSERT INTO test_routing_ks.my_test_table_compound (key, name, age, blah) VALUES (?, ?, ?, 'blah')";
        let prepared_insert = shotover.prepare(insert_cql).await;

        let select_cql =
            "SELECT blah FROM test_routing_ks.my_test_table_compound WHERE key = ? AND age = ?;";
        let prepared_select = shotover.prepare(select_cql).await;

        let update_cql =
            "UPDATE test_routing_ks.my_test_table_compound SET blah = 'notblah' WHERE key = ? AND age = ?";
        let prepared_update = cassandra.prepare(update_cql).await;

        let delete_cql =
            "DELETE FROM test_routing_ks.my_test_table_compound WHERE key = ? AND age = ?;";
        let prepared_delete = cassandra.prepare(delete_cql).await;

        for key in 0..10 {
            let shotover_hit = shotover
                .execute_prepared_coordinator_node(
                    &prepared_insert,
                    &[
                        ResultValue::Int(key),
                        ResultValue::Varchar(format!("name{key}")),
                        ResultValue::Int(key),
                    ],
                )
                .await;
            let cassandra_hit = cassandra
                .execute_prepared_coordinator_node(
                    &prepared_insert,
                    &[
                        ResultValue::Int(key),
                        ResultValue::Varchar(format!("name{key}")),
                        ResultValue::Int(key),
                    ],
                )
                .await;
            assert_eq!(shotover_hit, cassandra_hit);
        }

        for key in 0..10 {
            let shotover_hit = shotover
                .execute_prepared_coordinator_node(
                    &prepared_select,
                    &[ResultValue::Int(key), ResultValue::Int(key)],
                )
                .await;
            let cassandra_hit = cassandra
                .execute_prepared_coordinator_node(
                    &prepared_select,
                    &[ResultValue::Int(key), ResultValue::Int(key)],
                )
                .await;
            assert_eq!(shotover_hit, cassandra_hit);
        }

        for key in 0..10 {
            let shotover_hit = shotover
                .execute_prepared_coordinator_node(
                    &prepared_update,
                    &[ResultValue::Int(key), ResultValue::Int(key)],
                )
                .await;
            let cassandra_hit = cassandra
                .execute_prepared_coordinator_node(
                    &prepared_update,
                    &[ResultValue::Int(key), ResultValue::Int(key)],
                )
                .await;
            assert_eq!(shotover_hit, cassandra_hit);
        }

        for key in 0..10 {
            let shotover_hit = shotover
                .execute_prepared_coordinator_node(
                    &prepared_delete,
                    &[ResultValue::Int(key), ResultValue::Int(key)],
                )
                .await;
            let cassandra_hit = cassandra
                .execute_prepared_coordinator_node(
                    &prepared_delete,
                    &[ResultValue::Int(key), ResultValue::Int(key)],
                )
                .await;
            assert_eq!(shotover_hit, cassandra_hit);
        }
    }
}

mod composite_key {
    use crate::helpers::cassandra::{run_query, CassandraConnection, ResultValue};

    async fn create_keyspace(connection: &CassandraConnection) {
        let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_routing_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
        run_query(connection, create_ks).await;
    }

    async fn create_table(connection: &CassandraConnection) {
        let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_routing_ks.my_test_table_composite (key int, name text, age int, blah text, PRIMARY KEY((key, name), age));";
        run_query(connection, create_table_cql).await;
    }

    pub async fn test(shotover: &CassandraConnection, cassandra: &CassandraConnection) {
        create_keyspace(shotover).await;
        create_table(shotover).await;

        let insert_cql =
            "INSERT INTO test_routing_ks.my_test_table_composite (key, name, age, blah) VALUES (?, ?, ?, 'blah')";
        let prepared_insert = shotover.prepare(insert_cql).await;

        let select_cql = "SELECT blah FROM test_routing_ks.my_test_table_composite WHERE key = ? AND name = ? AND age = ?;";
        let prepared_select = shotover.prepare(select_cql).await;

        let update_cql =
            "UPDATE test_routing_ks.my_test_table_composite SET blah = 'notblah' WHERE key = ? AND name = ? AND age = ?";
        let prepared_update = cassandra.prepare(update_cql).await;

        let delete_cql =
            "DELETE FROM test_routing_ks.my_test_table_composite WHERE key = ? AND name = ? AND age = ?;";
        let prepared_delete = cassandra.prepare(delete_cql).await;

        for key in 0..10 {
            let shotover_hit = shotover
                .execute_prepared_coordinator_node(
                    &prepared_insert,
                    &[
                        ResultValue::Int(key),
                        ResultValue::Varchar(format!("name{key}")),
                        ResultValue::Int(key),
                    ],
                )
                .await;
            let cassandra_hit = cassandra
                .execute_prepared_coordinator_node(
                    &prepared_insert,
                    &[
                        ResultValue::Int(key),
                        ResultValue::Varchar(format!("name{key}")),
                        ResultValue::Int(key),
                    ],
                )
                .await;
            assert_eq!(shotover_hit, cassandra_hit);
        }

        for key in 0..10 {
            let shotover_hit = shotover
                .execute_prepared_coordinator_node(
                    &prepared_select,
                    &[
                        ResultValue::Int(key),
                        ResultValue::Varchar(format!("name{key}")),
                        ResultValue::Int(key),
                    ],
                )
                .await;
            let cassandra_hit = cassandra
                .execute_prepared_coordinator_node(
                    &prepared_select,
                    &[
                        ResultValue::Int(key),
                        ResultValue::Varchar(format!("name{key}")),
                        ResultValue::Int(key),
                    ],
                )
                .await;
            assert_eq!(shotover_hit, cassandra_hit);
        }

        for key in 0..10 {
            let shotover_hit = shotover
                .execute_prepared_coordinator_node(
                    &prepared_update,
                    &[
                        ResultValue::Int(key),
                        ResultValue::Varchar(format!("name{key}")),
                        ResultValue::Int(key),
                    ],
                )
                .await;
            let cassandra_hit = cassandra
                .execute_prepared_coordinator_node(
                    &prepared_update,
                    &[
                        ResultValue::Int(key),
                        ResultValue::Varchar(format!("name{key}")),
                        ResultValue::Int(key),
                    ],
                )
                .await;
            assert_eq!(shotover_hit, cassandra_hit);
        }

        for key in 0..10 {
            let shotover_hit = shotover
                .execute_prepared_coordinator_node(
                    &prepared_delete,
                    &[
                        ResultValue::Int(key),
                        ResultValue::Varchar(format!("name{key}")),
                        ResultValue::Int(key),
                    ],
                )
                .await;
            let cassandra_hit = cassandra
                .execute_prepared_coordinator_node(
                    &prepared_delete,
                    &[
                        ResultValue::Int(key),
                        ResultValue::Varchar(format!("name{key}")),
                        ResultValue::Int(key),
                    ],
                )
                .await;
            assert_eq!(shotover_hit, cassandra_hit);
        }
    }
}

pub async fn test(
    shotover_contact_point: &str,
    shotover_port: u16,
    cassandra_contact_point: &str,
    cassandra_port: u16,
    driver: CassandraDriver,
) {
    // execute_prepared_coordinator_node doesnt support cassandra-cpp yet.
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    let run = !matches!(driver, CassandraDriver::Datastax);
    #[cfg(not(feature = "cassandra-cpp-driver-tests"))]
    let run = true;

    if run {
        let mut shotover =
            CassandraConnection::new(shotover_contact_point, shotover_port, driver).await;
        shotover
            .enable_schema_awaiter(
                &format!("{}:{}", cassandra_contact_point, cassandra_port),
                None,
            )
            .await;
        let cassandra =
            CassandraConnection::new(cassandra_contact_point, cassandra_port, driver).await;

        single_key::test(&shotover, &cassandra).await;
        composite_key::test(&shotover, &cassandra).await;
        compound_key::test(&shotover, &cassandra).await;
    }
}
