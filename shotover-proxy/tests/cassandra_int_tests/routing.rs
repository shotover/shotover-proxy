use pretty_assertions::assert_eq;
use test_helpers::connection::cassandra::{CassandraConnectionBuilder, CassandraDriver};

mod single_key {
    use super::assert_eq;
    use test_helpers::connection::cassandra::{CassandraConnection, ResultValue, run_query};

    pub async fn setup_schema(connection: &CassandraConnection) {
        let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_routing_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
        run_query(connection, create_ks).await;

        let create_table_cql = "CREATE TABLE IF NOT EXISTS test_routing_ks.my_test_table_single (key int PRIMARY KEY, name text);";
        run_query(connection, create_table_cql).await;
    }

    pub async fn test(shotover: &CassandraConnection, cassandra: &CassandraConnection) {
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

    use super::assert_eq;
    use test_helpers::connection::cassandra::{CassandraConnection, ResultValue, run_query};

    pub async fn setup_schema(connection: &CassandraConnection) {
        let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_routing_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
        run_query(connection, create_ks).await;

        let create_table_cql = "CREATE TABLE IF NOT EXISTS test_routing_ks.my_test_table_compound (key int, name text, age int, blah text, PRIMARY KEY (key, age)) WITH CLUSTERING ORDER BY (age DESC);";
        run_query(connection, create_table_cql).await;
    }

    pub async fn test(shotover: &CassandraConnection, cassandra: &CassandraConnection) {
        let insert_cql = "INSERT INTO test_routing_ks.my_test_table_compound (key, name, age, blah) VALUES (?, ?, ?, 'blah')";
        let prepared_insert = shotover.prepare(insert_cql).await;

        let select_cql =
            "SELECT blah FROM test_routing_ks.my_test_table_compound WHERE key = ? AND age = ?;";
        let prepared_select = shotover.prepare(select_cql).await;

        let update_cql = "UPDATE test_routing_ks.my_test_table_compound SET blah = 'notblah' WHERE key = ? AND age = ?";
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

    use super::assert_eq;
    use rand::{Rng, distr::Alphanumeric};
    use test_helpers::connection::cassandra::{
        CassandraConnection, Consistency, ResultValue, run_query,
    };

    pub async fn test(shotover: &CassandraConnection, cassandra: &CassandraConnection) {
        simple_test(shotover, cassandra).await;
        types_test(shotover).await;
    }

    pub async fn setup_schema(connection: &CassandraConnection) {
        let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_routing_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
        run_query(connection, create_ks).await;

        let create_table_cql = "CREATE TABLE IF NOT EXISTS test_routing_ks.my_test_table_composite (key int, name text, age int, blah text, PRIMARY KEY((key, name), age));";
        run_query(connection, create_table_cql).await;

        let create_keyspace = "CREATE KEYSPACE stresscql2small WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};";
        let create_table = "CREATE TABLE stresscql2small.typestest (name text, choice boolean, address inet, PRIMARY KEY((name,choice), address)) WITH compaction = { 'class':'LeveledCompactionStrategy' } AND comment='A table of many types to test wide rows'";
        run_query(connection, create_keyspace).await;
        run_query(connection, create_table).await;
    }

    async fn simple_test(shotover: &CassandraConnection, cassandra: &CassandraConnection) {
        let insert_cql = "INSERT INTO test_routing_ks.my_test_table_composite (key, name, age, blah) VALUES (?, ?, ?, 'blah')";
        let prepared_insert = shotover.prepare(insert_cql).await;

        let select_cql = "SELECT blah FROM test_routing_ks.my_test_table_composite WHERE key = ? AND name = ? AND age = ?;";
        let prepared_select = shotover.prepare(select_cql).await;

        let update_cql = "UPDATE test_routing_ks.my_test_table_composite SET blah = 'notblah' WHERE key = ? AND name = ? AND age = ?";
        let prepared_update = cassandra.prepare(update_cql).await;

        let delete_cql = "DELETE FROM test_routing_ks.my_test_table_composite WHERE key = ? AND name = ? AND age = ?;";
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

    async fn types_test(connection: &CassandraConnection) {
        for _ in 0..1000 {
            let name: String = rand::rng()
                .sample_iter(&Alphanumeric)
                .take(48)
                .map(char::from)
                .collect();
            let choice = true;
            let address = "'127.0.0.1'";

            let insert = format!(
                "INSERT INTO stresscql2small.typestest (name, choice, address) VALUES ('{}', {}, {});",
                name, choice, address
            );
            run_query(connection, &insert).await;
        }

        let simple1_cql =
            "select * from stresscql2small.typestest where name = ? and choice = ? LIMIT 1";
        let simple1 = connection.prepare(simple1_cql).await;

        let range1_cql = "select name, choice, address  from stresscql2small.typestest where name = ? and choice = ? LIMIT 10";
        let range = connection.prepare(range1_cql).await;

        let simple2_cql = "select name, choice, address from stresscql2small.typestest where name = ? and choice = ? LIMIT 1";
        let simple2 = connection.prepare(simple2_cql).await;

        let name: String = "0FjhKM4rJQJaniCNHEkKlelmUsYIBJJ9IZuBh44WJTrcPrez".into();

        connection
            .execute_prepared(
                &range,
                &[
                    ResultValue::Varchar(name.clone()),
                    ResultValue::Boolean(true),
                ],
                Consistency::All,
            )
            .await
            .unwrap();

        connection
            .execute_prepared(
                &simple2,
                &[
                    ResultValue::Varchar(name.clone()),
                    ResultValue::Boolean(true),
                ],
                Consistency::All,
            )
            .await
            .unwrap();

        connection
            .execute_prepared(
                &simple1,
                &[ResultValue::Varchar(name), ResultValue::Boolean(true)],
                Consistency::All,
            )
            .await
            .unwrap();
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
    let run = !matches!(driver, CassandraDriver::Cpp);
    #[cfg(not(feature = "cassandra-cpp-driver-tests"))]
    let run = true;

    if run {
        let mut shotover =
            CassandraConnectionBuilder::new(shotover_contact_point, shotover_port, driver)
                .build()
                .await;
        shotover
            .enable_schema_awaiter(
                &format!("{}:{}", cassandra_contact_point, cassandra_port),
                None,
            )
            .await;
        single_key::setup_schema(&shotover).await;
        composite_key::setup_schema(&shotover).await;
        compound_key::setup_schema(&shotover).await;

        // We need to create this `cassandra` connection after we setup the schema to ensure that its routing metadata is completely up to date with the schema changes we just made.
        // We dont need to worry about the metadata of the `shotover` connection as the driver will automatically refresh its metadata due to running schema altering queries.
        let cassandra =
            CassandraConnectionBuilder::new(cassandra_contact_point, cassandra_port, driver)
                .build()
                .await;

        single_key::test(&shotover, &cassandra).await;
        composite_key::test(&shotover, &cassandra).await;
        compound_key::test(&shotover, &cassandra).await;
    }
}
