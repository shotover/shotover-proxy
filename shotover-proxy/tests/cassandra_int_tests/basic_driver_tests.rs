use crate::helpers::ShotoverManager;
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;

use crate::cassandra_int_tests::cassandra_connection;

mod keyspace {
    use cassandra_cpp::Session;

    use crate::cassandra_int_tests::{
        assert_query_result, assert_query_result_contains_row, run_query, ResultValue,
    };

    fn test_create_keyspace(session: &Session) {
        run_query(session, "CREATE KEYSPACE keyspace_tests_create WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        assert_query_result(
            session,
            "SELECT release_version FROM system.local",
            &[&[ResultValue::Varchar("3.11.10".into())]],
        );

        assert_query_result_contains_row(
            session,
            "SELECT keyspace_name FROM system_schema.keyspaces;",
            &[ResultValue::Varchar("keyspace_tests_create".into())],
        );
    }

    fn test_use_keyspace(session: &Session) {
        run_query(session, "USE system");

        assert_query_result(
            session,
            "SELECT release_version FROM local",
            &[&[ResultValue::Varchar("3.11.10".into())]],
        );
    }

    fn test_drop_keyspace(session: &Session) {
        run_query(session, "CREATE KEYSPACE keyspace_tests_delete_me WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        assert_query_result(
            session,
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='keyspace_tests_delete_me';",
            &[&[ResultValue::Varchar("keyspace_tests_delete_me".into())]],
        );
        run_query(session, "DROP KEYSPACE keyspace_tests_delete_me");
        run_query(
            session,
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='keyspace_tests_delete_me';",
        );
    }

    fn test_alter_keyspace(session: &Session) {
        run_query(session, "CREATE KEYSPACE keyspace_tests_alter_me WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES = false;");
        run_query(
            session,
            "ALTER KEYSPACE keyspace_tests_alter_me WITH DURABLE_WRITES = true;",
        );
        assert_query_result(
            session,
            "SELECT durable_writes FROM system_schema.keyspaces WHERE keyspace_name='keyspace_tests_alter_me'",
            &[&[ResultValue::Boolean(true)]],
        );
    }

    pub fn test(session: &Session) {
        test_create_keyspace(session);
        test_use_keyspace(session);
        test_drop_keyspace(session);
        test_alter_keyspace(session);
    }
}

mod table {
    use cassandra_cpp::Session;

    use crate::cassandra_int_tests::{
        assert_query_result, assert_query_result_contains_row,
        assert_query_result_not_contains_row, run_query, ResultValue,
    };

    fn test_create_table(session: &Session) {
        run_query(
            session,
            "CREATE TABLE test_table_keyspace.my_table (id UUID PRIMARY KEY, name text, age int);",
        );
        assert_query_result_contains_row(
            session,
            "SELECT table_name FROM system_schema.tables;",
            &[ResultValue::Varchar("my_table".into())],
        );
    }

    fn test_drop_table(session: &Session) {
        run_query(
            session,
            "CREATE TABLE test_table_keyspace.delete_me (id UUID PRIMARY KEY, name text, age int);",
        );

        assert_query_result_contains_row(
            session,
            "SELECT table_name FROM system_schema.tables;",
            &[ResultValue::Varchar("delete_me".into())],
        );
        run_query(session, "DROP TABLE test_table_keyspace.delete_me;");
        assert_query_result_not_contains_row(
            session,
            "SELECT table_name FROM system_schema.tables;",
            &[ResultValue::Varchar("delete_me".into())],
        );
    }

    fn test_alter_table(session: &Session) {
        run_query(
            session,
            "CREATE TABLE test_table_keyspace.alter_me (id UUID PRIMARY KEY, name text, age int);",
        );

        assert_query_result(session, "SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'test_table_keyspace' AND table_name = 'alter_me' AND column_name = 'age';", &[&[ResultValue::Varchar("age".into())]]);
        run_query(
            session,
            "ALTER TABLE test_table_keyspace.alter_me RENAME id TO new_id",
        );
        assert_query_result(session, "SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'test_table_keyspace' AND table_name = 'alter_me' AND column_name = 'new_id';", &[&[ResultValue::Varchar("new_id".into())]]);
    }

    pub fn test(session: &Session) {
        run_query(session, "CREATE KEYSPACE test_table_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        test_create_table(session);
        test_drop_table(session);
        test_alter_table(session);
    }
}

mod udt {
    use cassandra_cpp::{stmt, Session};

    use crate::cassandra_int_tests::run_query;

    fn test_create_udt(session: &Session) {
        run_query(
            session,
            "CREATE TYPE test_type_keyspace.test_type_name (foo text, bar int)",
        );
        run_query(
            session,
            "CREATE TABLE test_type_keyspace.test_table (id int PRIMARY KEY, foo test_type_name);",
        );
        run_query(
            session,
            "INSERT INTO test_type_keyspace.test_table (id, foo) VALUES (1, {foo: 'yes', bar: 1})",
        );
    }

    fn test_drop_udt(session: &Session) {
        run_query(
            session,
            "CREATE TYPE test_type_keyspace.test_type_drop_me (foo text, bar int)",
        );
        run_query(session, "DROP TYPE test_type_keyspace.test_type_drop_me;");
        let statement = stmt!(
            "CREATE TABLE test_type_keyspace.test_delete_table (id int PRIMARY KEY, foo test_type_drop_me);"
        );
        let result = session.execute(&statement).wait().unwrap_err().to_string();
        assert_eq!(result, "Cassandra detailed error SERVER_INVALID_QUERY: Unknown type test_type_keyspace.test_type_drop_me");
    }

    pub fn test(session: &Session) {
        run_query(session, "CREATE KEYSPACE test_type_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        test_create_udt(session);
        test_drop_udt(session);
    }
}

mod functions {
    use cassandra_cpp::{stmt, Session};

    use crate::cassandra_int_tests::{assert_query_result, run_query, ResultValue};

    fn drop_function(session: &Session) {
        assert_query_result(session, "SELECT test_function_keyspace.my_function(x, y) FROM test_function_keyspace.test_function_table WHERE id=1;", &[&[ResultValue::Int(4)]]);
        run_query(session, "DROP FUNCTION test_function_keyspace.my_function");

        let statement = stmt!("SELECT test_function_keyspace.my_function(x) FROM test_function_keyspace.test_function_table WHERE id=1;");
        let result = session.execute(&statement).wait().unwrap_err().to_string();

        assert_eq!(result, "Cassandra detailed error SERVER_INVALID_QUERY: Unknown function 'test_function_keyspace.my_function'");
    }

    fn create_function(session: &Session) {
        run_query(
            session,
            "CREATE FUNCTION test_function_keyspace.my_function (a int, b int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE javascript AS 'a * b';",
        );
        assert_query_result(session, "SELECT test_function_keyspace.my_function(x, y) FROM test_function_keyspace.test_function_table;",&[&[ResultValue::Int(4)], &[ResultValue::Int(9)], &[ResultValue::Int(16)]]);
    }

    pub fn test(session: &Session) {
        run_query(session, "CREATE KEYSPACE test_function_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        run_query(
            session,
            "CREATE TABLE test_function_keyspace.test_function_table (id int PRIMARY KEY, x int, y int);",
        );
        run_query(
            session,
            r#"BEGIN BATCH
INSERT INTO test_function_keyspace.test_function_table (id, x, y) VALUES (1, 2, 2);
INSERT INTO test_function_keyspace.test_function_table (id, x, y) VALUES (2, 3, 3);
INSERT INTO test_function_keyspace.test_function_table (id, x, y) VALUES (3, 4, 4);
APPLY BATCH;"#,
        );

        create_function(session);
        drop_function(session);
    }
}

mod cache {
    use cassandra_cpp::Session;
    use redis::Commands;

    use crate::cassandra_int_tests::{assert_query_result, run_query, ResultValue};

    pub fn test(session: &Session, redis_connection: &mut redis::Connection) {
        run_query(session, "CREATE KEYSPACE test_cache_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        run_query(
            session,
            "CREATE TABLE test_cache_keyspace.test_table (id int PRIMARY KEY, x int, name varchar);",
        );
        run_query(
            session,
            r#"BEGIN BATCH
                INSERT INTO test_cache_keyspace.test_table (id, x, name) VALUES (1, 11, 'foo');
                INSERT INTO test_cache_keyspace.test_table (id, x, name) VALUES (2, 12, 'bar');
                INSERT INTO test_cache_keyspace.test_table (id, x, name) VALUES (3, 13, 'baz');
            APPLY BATCH;"#,
        );

        // TODO: SELECTS without a WHERE do not get cached
        assert_query_result(
            session,
            "SELECT id, x, name FROM test_cache_keyspace.test_table",
            &[
                &[
                    ResultValue::Int(1),
                    ResultValue::Int(11),
                    ResultValue::Varchar("foo".into()),
                ],
                &[
                    ResultValue::Int(2),
                    ResultValue::Int(12),
                    ResultValue::Varchar("bar".into()),
                ],
                &[
                    ResultValue::Int(3),
                    ResultValue::Int(13),
                    ResultValue::Varchar("baz".into()),
                ],
            ],
        );

        // query against the primary key
        assert_query_result(
            session,
            "SELECT id, x, name FROM test_cache_keyspace.test_table WHERE id=1",
            &[],
        );

        // query against some other field
        assert_query_result(
            session,
            "SELECT id, x, name FROM test_cache_keyspace.test_table WHERE x=11",
            &[],
        );

        // Insert a dummy key to ensure the keys command is working correctly, we can remove this later.
        redis_connection
            .set::<&str, i32, ()>("dummy_key", 1)
            .unwrap();
        let result: Vec<String> = redis_connection.keys("*").unwrap();
        assert_eq!(result, ["dummy_key".to_string()]);
    }
}

#[test]
#[serial]
fn test_cluster() {
    let _compose = DockerCompose::new("examples/cassandra-cluster/docker-compose.yml")
        .wait_for_n_t("Startup complete", 3, 90);

    let _handles: Vec<_> = [
        "examples/cassandra-cluster/topology1.yaml",
        "examples/cassandra-cluster/topology2.yaml",
        "examples/cassandra-cluster/topology3.yaml",
    ]
    .into_iter()
    .map(ShotoverManager::from_topology_file_without_observability)
    .collect();

    let connection = cassandra_connection("127.0.0.1", 9042);

    keyspace::test(&connection);
    table::test(&connection);
    udt::test(&connection);
    functions::test(&connection);
}

#[test]
#[serial]
fn test_passthrough() {
    let _compose = DockerCompose::new("examples/cassandra-passthrough/docker-compose.yml")
        .wait_for_n_t("Startup complete", 1, 90);

    let _shotover_manager =
        ShotoverManager::from_topology_file("examples/cassandra-passthrough/topology.yaml");

    let connection = cassandra_connection("127.0.0.1", 9042);

    keyspace::test(&connection);
    table::test(&connection);
    udt::test(&connection);
    functions::test(&connection);
}

#[test]
#[serial]
fn test_cassandra_redis_cache() {
    let _compose = DockerCompose::new("examples/cassandra-redis-cache/docker-compose.yml")
        .wait_for_n_t("Startup complete", 1, 90);

    let shotover_manager =
        ShotoverManager::from_topology_file("examples/cassandra-redis-cache/topology.yaml");

    let mut redis_connection = shotover_manager.redis_connection(6379);
    let connection = cassandra_connection("127.0.0.1", 9042);

    keyspace::test(&connection);
    table::test(&connection);
    udt::test(&connection);
    functions::test(&connection);
    cache::test(&connection, &mut redis_connection);
}
