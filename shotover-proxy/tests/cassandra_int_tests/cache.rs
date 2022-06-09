use crate::helpers::cassandra::{assert_query_result, run_query, ResultValue};
use cassandra_cpp::Session;
use redis::Commands;
use std::collections::HashSet;

pub fn test(cassandra_session: &Session, redis_connection: &mut redis::Connection) {
    test_batch_insert(cassandra_session, redis_connection);
    test_simple(cassandra_session, redis_connection);
}

fn test_batch_insert(cassandra_session: &Session, redis_connection: &mut redis::Connection) {
    redis::cmd("FLUSHDB").execute(redis_connection);

    run_query(cassandra_session, "CREATE KEYSPACE test_cache_keyspace_batch_insert WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    run_query(
            cassandra_session,
            "CREATE TABLE test_cache_keyspace_batch_insert.test_table (id int PRIMARY KEY, x int, name varchar);",
        );
    run_query(
        cassandra_session,
        r#"BEGIN BATCH
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (1, 11, 'foo');
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (2, 12, 'bar');
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (3, 13, 'baz');
            APPLY BATCH;"#,
    );

    // TODO: SELECTS without a WHERE do not hit cache
    assert_query_result(
        cassandra_session,
        "SELECT id, x, name FROM test_cache_keyspace_batch_insert.test_table",
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
        cassandra_session,
        "SELECT id, x, name FROM test_cache_keyspace_batch_insert.test_table WHERE id=1",
        &[],
    );

    // query against some other field
    assert_query_result(
        cassandra_session,
        "SELECT id, x, name FROM test_cache_keyspace_batch_insert.test_table WHERE x=11",
        &[],
    );

    // Insert a dummy key to ensure the keys command is working correctly, we can remove this later.
    redis_connection
        .set::<&str, i32, ()>("dummy_key", 1)
        .unwrap();
    let result: Vec<String> = redis_connection.keys("*").unwrap();
    assert_eq!(result, ["dummy_key".to_string()]);
}

fn test_simple(cassandra_session: &Session, redis_connection: &mut redis::Connection) {
    redis::cmd("FLUSHDB").execute(redis_connection);

    run_query(cassandra_session, "CREATE KEYSPACE test_cache_keyspace_simple WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    run_query(
            cassandra_session,
            "CREATE TABLE test_cache_keyspace_simple.test_table (id int PRIMARY KEY, x int, name varchar);",
        );

    run_query(
        cassandra_session,
        "INSERT INTO test_cache_keyspace_simple.test_table (id, x, name) VALUES (1, 11, 'foo');",
    );
    run_query(
        cassandra_session,
        "INSERT INTO test_cache_keyspace_simple.test_table (id, x, name) VALUES (2, 12, 'bar');",
    );
    run_query(
        cassandra_session,
        "INSERT INTO test_cache_keyspace_simple.test_table (id, x, name) VALUES (3, 13, 'baz');",
    );

    // TODO: SELECTS without a WHERE do not hit cache
    assert_query_result(
        cassandra_session,
        "SELECT id, x, name FROM test_cache_keyspace_simple.test_table",
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
        cassandra_session,
        "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE id=1",
        &[],
    );

    // query against some other field
    assert_query_result(
        cassandra_session,
        "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE x=11",
        &[],
    );

    let result: HashSet<String> = redis_connection.keys("*").unwrap();
    let expected: HashSet<String> = ["1", "2", "3"].into_iter().map(|x| x.to_string()).collect();
    assert_eq!(result, expected);

    assert_sorted_set_equals(redis_connection, "1", &["1:11", "1:foo"]);
    assert_sorted_set_equals(redis_connection, "2", &["2:12", "2:bar"]);
    assert_sorted_set_equals(redis_connection, "3", &["3:13", "3:baz"]);
}

fn assert_sorted_set_equals(
    redis_connection: &mut redis::Connection,
    key: &str,
    expected_values: &[&str],
) {
    let expected_values: HashSet<String> = expected_values.iter().map(|x| x.to_string()).collect();
    let values = redis_connection
        .zrange::<&str, HashSet<String>>(key, 0, -1)
        .unwrap();
    assert_eq!(values, expected_values)
}
