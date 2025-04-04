mod assert;

use pretty_assertions::assert_eq;
use redis::Commands;
use std::collections::HashSet;
use test_helpers::connection::cassandra::{CassandraConnection, ResultValue, run_query};

pub async fn test(
    cassandra_session: &CassandraConnection,
    valkey_connection: &mut redis::Connection,
) {
    redis::cmd("FLUSHDB").exec(valkey_connection).unwrap();

    run_query(cassandra_session, "CREATE KEYSPACE test_cache_keyspace_simple WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
    run_query(
        cassandra_session,
        "CREATE TABLE test_cache_keyspace_simple.test_table (id int PRIMARY KEY, x int, name varchar);",
    ).await;

    run_query(
        cassandra_session,
        "INSERT INTO test_cache_keyspace_simple.test_table (id, x, name) VALUES (1, 11, 'foo');",
    )
    .await;
    run_query(
        cassandra_session,
        "INSERT INTO test_cache_keyspace_simple.test_table (id, x, name) VALUES (2, 12, 'bar');",
    )
    .await;
    run_query(
        cassandra_session,
        "INSERT INTO test_cache_keyspace_simple.test_table (id, x, name) VALUES (3, 13, 'baz');",
    )
    .await;

    // selects without where clauses do not hit the cache
    assert::assert_query_is_uncacheable(
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
    )
    .await;

    // query against the primary key
    assert::assert_query_is_cached(
        cassandra_session,
        "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE id=1",
        &[&[
            ResultValue::Int(1),
            ResultValue::Int(11),
            ResultValue::Varchar("foo".into()),
        ]],
    )
    .await;

    // ensure key 2 and 3 are also loaded
    assert::assert_query_is_cached(
        cassandra_session,
        "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE id=2",
        &[&[
            ResultValue::Int(2),
            ResultValue::Int(12),
            ResultValue::Varchar("bar".into()),
        ]],
    )
    .await;

    assert::assert_query_is_cached(
        cassandra_session,
        "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE id=3",
        &[&[
            ResultValue::Int(3),
            ResultValue::Int(13),
            ResultValue::Varchar("baz".into()),
        ]],
    )
    .await;

    // query without primary key does not hit the cache
    assert::assert_query_is_uncacheable(
        cassandra_session,
        "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE x=11 ALLOW FILTERING",
        &[&[
            ResultValue::Int(1),
            ResultValue::Int(11),
            ResultValue::Varchar("foo".into()),
        ]],
    )
    .await;

    let result: HashSet<String> = valkey_connection.keys("*").unwrap();
    let expected = HashSet::from([
        "test_cache_keyspace_simple.test_table:1".to_string(),
        "test_cache_keyspace_simple.test_table:2".to_string(),
        "test_cache_keyspace_simple.test_table:3".to_string(),
    ]);
    assert_eq!(result, expected);

    assert::assert_sorted_set_equals(
        valkey_connection,
        "test_cache_keyspace_simple.test_table:1",
        &["id, x, name WHERE "],
    );
    assert::assert_sorted_set_equals(
        valkey_connection,
        "test_cache_keyspace_simple.test_table:2",
        &["id, x, name WHERE "],
    );
    assert::assert_sorted_set_equals(
        valkey_connection,
        "test_cache_keyspace_simple.test_table:3",
        &["id, x, name WHERE "],
    );
}
