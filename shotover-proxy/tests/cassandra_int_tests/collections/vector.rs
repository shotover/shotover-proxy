use super::*;

// We dont use the collection abstractions used by list/map/set since vectors only support a small subset of data types.

async fn create(connection: &CassandraConnection) {
    run_query(
        connection,
        "CREATE TABLE collections.vector (id int PRIMARY KEY, col0 vector<int, 1>, col1 vector<bigint, 2>, col2 vector<float, 2>, col3 vector<double, 2>);",
    )
    .await;
}

async fn insert(connection: &CassandraConnection) {
    run_query(
        connection,
        "INSERT INTO collections.vector (id, col0, col1, col2, col3) VALUES (1, [1], [2, 3], [4.1, 5.2], [6.1, 7.2]);",
    )
    .await;
}

async fn select(connection: &CassandraConnection) {
    let results = vec![
        ResultValue::Vector(vec![ResultValue::Int(1)]),
        ResultValue::Vector(vec![ResultValue::BigInt(2), ResultValue::BigInt(3)]),
        ResultValue::Vector(vec![
            ResultValue::Float(4.1.into()),
            ResultValue::Float(5.2.into()),
        ]),
        ResultValue::Vector(vec![
            ResultValue::Double(6.1.into()),
            ResultValue::Double(7.2.into()),
        ]),
    ];

    assert_query_result(
        connection,
        "SELECT col0, col1, col2, col3 FROM collections.vector;",
        &[&results],
    )
    .await;
}

pub async fn test(connection: &CassandraConnection) {
    create(connection).await;
    insert(connection).await;
    select(connection).await;
}
