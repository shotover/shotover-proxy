use super::*;

async fn create(connection: &CassandraConnection) {
    create_table(connection, "set_native", |s| format!("set<{s}>")).await;
    create_table(connection, "set_set", |s| format!("set<frozen<set<{s}>>>")).await;
    create_table(connection, "set_list", |s| {
        format!("set<frozen<list<{s}>>>")
    })
    .await;
    create_table(connection, "set_map", |s| {
        format!("set<frozen<map<int, {s}>>>")
    })
    .await;
}

async fn insert(connection: &CassandraConnection) {
    insert_table(connection, "set_native", |elements| {
        format!("{{{}}}", elements.join(","))
    })
    .await;
    insert_table(connection, "set_set", |elements| {
        let set = format!("{{{}}}", elements.join(","));
        format!("{{{}}}", (0..3).map(|_| &set).join(","))
    })
    .await;
    insert_table(connection, "set_list", |elements| {
        let list = format!("[{}]", elements.join(","));
        format!("{{{}}}", (0..3).map(|_| &list).join(","))
    })
    .await;
    insert_table(connection, "set_map", |elements| {
        let map = format!(
            "{{{}}}",
            elements
                .iter()
                .enumerate()
                .map(|(i, element)| { format!("{i}: {element}") })
                .join(",")
        );

        format!("{{{}}}", (0..3).map(|_| &map).join(","))
    })
    .await;
}

async fn select(connection: &CassandraConnection, driver: CassandraDriver) {
    select_table(connection, "set_native", ResultValue::Set, driver).await;
    select_table(
        connection,
        "set_set",
        |t| ResultValue::Set(vec![ResultValue::Set(t.clone())]),
        driver,
    )
    .await;
    select_table(
        connection,
        "set_list",
        |t| ResultValue::Set(vec![ResultValue::List(t)]),
        driver,
    )
    .await;
    select_table(
        connection,
        "set_map",
        |t| {
            ResultValue::Set(vec![ResultValue::Map(
                t.iter()
                    .enumerate()
                    .map(|(i, x)| (ResultValue::Int(i as i32), x.clone()))
                    .collect(),
            )])
        },
        driver,
    )
    .await;
}

pub async fn test(connection: &CassandraConnection, driver: CassandraDriver) {
    create(connection).await;
    insert(connection).await;
    select(connection, driver).await;
}
