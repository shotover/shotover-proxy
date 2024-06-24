use super::*;

async fn create(connection: &CassandraConnection) {
    create_table(connection, "list_native", |s| format!("list<{s}>")).await;
    create_table(connection, "list_set", |s| {
        format!("list<frozen<set<{s}>>>")
    })
    .await;
    create_table(connection, "list_list", |s| {
        format!("list<frozen<list<{s}>>>")
    })
    .await;
    create_table(connection, "list_map", |s| {
        format!("list<frozen<map<int, {s}>>>")
    })
    .await;
}

async fn insert(connection: &CassandraConnection) {
    insert_table(connection, "list_native", |elements| {
        format!("[{}]", elements.join(","))
    })
    .await;
    insert_table(connection, "list_set", |elements| {
        let set: String = format!("{{{}}}", elements.join(","));
        format!("[{}]", (0..3).map(|_| &set).join(","))
    })
    .await;
    insert_table(connection, "list_list", |elements| {
        let list = format!("[{}]", elements.join(","));
        format!("[{}]", (0..3).map(|_| &list).join(","))
    })
    .await;
    insert_table(connection, "list_map", |elements| {
        let map = format!(
            "{{{}}}",
            elements
                .iter()
                .enumerate()
                .map(|(i, element)| { format!("{i}: {element}") })
                .join(",")
        );

        format!("[{}]", (0..3).map(|_| &map).join(","))
    })
    .await;
}

async fn select(connection: &CassandraConnection, driver: CassandraDriver) {
    select_table(connection, "list_native", ResultValue::List, driver).await;
    select_table(
        connection,
        "list_set",
        |t| {
            ResultValue::List(vec![
                ResultValue::Set(t.clone()),
                ResultValue::Set(t.clone()),
                ResultValue::Set(t),
            ])
        },
        driver,
    )
    .await;
    select_table(
        connection,
        "list_list",
        |t| {
            ResultValue::List(vec![
                ResultValue::List(t.clone()),
                ResultValue::List(t.clone()),
                ResultValue::List(t),
            ])
        },
        driver,
    )
    .await;
    select_table(
        connection,
        "list_map",
        |t| {
            let map = ResultValue::Map(
                t.iter()
                    .enumerate()
                    .map(|(i, x)| (ResultValue::Int(i as i32), x.clone()))
                    .collect(),
            );
            ResultValue::List(vec![map.clone(), map.clone(), map])
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
