use super::*;

async fn create(connection: &CassandraConnection) {
    create_table(connection, "map_native", |s| format!("map<int, {s}>")).await;
    create_table(connection, "map_set", |s| {
        format!("map<int, frozen<set<{s}>>>")
    })
    .await;
    create_table(connection, "map_list", |s| {
        format!("map<int, frozen<list<{s}>>>")
    })
    .await;
    create_table(connection, "map_map", |s| {
        format!("map<int, frozen<map<int, {s}>>>")
    })
    .await;
}

async fn insert(connection: &CassandraConnection) {
    insert_table(connection, "map_native", |elements| {
        format!(
            "{{{}}}",
            elements
                .iter()
                .enumerate()
                .map(|(i, element)| format!("{i}: {element}"))
                .join(",")
        )
    })
    .await;
    insert_table(connection, "map_set", |elements| {
        let set = format!("{{{}}}", elements.join(","));
        format!("{{{}}}", (0..3).map(|i| format!("{i}: {set}")).join(","))
    })
    .await;
    insert_table(connection, "map_list", |elements| {
        let list = format!("[{}]", elements.join(","));
        format!("{{{}}}", (0..3).map(|i| format!("{i}: {list}")).join(","))
    })
    .await;
    insert_table(connection, "map_map", |elements| {
        let map = format!(
            "{{{}}}",
            elements
                .iter()
                .enumerate()
                .map(|(i, element)| { format!("{i}: {element}") })
                .join(",")
        );

        format!("{{{}}}", (0..3).map(|i| format!("{i}: {map}")).join(","))
    })
    .await;
}

async fn select(connection: &CassandraConnection, driver: CassandraDriver) {
    select_table(
        connection,
        "map_native",
        |t| {
            ResultValue::Map(
                t.iter()
                    .enumerate()
                    .map(|(i, x)| (ResultValue::Int(i as i32), x.clone()))
                    .collect(),
            )
        },
        driver,
    )
    .await;
    select_table(
        connection,
        "map_set",
        |t| {
            ResultValue::Map(vec![
                (ResultValue::Int(0), ResultValue::Set(t.clone())),
                (ResultValue::Int(1), ResultValue::Set(t.clone())),
                (ResultValue::Int(2), ResultValue::Set(t)),
            ])
        },
        driver,
    )
    .await;
    select_table(
        connection,
        "map_list",
        |t| {
            ResultValue::Map(vec![
                (ResultValue::Int(0), ResultValue::List(t.clone())),
                (ResultValue::Int(1), ResultValue::List(t.clone())),
                (ResultValue::Int(2), ResultValue::List(t)),
            ])
        },
        driver,
    )
    .await;
    select_table(
        connection,
        "map_map",
        |t| {
            let map = ResultValue::Map(
                t.iter()
                    .enumerate()
                    .map(|(i, x)| (ResultValue::Int(i as i32), x.clone()))
                    .collect(),
            );
            ResultValue::Map(vec![
                (ResultValue::Int(0), map.clone()),
                (ResultValue::Int(1), map.clone()),
                (ResultValue::Int(2), map),
            ])
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
