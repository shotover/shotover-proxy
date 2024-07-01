use super::*;

async fn create(connection: &CassandraConnection) {
    create_table(connection, "vector_native", |s| format!("vector<{s}, 2>")).await;
    // create_table(connection, "vector_set", |s| {
    //     format!("vector<frozen<set<{s}>>, 3>")
    // })
    // .await;
    // create_table(connection, "vector_list", |s| {
    //     format!("vector<frozen<list<{s}>>, 3>")
    // })
    // .await;
    // create_table(connection, "vector_map", |s| {
    //     format!("vector<frozen<map<int, {s}>>, 3>")
    // })
    // .await;
}

async fn insert(connection: &CassandraConnection) {
    insert_table(connection, "vector_native", |elements| {
        format!("[{}]", elements[0..2].join(","))
    })
    .await;
    // insert_table(connection, "vector_set", |elements| {
    //     let set: String = format!("{{{}}}", elements.join(","));
    //     format!("[{}]", (0..3).map(|_| &set).join(","))
    // })
    // .await;
    // insert_table(connection, "vector_list", |elements| {
    //     let list = format!("[{}]", elements.join(","));
    //     format!("[{}]", (0..3).map(|_| &list).join(","))
    // })
    // .await;
    // insert_table(connection, "vector_map", |elements| {
    //     let map = format!(
    //         "{{{}}}",
    //         elements
    //             .iter()
    //             .enumerate()
    //             .map(|(i, element)| { format!("{i}: {element}") })
    //             .join(",")
    //     );

    //     format!("[{}]", (0..3).map(|_| &map).join(","))
    // })
    // .await;
}

async fn select(connection: &CassandraConnection, driver: CassandraDriver) {
    select_table(
        connection,
        "vector_native",
        |t| ResultValue::Vector(t[0..2].to_vec()),
        driver,
    )
    .await;
    // select_table(
    //     connection,
    //     "vector_set",
    //     |t| {
    //         ResultValue::Vector(vec![
    //             ResultValue::Set(t.clone()),
    //             ResultValue::Set(t.clone()),
    //             ResultValue::Set(t),
    //         ])
    //     },
    //     driver,
    // )
    // .await;
    // select_table(
    //     connection,
    //     "vector_list",
    //     |t| {
    //         ResultValue::Vector(vec![
    //             ResultValue::List(t.clone()),
    //             ResultValue::List(t.clone()),
    //             ResultValue::List(t),
    //         ])
    //     },
    //     driver,
    // )
    // .await;
    // select_table(
    //     connection,
    //     "vector_map",
    //     |t| {
    //         let map = ResultValue::Map(
    //             t.iter()
    //                 .enumerate()
    //                 .map(|(i, x)| (ResultValue::Int(i as i32), x.clone()))
    //                 .collect(),
    //         );
    //         ResultValue::Vector(vec![map.clone(), map.clone(), map])
    //     },
    //     driver,
    // )
    // .await;
}

pub async fn test(connection: &CassandraConnection, driver: CassandraDriver) {
    create(connection).await;
    insert(connection).await;
    select(connection, driver).await;
}
