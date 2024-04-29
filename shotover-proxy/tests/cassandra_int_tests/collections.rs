use cassandra_protocol::frame::message_result::ColType;
use itertools::Itertools;
use test_helpers::connection::cassandra::{
    assert_query_result, run_query, CassandraConnection, CassandraDriver, ResultValue,
};

const NATIVE_COL_TYPES: [ColType; 17] = [
    ColType::Ascii,
    ColType::Bigint,
    ColType::Blob,
    // ColType::Boolean,
    ColType::Decimal,
    ColType::Double,
    ColType::Float,
    ColType::Int,
    ColType::Timestamp,
    ColType::Uuid,
    ColType::Varchar,
    ColType::Varint,
    ColType::Timeuuid,
    ColType::Inet,
    ColType::Date,
    ColType::Time,
    ColType::Smallint,
    ColType::Tinyint,
];

fn get_type_str(col_type: ColType) -> &'static str {
    match col_type {
        ColType::Custom => "custom",
        ColType::Ascii => "ascii",
        ColType::Bigint => "bigint",
        ColType::Blob => "blob",
        ColType::Boolean => "boolean",
        ColType::Counter => "counter",
        ColType::Decimal => "decimal",
        ColType::Double => "double",
        ColType::Float => "float",
        ColType::Int => "int",
        ColType::Timestamp => "timestamp",
        ColType::Uuid => "uuid",
        ColType::Varchar => "varchar",
        ColType::Varint => "varint",
        ColType::Timeuuid => "timeuuid",
        ColType::Inet => "inet",
        ColType::Date => "date",
        ColType::Time => "time",
        ColType::Smallint => "smallint",
        ColType::Tinyint => "tinyint",
        ColType::List => "list",
        ColType::Map => "map",
        ColType::Set => "set",
        ColType::Udt => "udt",
        ColType::Tuple => "tuple",
        ColType::Duration => "duration",
        _ => unreachable!(),
    }
}

fn get_type_example(col_type: ColType) -> Vec<&'static str> {
    match col_type {
        ColType::Ascii => vec!["'ascii string'", "'other string'", "'other string 2'"],
        ColType::Bigint => vec!["1844674407370", "1844674407371", "1844674407372"],
        ColType::Blob => vec!["bigIntAsBlob(10)", "bigIntAsBlob(11)", "bigIntAsBlob(12)"],
        ColType::Boolean => vec!["true"],
        ColType::Counter => vec!["12", "13", "14"],
        ColType::Decimal => vec!["1.111", "1.112", "1.113"],
        ColType::Double => vec!["1.11", "1.12", "1.13"],
        ColType::Float => vec!["1.11", "1.12", "1.13"],
        ColType::Int => vec!["1", "2", "3"],
        ColType::Timestamp => vec![
            "'2011-02-03 04:01+0000'",
            "'2011-02-03 04:02+0000'",
            "'2011-02-03 04:03+0000'",
        ],
        ColType::Uuid => vec![
            "84196262-53de-11ec-bf63-0242ac130001",
            "84196262-53de-11ec-bf63-0242ac130002",
            "84196262-53de-11ec-bf63-0242ac130003",
        ],
        ColType::Varchar => vec!["'varchar 1'", "'varchar 2'", "'varchar 3'"],
        ColType::Varint => vec!["198121", "198122", "198123"],
        ColType::Timeuuid => vec![
            "84196262-53de-11ec-bf63-0242ac130001",
            "84196262-53de-11ec-bf63-0242ac130002",
            "84196262-53de-11ec-bf63-0242ac130003",
        ],
        ColType::Inet => vec!["'127.0.0.1'", "'127.0.0.2'", "'127.0.0.3'"],
        ColType::Date => vec!["'2011-02-01'", "'2011-02-02'", "'2011-02-03'"],
        ColType::Time => vec!["'08:12:54'", "'08:12:55'", "'08:12:56'"],
        ColType::Smallint => vec!["32765", "32766", "32767"],
        ColType::Tinyint => vec!["121", "122", "123"],
        _ => panic!("dont have an example for {}", col_type),
    }
}

fn get_type_example_result_value(col_type: ColType) -> Vec<ResultValue> {
    match col_type {
        ColType::Ascii => vec![
            ResultValue::Ascii("ascii string".into()),
            ResultValue::Ascii("other string".into()),
            ResultValue::Ascii("other string 2".into()),
        ],
        ColType::Bigint => vec![
            ResultValue::BigInt(1844674407370),
            ResultValue::BigInt(1844674407371),
            ResultValue::BigInt(1844674407372),
        ],
        ColType::Blob => vec![
            ResultValue::Blob(vec![0, 0, 0, 0, 0, 0, 0, 10]),
            ResultValue::Blob(vec![0, 0, 0, 0, 0, 0, 0, 11]),
            ResultValue::Blob(vec![0, 0, 0, 0, 0, 0, 0, 12]),
        ],
        ColType::Boolean => vec![ResultValue::Boolean(true)],
        ColType::Counter => vec![
            ResultValue::Counter(12),
            ResultValue::Counter(13),
            ResultValue::Counter(14),
        ],
        ColType::Decimal => vec![
            ResultValue::Decimal(vec![0, 0, 0, 3, 4, 87]),
            ResultValue::Decimal(vec![0, 0, 0, 3, 4, 88]),
            ResultValue::Decimal(vec![0, 0, 0, 3, 4, 89]),
        ],
        ColType::Double => vec![
            ResultValue::Double(1.11.into()),
            ResultValue::Double(1.12.into()),
            ResultValue::Double(1.13.into()),
        ],
        ColType::Float => vec![
            ResultValue::Float(1.11.into()),
            ResultValue::Float(1.12.into()),
            ResultValue::Float(1.13.into()),
        ],
        ColType::Int => vec![
            ResultValue::Int(1),
            ResultValue::Int(2),
            ResultValue::Int(3),
        ],
        ColType::Timestamp => vec![
            ResultValue::Timestamp(1296705660000),
            ResultValue::Timestamp(1296705720000),
            ResultValue::Timestamp(1296705780000),
        ],
        ColType::Uuid => vec![
            ResultValue::Uuid(
                uuid::Uuid::parse_str("84196262-53de-11ec-bf63-0242ac130001").unwrap(),
            ),
            ResultValue::Uuid(
                uuid::Uuid::parse_str("84196262-53de-11ec-bf63-0242ac130002").unwrap(),
            ),
            ResultValue::Uuid(
                uuid::Uuid::parse_str("84196262-53de-11ec-bf63-0242ac130003").unwrap(),
            ),
        ],
        ColType::Varchar => vec![
            ResultValue::Varchar("varchar 1".into()),
            ResultValue::Varchar("varchar 2".into()),
            ResultValue::Varchar("varchar 3".into()),
        ],
        ColType::Varint => vec![
            ResultValue::VarInt(vec![3, 5, 233]),
            ResultValue::VarInt(vec![3, 5, 234]),
            ResultValue::VarInt(vec![3, 5, 235]),
        ],
        ColType::Timeuuid => vec![
            ResultValue::TimeUuid(
                uuid::Uuid::parse_str("84196262-53de-11ec-bf63-0242ac130001").unwrap(),
            ),
            ResultValue::TimeUuid(
                uuid::Uuid::parse_str("84196262-53de-11ec-bf63-0242ac130002").unwrap(),
            ),
            ResultValue::TimeUuid(
                uuid::Uuid::parse_str("84196262-53de-11ec-bf63-0242ac130003").unwrap(),
            ),
        ],
        ColType::Inet => vec![
            ResultValue::Inet("127.0.0.1".parse().unwrap()),
            ResultValue::Inet("127.0.0.2".parse().unwrap()),
            ResultValue::Inet("127.0.0.3".parse().unwrap()),
        ],
        ColType::Date => vec![
            ResultValue::Date(2147498654),
            ResultValue::Date(2147498655),
            ResultValue::Date(2147498656),
        ],
        ColType::Time => vec![
            ResultValue::Time(29574000000000),
            ResultValue::Time(29575000000000),
            ResultValue::Time(29576000000000),
        ],
        ColType::Smallint => vec![
            ResultValue::SmallInt(32765),
            ResultValue::SmallInt(32766),
            ResultValue::SmallInt(32767),
        ],
        ColType::Tinyint => vec![
            ResultValue::TinyInt(121),
            ResultValue::TinyInt(122),
            ResultValue::TinyInt(123),
        ],
        _ => panic!("dont have an example for {}", col_type),
    }
}

fn column_list(cols: &[ColType]) -> String {
    cols.iter()
        .enumerate()
        .map(|(i, _)| format!("col_{i}"))
        .format(", ")
        .to_string()
}

async fn create_table(
    connection: &CassandraConnection,
    name: &str,
    collection_type: fn(&str) -> String,
) {
    let columns = NATIVE_COL_TYPES
        .into_iter()
        .enumerate()
        .map(|(i, col_type)| format!("col_{i} {}", collection_type(get_type_str(col_type))))
        .format(", ");
    run_query(
        connection,
        &format!("CREATE TABLE collections.{name} (id int PRIMARY KEY, {columns});"),
    )
    .await;
}

async fn insert_table(
    connection: &CassandraConnection,
    name: &str,
    collection_type: fn(Vec<&str>) -> String,
) {
    let columns = column_list(&NATIVE_COL_TYPES);
    let values = NATIVE_COL_TYPES
        .into_iter()
        .map(|col_type| collection_type(get_type_example(col_type)))
        .format(", ");
    run_query(
        connection,
        &format!("INSERT INTO collections.{name} (id, {columns}) VALUES (1, {values});"),
    )
    .await;
}

fn set_to_list(value: &mut ResultValue) {
    match value {
        ResultValue::List(values) => {
            for inner_value in values.iter_mut() {
                set_to_list(inner_value);
            }
        }
        ResultValue::Set(values) => {
            for inner_value in values.iter_mut() {
                set_to_list(inner_value);
            }
            *value = ResultValue::List(values.clone())
        }
        ResultValue::Map(key_values) => {
            for (key, value) in key_values.iter_mut() {
                set_to_list(key);
                set_to_list(value);
            }
        }
        _ => {}
    }
}

async fn select_table(
    connection: &CassandraConnection,
    name: &str,
    collection_type: fn(Vec<ResultValue>) -> ResultValue,
    driver: CassandraDriver,
) {
    let columns = column_list(&NATIVE_COL_TYPES);
    let mut results: Vec<_> = NATIVE_COL_TYPES
        .into_iter()
        .map(|x| collection_type(get_type_example_result_value(x)))
        .collect();

    // TODO: fix upstream to remove hack
    //       because cdrs-tokio doesnt support set properly, we need to map any sets into lists
    #[allow(irrefutable_let_patterns)]
    if let CassandraDriver::CdrsTokio = driver {
        for value in &mut results {
            set_to_list(value);
        }
    }
    assert_query_result(
        connection,
        &format!("SELECT {columns} FROM collections.{name};"),
        &[&results],
    )
    .await;
}

mod list {
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
            let sets: String = format!("{{{}}}", elements.join(","));

            let mut result = String::from("[");

            for _ in 0..2 {
                result.push_str(&format!("{},", sets));
            }

            result.push_str(&format!("{}]", sets));

            result
        })
        .await;
        insert_table(connection, "list_list", |elements| {
            let lists = format!("[{}]", elements.join(","));

            let mut result = String::from("[");
            for _ in 0..2 {
                result.push_str(&format!("{},", lists));
            }
            result.push_str(&format!("{}]", lists));

            result
        })
        .await;
        insert_table(connection, "list_map", |elements| {
            let mut maps = String::from("{");
            for (i, element) in elements.iter().enumerate().take(2) {
                maps.push_str(&format!("{}: {}, ", i, element));
            }
            maps.push_str(&format!("{}: {}}}", 2, elements[2]));

            let mut result = String::from("[");
            for _ in 0..2 {
                result.push_str(&format!("{}, ", maps));
            }
            result.push_str(&format!("{}]", maps));

            result
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
                ResultValue::List(vec![
                    ResultValue::Map(vec![
                        (ResultValue::Int(0), t[0].clone()),
                        (ResultValue::Int(1), t[1].clone()),
                        (ResultValue::Int(2), t[2].clone()),
                    ]),
                    ResultValue::Map(vec![
                        (ResultValue::Int(0), t[0].clone()),
                        (ResultValue::Int(1), t[1].clone()),
                        (ResultValue::Int(2), t[2].clone()),
                    ]),
                    ResultValue::Map(vec![
                        (ResultValue::Int(0), t[0].clone()),
                        (ResultValue::Int(1), t[1].clone()),
                        (ResultValue::Int(2), t[2].clone()),
                    ]),
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
}

mod set {
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
            let sets: String = format!("{{{}}}", elements.join(","));

            let mut result = String::from("{");

            for _ in 0..2 {
                result.push_str(&format!("{},", sets));
            }
            result.push_str(&format!("{}}}", sets));

            result
        })
        .await;
        insert_table(connection, "set_list", |elements| {
            let lists = format!("[{}]", elements.join(","));
            let mut result = String::from("{");

            for _ in 0..2 {
                result.push_str(&format!("{},", lists));
            }

            result.push_str(&format!("{}}}", lists));

            result
        })
        .await;
        insert_table(connection, "set_map", |elements| {
            let mut maps = String::from("{");
            for (i, element) in elements.iter().enumerate().take(2) {
                maps.push_str(&format!("{}: {}, ", i, element));
            }
            maps.push_str(&format!("{}: {}}}", 2, elements[2]));

            let mut result = String::from("{");

            for _ in 0..2 {
                result.push_str(&format!("{},", maps));
            }
            result.push_str(&format!("{}}}", maps));

            result
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
                ResultValue::Set(vec![ResultValue::Map(vec![
                    (ResultValue::Int(0), t[0].clone()),
                    (ResultValue::Int(1), t[1].clone()),
                    (ResultValue::Int(2), t[2].clone()),
                ])])
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
}

mod map {
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
            let mut map = String::from("{");

            for (i, element) in elements.iter().enumerate().take(2) {
                map.push_str(&format!("{}: {}, ", i, element));
            }
            map.push_str(&format!("{}: {}}}", 2, elements[2]));

            map
        })
        .await;
        insert_table(connection, "map_set", |elements| {
            let sets: String = format!("{{{}}}", elements.join(","));
            let mut map = String::from("{");

            for i in 0..2 {
                map.push_str(&format!("{}: {}, ", i, sets));
            }
            map.push_str(&format!("{}: {}}}", 2, sets));

            map
        })
        .await;
        insert_table(connection, "map_list", |elements| {
            let lists = format!("[{}]", elements.join(","));
            let mut map = String::from("{");

            for i in 0..2 {
                map.push_str(&format!("{}: {}, ", i, lists));
            }
            map.push_str(&format!("{}: {}}}", 2, lists));

            map
        })
        .await;
        insert_table(connection, "map_map", |elements| {
            let mut maps = String::from("{");
            for (i, element) in elements.iter().enumerate().take(2) {
                maps.push_str(&format!("{}: {}, ", i, element));
            }
            maps.push_str(&format!("{}: {}}}", 2, elements[2]));

            let mut map = String::from("{");

            for i in 0..2 {
                map.push_str(&format!("{}: {}, ", i, maps));
            }
            map.push_str(&format!("{}: {}}}", 2, maps));

            map
        })
        .await;
    }

    async fn select(connection: &CassandraConnection, driver: CassandraDriver) {
        select_table(
            connection,
            "map_native",
            |t| {
                ResultValue::Map(vec![
                    (ResultValue::Int(0), t[0].clone()),
                    (ResultValue::Int(1), t[1].clone()),
                    (ResultValue::Int(2), t[2].clone()),
                ])
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
                ResultValue::Map(vec![
                    (
                        ResultValue::Int(0),
                        ResultValue::Map(vec![
                            (ResultValue::Int(0), t[0].clone()),
                            (ResultValue::Int(1), t[1].clone()),
                            (ResultValue::Int(2), t[2].clone()),
                        ]),
                    ),
                    (
                        ResultValue::Int(1),
                        ResultValue::Map(vec![
                            (ResultValue::Int(0), t[0].clone()),
                            (ResultValue::Int(1), t[1].clone()),
                            (ResultValue::Int(2), t[2].clone()),
                        ]),
                    ),
                    (
                        ResultValue::Int(2),
                        ResultValue::Map(vec![
                            (ResultValue::Int(0), t[0].clone()),
                            (ResultValue::Int(1), t[1].clone()),
                            (ResultValue::Int(2), t[2].clone()),
                        ]),
                    ),
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
}

pub async fn test(connection: &CassandraConnection, driver: CassandraDriver) {
    run_query(
        connection,
        "CREATE KEYSPACE collections WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    ).await;

    list::test(connection, driver).await;
    set::test(connection, driver).await;
    map::test(connection, driver).await;
}
