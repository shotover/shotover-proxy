use cassandra_protocol::frame::message_result::ColType;
use itertools::Itertools;
use test_helpers::connection::cassandra::{
    assert_query_result, run_query, CassandraConnection, CassandraDriver, ResultValue,
};

const NATIVE_COL_TYPES: [ColType; 18] = [
    ColType::Ascii,
    ColType::Bigint,
    ColType::Blob,
    ColType::Boolean,
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
        ColType::Boolean => vec!["false", "true"],
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
        ColType::Boolean => vec![ResultValue::Boolean(false), ResultValue::Boolean(true)],
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
}

pub mod vector {
    use super::*;

    async fn create(connection: &CassandraConnection) {
        let cql = "CREATE TABLE IF NOT EXISTS collections.vectors (
  vector VECTOR <FLOAT, 5>,
  id int,
  created_at timestamp,
  PRIMARY KEY (id, created_at)
)
WITH CLUSTERING ORDER BY (created_at DESC);";
        run_query(connection, cql).await;

        let create_index_cql = "CREATE INDEX IF NOT EXISTS ann_index
  ON collections.vectors(vector) USING 'sai';";
        run_query(connection, create_index_cql).await;
    }

    async fn insert(connection: &CassandraConnection) {
        let insert_vector_cql = [
            "INSERT INTO collections.vectors (id, vector, created_at) VALUES (
                1,
                [0.45, 0.09, 0.01, 0.2, 0.11],
                '2017-02-14 12:43:20-0800'
            );",
            "INSERT INTO collections.vectors (id, vector, created_at) VALUES (
                2,
                [0.99, 0.5, 0.99, 0.1, 0.34],
                '2018-02-14 12:43:20-0800'
            );",
            "INSERT INTO collections.vectors (id, vector, created_at) VALUES (
                3,
                [0.9, 0.54, 0.12, 0.1, 0.95],
                '2019-02-14 12:43:20-0800'
            );",
            "INSERT INTO collections.vectors (id, vector, created_at) VALUES (
                4,
                [0.13, 0.8, 0.35, 0.17, 0.03],
                '2020-02-14 12:43:20-0800'
            );",
            "INSERT INTO collections.vectors (id, vector, created_at) VALUES (
                5,
                [0.3, 0.34, 0.2, 0.78, 0.25],
                '2021-02-14 12:43:20-0800'
            );",
            "INSERT INTO collections.vectors (id, vector, created_at) VALUES (
                6,
                [0.1, 0.4, 0.1, 0.52, 0.09],
                '2022-02-14 12:43:20-0800'
            );",
            "INSERT INTO collections.vectors (id, vector, created_at) VALUES (
                7,
                [0.3, 0.75, 0.2, 0.2, 0.5],
                '2023-02-14 12:43:20-0800'
            );",
        ];
        for row in insert_vector_cql {
            run_query(connection, row).await;
        }
    }

    async fn select(connection: &CassandraConnection) {
        let select_cql = "SELECT * FROM collections.vectors
    ORDER BY vector ANN OF [0.15, 0.1, 0.1, 0.35, 0.55]
    LIMIT 3;";

        assert_query_result(
            connection,
            select_cql,
            &[
                &[
                    ResultValue::Int(3),
                    ResultValue::Timestamp(1550177000000),
                    ResultValue::Vector(vec![
                        ResultValue::Float(0.9.into()),
                        ResultValue::Float(0.54.into()),
                        ResultValue::Float(0.12.into()),
                        ResultValue::Float(0.1.into()),
                        ResultValue::Float(0.95.into()),
                    ]),
                ],
                &[
                    ResultValue::Int(5),
                    ResultValue::Timestamp(1613335400000),
                    ResultValue::Vector(vec![
                        ResultValue::Float(0.3.into()),
                        ResultValue::Float(0.34.into()),
                        ResultValue::Float(0.2.into()),
                        ResultValue::Float(0.78.into()),
                        ResultValue::Float(0.25.into()),
                    ]),
                ],
                &[
                    ResultValue::Int(7),
                    ResultValue::Timestamp(1676407400000),
                    ResultValue::Vector(vec![
                        ResultValue::Float(0.3.into()),
                        ResultValue::Float(0.75.into()),
                        ResultValue::Float(0.2.into()),
                        ResultValue::Float(0.2.into()),
                        ResultValue::Float(0.5.into()),
                    ]),
                ],
            ],
        )
        .await;
    }

    pub async fn test(connection: &CassandraConnection) {
        create(connection).await;
        insert(connection).await;
        select(connection).await;
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
