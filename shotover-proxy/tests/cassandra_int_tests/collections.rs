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

fn get_type_example(col_type: ColType) -> &'static str {
    match col_type {
        ColType::Ascii => "'ascii string'",
        ColType::Bigint => "1844674407370",
        ColType::Blob => "bigIntAsBlob(10)",
        ColType::Boolean => "true",
        ColType::Counter => "12",
        ColType::Decimal => "1.111",
        ColType::Double => "1.11",
        ColType::Float => "1.11",
        ColType::Int => "1",
        ColType::Timestamp => "'2011-02-03 04:05+0000'",
        ColType::Uuid => "84196262-53de-11ec-bf63-0242ac130002",
        ColType::Varchar => "'varchar'",
        ColType::Varint => "198121",
        ColType::Timeuuid => "84196262-53de-11ec-bf63-0242ac130002",
        ColType::Inet => "'127.0.0.1'",
        ColType::Date => "'2011-02-03'",
        ColType::Time => "'08:12:54'",
        ColType::Smallint => "32767",
        ColType::Tinyint => "127",
        _ => panic!("dont have an example for {}", col_type),
    }
}

fn get_type_example_result_value(col_type: ColType) -> ResultValue {
    match col_type {
        ColType::Ascii => ResultValue::Ascii("ascii string".into()),
        ColType::Bigint => ResultValue::BigInt(1844674407370),
        ColType::Blob => ResultValue::Blob(vec![0, 0, 0, 0, 0, 0, 0, 10]),
        ColType::Boolean => ResultValue::Boolean(true),
        ColType::Counter => ResultValue::Counter(12),
        ColType::Decimal => ResultValue::Decimal(vec![0, 0, 0, 3, 4, 87]),
        ColType::Double => ResultValue::Double(1.11.into()),
        ColType::Float => ResultValue::Float(1.11.into()),
        ColType::Int => ResultValue::Int(1),
        ColType::Timestamp => ResultValue::Timestamp(1296705900000),
        ColType::Uuid => ResultValue::Uuid(
            uuid::Uuid::parse_str("84196262-53de-11ec-bf63-0242ac130002").unwrap(),
        ),

        ColType::Varchar => ResultValue::Varchar("varchar".into()),
        ColType::Varint => ResultValue::VarInt(vec![3, 5, 233]),
        ColType::Timeuuid => ResultValue::TimeUuid(
            uuid::Uuid::parse_str("84196262-53de-11ec-bf63-0242ac130002").unwrap(),
        ),
        ColType::Inet => ResultValue::Inet("127.0.0.1".parse().unwrap()),
        ColType::Date => ResultValue::Date(2147498656),
        ColType::Time => ResultValue::Time(29574000000000),
        ColType::Smallint => ResultValue::SmallInt(32767),
        ColType::Tinyint => ResultValue::TinyInt(127),
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
    collection_type: fn(&str) -> String,
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
    collection_type: fn(ResultValue) -> ResultValue,
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
        insert_table(connection, "list_native", |s| format!("[{s}]")).await;
        insert_table(connection, "list_set", |s| format!("[{{{s}}}]")).await;
        insert_table(connection, "list_list", |s| format!("[[{s}]]")).await;
        insert_table(connection, "list_map", |s| format!("[{{ 0: {s} }}]")).await;
    }

    async fn select(connection: &CassandraConnection, driver: CassandraDriver) {
        select_table(
            connection,
            "list_native",
            |t| ResultValue::List(vec![t]),
            driver,
        )
        .await;
        select_table(
            connection,
            "list_set",
            |t| ResultValue::List(vec![ResultValue::Set(vec![t])]),
            driver,
        )
        .await;
        select_table(
            connection,
            "list_list",
            |t| ResultValue::List(vec![ResultValue::List(vec![t])]),
            driver,
        )
        .await;
        select_table(
            connection,
            "list_map",
            |t| ResultValue::List(vec![ResultValue::Map(vec![(ResultValue::Int(0), t)])]),
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
        insert_table(connection, "set_native", |s| format!("{{{s}}}")).await;
        insert_table(connection, "set_set", |s| format!("{{ {{{s}}} }}")).await;
        insert_table(connection, "set_list", |s| format!("{{ [{s}] }}")).await;
        insert_table(connection, "set_map", |s| format!("{{ {{ 0: {s} }} }}")).await;
    }

    async fn select(connection: &CassandraConnection, driver: CassandraDriver) {
        select_table(
            connection,
            "set_native",
            |t| ResultValue::Set(vec![t]),
            driver,
        )
        .await;
        select_table(
            connection,
            "set_set",
            |t| ResultValue::Set(vec![ResultValue::Set(vec![t])]),
            driver,
        )
        .await;
        select_table(
            connection,
            "set_list",
            |t| ResultValue::Set(vec![ResultValue::List(vec![t])]),
            driver,
        )
        .await;
        select_table(
            connection,
            "set_map",
            |t| ResultValue::Set(vec![ResultValue::Map(vec![(ResultValue::Int(0), t)])]),
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
        insert_table(connection, "map_native", |s| format!("{{ 0: {s} }}")).await;
        insert_table(connection, "map_set", |s| format!("{{ 0: {{{s}}} }}")).await;
        insert_table(connection, "map_list", |s| format!("{{ 0: [{s}] }}")).await;
        insert_table(connection, "map_map", |s| format!("{{ 0: {{ 0: {s} }} }}")).await;
    }

    async fn select(connection: &CassandraConnection, driver: CassandraDriver) {
        select_table(
            connection,
            "map_native",
            |t| ResultValue::Map(vec![(ResultValue::Int(0), t)]),
            driver,
        )
        .await;
        select_table(
            connection,
            "map_set",
            |t| ResultValue::Map(vec![(ResultValue::Int(0), ResultValue::Set(vec![t]))]),
            driver,
        )
        .await;
        select_table(
            connection,
            "map_list",
            |t| ResultValue::Map(vec![(ResultValue::Int(0), ResultValue::List(vec![t]))]),
            driver,
        )
        .await;
        select_table(
            connection,
            "map_map",
            |t| {
                ResultValue::Map(vec![(
                    ResultValue::Int(0),
                    ResultValue::Map(vec![(ResultValue::Int(0), t)]),
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

pub async fn test(connection: &CassandraConnection, driver: CassandraDriver) {
    run_query(
        connection,
        "CREATE KEYSPACE collections WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    ).await;

    list::test(connection, driver).await;
    set::test(connection, driver).await;
    map::test(connection, driver).await;
}
