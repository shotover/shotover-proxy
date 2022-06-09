use crate::helpers::cassandra::{assert_query_result, run_query, ResultValue};
use cassandra_cpp::Session;
use cassandra_protocol::frame::frame_result::ColType;

fn get_map_example(value: &str) -> String {
    format!("{{0 : {}}}", value)
}

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

const COLLECTION_COL_TYPES: [ColType; 2] = [ColType::Set, ColType::List];

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
        ColType::Null => "null",
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
        ColType::Inet => ResultValue::Inet("127.0.0.1".into()),
        ColType::Date => ResultValue::Date(vec![128, 0, 58, 160]),
        ColType::Time => ResultValue::Time(vec![0, 0, 26, 229, 187, 195, 188, 0]),
        ColType::Smallint => ResultValue::SmallInt(32767),
        ColType::Tinyint => ResultValue::TinyInt(127),
        _ => panic!("dont have an example for {}", col_type),
    }
}

mod list {
    use super::*;

    fn create(session: &Session) {
        // create lists of native types
        for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "CREATE TABLE test_collections_keyspace.test_list_table_{} (id int PRIMARY KEY, my_list list<{}>);",
                        i,
                        get_type_str(*col_type)
                    )
                    .as_str(),
                );
        }

        // create lists of lists and sets
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            for (j, collection_col_type) in COLLECTION_COL_TYPES.iter().enumerate() {
                run_query(
                    session,
                    format!(
                        "CREATE TABLE test_collections_keyspace.test_list_table_{}_{} (id int PRIMARY KEY, my_list frozen<list<{}<{}>>>);",
                        i,
                        j,
                        get_type_str(*collection_col_type),
                        get_type_str(*native_col_type)
                    )
                    .as_str(),
                );
            }
        }

        // create lists of maps
        for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "CREATE TABLE test_collections_keyspace.test_list_table_map_{} (id int PRIMARY KEY, my_list frozen<list<frozen<map<int, {}>>>>);",
                        i,
                        get_type_str(*col_type)
                    )
                    .as_str()
                );
        }
    }

    fn insert(session: &Session) {
        // insert lists of native types
        for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            let query = format!(
                    "INSERT INTO test_collections_keyspace.test_list_table_{} (id, my_list) VALUES ({}, [{}]);",
                    i,
                    i,
                    get_type_example(*col_type)
                );
            run_query(session, query.as_str());
        }

        // test inserting list of sets
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "INSERT INTO test_collections_keyspace.test_list_table_{}_0 (id, my_list) VALUES ({}, [{{{}}}]);",
                        i,
                        i,
                        get_type_example(*native_col_type)
                    )
                    .as_str(),
                );
        }

        // test inserting list of lists
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "INSERT INTO test_collections_keyspace.test_list_table_{}_1 (id, my_list) VALUES ({}, [[{}]]);",
                        i,
                        i,
                        get_type_example(*native_col_type)
                    )
                    .as_str(),
                );
        }
        // test inserting list of maps
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "INSERT INTO test_collections_keyspace.test_list_table_map_{} (id, my_list) VALUES ({}, [{}]);",
                        i,
                        i,
                        get_map_example(get_type_example(*native_col_type))
                    )
                    .as_str(),
                );
        }
    }

    fn select(session: &Session) {
        // select lists of native types
        for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            let query = format!(
                "SELECT my_list FROM test_collections_keyspace.test_list_table_{};",
                i
            );

            assert_query_result(
                session,
                query.as_str(),
                &[&[ResultValue::List(vec![get_type_example_result_value(
                    *col_type,
                )])]],
            );
        }

        // test selecting list of sets
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            assert_query_result(
                session,
                format!(
                    "SELECT my_list FROM test_collections_keyspace.test_list_table_{}_0;",
                    i
                )
                .as_str(),
                &[&[ResultValue::List(vec![ResultValue::Set(vec![
                    get_type_example_result_value(*native_col_type),
                ])])]],
            );
        }

        // test selecting list of lists
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            assert_query_result(
                session,
                format!(
                    "SELECT my_list FROM test_collections_keyspace.test_list_table_{}_1;",
                    i
                )
                .as_str(),
                &[&[ResultValue::List(vec![ResultValue::List(vec![
                    get_type_example_result_value(*native_col_type),
                ])])]],
            );
        }

        // test selecting list of maps
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            assert_query_result(
                session,
                format!(
                    "SELECT my_list FROM test_collections_keyspace.test_list_table_map_{};",
                    i,
                )
                .as_str(),
                &[&[ResultValue::List(vec![ResultValue::Map(vec![(
                    ResultValue::Int(0),
                    get_type_example_result_value(*native_col_type),
                )])])]],
            );
        }
    }

    pub fn test(session: &Session) {
        create(session);
        insert(session);
        select(session);
    }
}

mod set {
    use super::*;

    fn create(session: &Session) {
        // create sets of native types
        for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "CREATE TABLE test_collections_keyspace.test_set_table_{} (id int PRIMARY KEY, my_set set<{}>);",
                        i,
                        get_type_str(*col_type)
                    )
                    .as_str(),
                );
        }

        // create sets of lists and sets
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            for (j, collection_col_type) in COLLECTION_COL_TYPES.iter().enumerate() {
                run_query(
                        session,
                        format!(
                            "CREATE TABLE test_collections_keyspace.test_set_table_{}_{} (id int PRIMARY KEY, my_set frozen<set<{}<{}>>>);",
                            i,
                            j,
                            get_type_str(*collection_col_type),
                            get_type_str(*native_col_type)
                        )
                        .as_str(),
                );
            }
        }

        // create sets of maps
        for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(session, format!("CREATE TABLE test_collections_keyspace.test_set_table_map_{} (id int PRIMARY KEY, my_set frozen<set<frozen<map<int, {}>>>>);", i, get_type_str(*col_type)).as_str());
        }
    }

    fn insert(session: &Session) {
        // insert sets of native types
        for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            let query = format!(
                    "INSERT INTO test_collections_keyspace.test_set_table_{} (id, my_set) VALUES ({}, {{{}}});",
                    i,
                    i,
                    get_type_example(*col_type)
                );
            run_query(session, query.as_str());
        }

        // test inserting sets of sets
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "INSERT INTO test_collections_keyspace.test_set_table_{}_0 (id, my_set) VALUES ({}, {{{{{}}}}});",
                        i,
                        i,
                        get_type_example(*native_col_type)
                    )
                    .as_str(),
                );
        }

        // test inserting set of lists
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "INSERT INTO test_collections_keyspace.test_set_table_{}_1 (id, my_set) VALUES ({}, {{[{}]}});",
                        i,
                        i,
                        get_type_example(*native_col_type)
                    )
                    .as_str(),
                );
        }

        // test inserting set of maps
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "INSERT INTO test_collections_keyspace.test_set_table_map_{} (id, my_set) VALUES ({}, {{{}}});",
                        i,
                        i,
                        get_map_example(get_type_example(*native_col_type))
                    )
                    .as_str(),
                );
        }
    }

    fn select(session: &Session) {
        // select sets of native types
        for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            let query = format!(
                "SELECT my_set FROM test_collections_keyspace.test_set_table_{};",
                i
            );

            assert_query_result(
                session,
                query.as_str(),
                &[&[ResultValue::Set(vec![get_type_example_result_value(
                    *col_type,
                )])]],
            );
        }

        // test selecting set of sets
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            assert_query_result(
                session,
                format!(
                    "SELECT my_set FROM test_collections_keyspace.test_set_table_{}_0;",
                    i
                )
                .as_str(),
                &[&[ResultValue::Set(vec![ResultValue::Set(vec![
                    get_type_example_result_value(*native_col_type),
                ])])]],
            );
        }

        // test selecting set of lists
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            assert_query_result(
                session,
                format!(
                    "SELECT my_set FROM test_collections_keyspace.test_set_table_{}_1;",
                    i
                )
                .as_str(),
                &[&[ResultValue::Set(vec![ResultValue::List(vec![
                    get_type_example_result_value(*native_col_type),
                ])])]],
            );
        }

        // test selecting set of maps
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            assert_query_result(
                session,
                format!(
                    "SELECT my_set FROM test_collections_keyspace.test_set_table_map_{};",
                    i,
                )
                .as_str(),
                &[&[ResultValue::Set(vec![ResultValue::Map(vec![(
                    ResultValue::Int(0),
                    get_type_example_result_value(*native_col_type),
                )])])]],
            );
        }
    }

    pub fn test(session: &Session) {
        create(session);
        insert(session);
        select(session);
    }
}

mod map {
    use super::*;

    fn create(session: &Session) {
        // create maps of native types
        for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "CREATE TABLE test_collections_keyspace.test_map_table_{} (id int PRIMARY KEY, my_map map<int, {}>);",
                        i,
                        get_type_str(*col_type)
                    )
                    .as_str(),
                );
        }

        // create maps of lists and sets
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            for (j, collection_col_type) in COLLECTION_COL_TYPES.iter().enumerate() {
                run_query(
                        session,
                        format!(
                            "CREATE TABLE test_collections_keyspace.test_map_table_{}_{} (id int PRIMARY KEY, my_map frozen<map<int, {}<{}>>>);",
                            i,
                            j,
                            get_type_str(*collection_col_type),
                            get_type_str(*native_col_type)
                        )
                        .as_str(),
                    );
            }
        }

        // create maps of maps
        for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "CREATE TABLE test_collections_keyspace.test_map_table_map_{} (id int PRIMARY KEY, my_map frozen<map<int, frozen<map<int, {}>>>>);",
                        i,
                        get_type_str(*col_type)
                    )
                    .as_str()
                );
        }
    }

    fn insert(session: &Session) {
        // insert maps of native types
        for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            let query = format!(
                    "INSERT INTO test_collections_keyspace.test_map_table_{} (id, my_map) VALUES ({}, {});",
                    i,
                    i,
                    get_map_example(get_type_example(*col_type))
                );
            run_query(session, query.as_str());
        }

        // test inserting map of sets
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "INSERT INTO test_collections_keyspace.test_map_table_{}_0 (id, my_map) VALUES ({}, {});",
                        i,
                        i,
                        get_map_example(format!("{{{}}}", get_type_example(*native_col_type)).as_str())
                    )
                    .as_str()
                );
        }

        // test inserting map of lists
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "INSERT INTO test_collections_keyspace.test_map_table_{}_1 (id, my_map) VALUES ({}, {});",
                        i,
                        i,
                        get_map_example(format!("[{}]", get_type_example(*native_col_type)).as_str())
                    )
                    .as_str()
                );
        }

        // test inserting map of maps
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            run_query(
                    session,
                    format!(
                        "INSERT INTO test_collections_keyspace.test_map_table_map_{} (id, my_map) VALUES ({}, {{0: {}}});",
                        i,
                        i,
                        get_map_example(get_type_example(*native_col_type))
                    )
                    .as_str()
                );
        }
    }

    fn select(session: &Session) {
        // select sets of native types
        for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            let query = format!(
                "SELECT my_map FROM test_collections_keyspace.test_map_table_{};",
                i
            );

            assert_query_result(
                session,
                query.as_str(),
                &[&[ResultValue::Map(vec![(
                    ResultValue::Int(0),
                    get_type_example_result_value(*col_type),
                )])]],
            );
        }

        // test selecting map of sets
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            assert_query_result(
                session,
                format!(
                    "SELECT my_map FROM test_collections_keyspace.test_map_table_{}_0;",
                    i
                )
                .as_str(),
                &[&[ResultValue::Map(vec![(
                    ResultValue::Int(0),
                    ResultValue::Set(vec![get_type_example_result_value(*native_col_type)]),
                )])]],
            );
        }

        // test selecting map of lists
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            assert_query_result(
                session,
                format!(
                    "SELECT my_map FROM test_collections_keyspace.test_map_table_{}_1;",
                    i
                )
                .as_str(),
                &[&[ResultValue::Map(vec![(
                    ResultValue::Int(0),
                    ResultValue::List(vec![get_type_example_result_value(*native_col_type)]),
                )])]],
            );
        }

        // test selecting map of maps
        for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
            assert_query_result(
                session,
                format!(
                    "SELECT my_map FROM test_collections_keyspace.test_map_table_map_{};",
                    i,
                )
                .as_str(),
                &[&[ResultValue::Map(vec![(
                    ResultValue::Int(0),
                    ResultValue::Map(vec![(
                        ResultValue::Int(0),
                        get_type_example_result_value(*native_col_type),
                    )]),
                )])]],
            );
        }
    }

    pub fn test(session: &Session) {
        create(session);
        insert(session);
        select(session);
    }
}

pub fn test(session: &Session) {
    run_query(session, "CREATE KEYSPACE test_collections_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");

    list::test(session);
    set::test(session);
    map::test(session);
}
