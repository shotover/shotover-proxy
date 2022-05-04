use crate::helpers::cassandra::{assert_query_result, run_query, ResultValue};
use crate::helpers::ShotoverManager;
use cassandra_cpp::{stmt, Batch, BatchType, Error, ErrorKind, Session};
use futures::future::{join_all, try_join_all};
use metrics_util::debugging::DebuggingRecorder;
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;

mod keyspace {
    use crate::helpers::cassandra::{
        assert_query_result, assert_query_result_contains_row, run_query, ResultValue,
    };
    use cassandra_cpp::Session;

    fn test_create_keyspace(session: &Session) {
        run_query(session, "CREATE KEYSPACE keyspace_tests_create WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        assert_query_result(
            session,
            "SELECT bootstrapped FROM system.local",
            &[&[ResultValue::Varchar("COMPLETED".into())]],
        );
        assert_query_result_contains_row(
            session,
            "SELECT keyspace_name FROM system_schema.keyspaces;",
            &[ResultValue::Varchar("keyspace_tests_create".into())],
        );
    }

    fn test_use_keyspace(session: &Session) {
        run_query(session, "USE system");

        assert_query_result(
            session,
            "SELECT bootstrapped FROM local",
            &[&[ResultValue::Varchar("COMPLETED".into())]],
        );
    }

    fn test_drop_keyspace(session: &Session) {
        run_query(session, "CREATE KEYSPACE keyspace_tests_delete_me WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        assert_query_result(
            session,
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='keyspace_tests_delete_me';",
            &[&[ResultValue::Varchar("keyspace_tests_delete_me".into())]],
        );
        run_query(session, "DROP KEYSPACE keyspace_tests_delete_me");
        run_query(
            session,
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='keyspace_tests_delete_me';",
        );
    }

    fn test_alter_keyspace(session: &Session) {
        run_query(session, "CREATE KEYSPACE keyspace_tests_alter_me WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES = false;");
        run_query(
            session,
            "ALTER KEYSPACE keyspace_tests_alter_me WITH DURABLE_WRITES = true;",
        );
        assert_query_result(
            session,
            "SELECT durable_writes FROM system_schema.keyspaces WHERE keyspace_name='keyspace_tests_alter_me'",
            &[&[ResultValue::Boolean(true)]],
        );
    }

    pub fn test(session: &Session) {
        test_create_keyspace(session);
        test_use_keyspace(session);
        test_drop_keyspace(session);
        test_alter_keyspace(session);
    }
}

mod table {
    use crate::helpers::cassandra::{
        assert_query_result, assert_query_result_contains_row,
        assert_query_result_not_contains_row, run_query, ResultValue,
    };
    use cassandra_cpp::Session;

    fn test_create_table(session: &Session) {
        run_query(
            session,
            "CREATE TABLE test_table_keyspace.my_table (id UUID PRIMARY KEY, name text, age int);",
        );
        assert_query_result_contains_row(
            session,
            "SELECT table_name FROM system_schema.tables;",
            &[ResultValue::Varchar("my_table".into())],
        );
    }

    fn test_drop_table(session: &Session) {
        run_query(
            session,
            "CREATE TABLE test_table_keyspace.delete_me (id UUID PRIMARY KEY, name text, age int);",
        );

        assert_query_result_contains_row(
            session,
            "SELECT table_name FROM system_schema.tables;",
            &[ResultValue::Varchar("delete_me".into())],
        );
        run_query(session, "DROP TABLE test_table_keyspace.delete_me;");
        assert_query_result_not_contains_row(
            session,
            "SELECT table_name FROM system_schema.tables;",
            &[ResultValue::Varchar("delete_me".into())],
        );
    }

    fn test_alter_table(session: &Session) {
        run_query(
            session,
            "CREATE TABLE test_table_keyspace.alter_me (id UUID PRIMARY KEY, name text, age int);",
        );

        assert_query_result(session, "SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'test_table_keyspace' AND table_name = 'alter_me' AND column_name = 'age';", &[&[ResultValue::Varchar("age".into())]]);
        run_query(
            session,
            "ALTER TABLE test_table_keyspace.alter_me RENAME id TO new_id",
        );
        assert_query_result(session, "SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'test_table_keyspace' AND table_name = 'alter_me' AND column_name = 'new_id';", &[&[ResultValue::Varchar("new_id".into())]]);
    }

    pub fn test(session: &Session) {
        run_query(session, "CREATE KEYSPACE test_table_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        test_create_table(session);
        test_drop_table(session);
        test_alter_table(session);
    }
}

mod udt {
    use crate::helpers::cassandra::run_query;
    use cassandra_cpp::{stmt, Session};

    fn test_create_udt(session: &Session) {
        run_query(
            session,
            "CREATE TYPE test_udt_keyspace.test_type_name (foo text, bar int)",
        );
        run_query(
            session,
            "CREATE TABLE test_udt_keyspace.test_table (id int PRIMARY KEY, foo test_type_name);",
        );
        run_query(
            session,
            "INSERT INTO test_udt_keyspace.test_table (id, foo) VALUES (1, {foo: 'yes', bar: 1})",
        );
    }

    fn test_drop_udt(session: &Session) {
        run_query(
            session,
            "CREATE TYPE test_udt_keyspace.test_type_drop_me (foo text, bar int)",
        );
        run_query(session, "DROP TYPE test_udt_keyspace.test_type_drop_me;");
        let statement = stmt!(
            "CREATE TABLE test_udt_keyspace.test_delete_table (id int PRIMARY KEY, foo test_type_drop_me);"
        );
        let result = session.execute(&statement).wait().unwrap_err().to_string();
        assert_eq!(result, "Cassandra detailed error SERVER_INVALID_QUERY: Unknown type test_udt_keyspace.test_type_drop_me");
    }

    pub fn test(session: &Session) {
        run_query(session, "CREATE KEYSPACE test_udt_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        test_create_udt(session);
        test_drop_udt(session);
    }
}

mod native_types {
    use crate::helpers::cassandra::{assert_query_result, run_query, ResultValue};
    use cassandra_cpp::Session;

    fn select(session: &Session) {
        assert_query_result(
            session,
            "SELECT * from test_native_types_keyspace.native_types_table WHERE id=1",
            &[&[
                ResultValue::Int(1),
                ResultValue::Ascii("ascii string".into()),
                ResultValue::BigInt(1844674407370),
                ResultValue::Blob(vec![0, 0, 0, 0, 0, 0, 0, 10]),
                ResultValue::Boolean(true),
                ResultValue::Date(vec![128, 0, 58, 160]),
                ResultValue::Decimal(vec![0, 0, 0, 3, 4, 87]),
                ResultValue::Double(1.11.into()),
                ResultValue::Float(1.11.into()),
                ResultValue::Inet("127.0.0.1".into()),
                ResultValue::SmallInt(32767),
                ResultValue::Varchar("text".into()),
                ResultValue::Time(vec![0, 0, 26, 229, 187, 195, 188, 0]),
                ResultValue::Timestamp(1296705900000),
                ResultValue::TimeUuid(
                    uuid::Uuid::parse_str("84196262-53de-11ec-bf63-0242ac130002").unwrap(),
                ),
                ResultValue::TinyInt(127),
                ResultValue::Uuid(
                    uuid::Uuid::parse_str("84196262-53de-11ec-bf63-0242ac130002").unwrap(),
                ),
                ResultValue::Varchar("varchar".into()),
                ResultValue::VarInt(vec![3, 5, 233]),
            ]],
        )
    }

    fn insert(session: &Session) {
        for i in 0..10 {
            run_query(
                session,
                format!(
                    "INSERT INTO test_native_types_keyspace.native_types_table (
id,
uuid_test,
ascii_test,
bigint_test,
blob_test,
boolean_test,
date_test,
decimal_test,
double_test,
float_test,
inet_test,
smallint_test,
text_test,
time_test,
timestamp_test,
timeuuid_test,
tinyint_test,
varchar_test,
varint_test)
VALUES (
{},
84196262-53de-11ec-bf63-0242ac130002,
'ascii string',
1844674407370,
bigIntAsBlob(10),
true,
'2011-02-03',
1.111,
1.11,
1.11,
'127.0.0.1',
32767,
'text',
'08:12:54',
'2011-02-03 04:05+0000',
84196262-53de-11ec-bf63-0242ac130002,
127,
'varchar',
198121);",
                    i
                )
                .as_str(),
            );
        }
    }

    pub fn test(session: &Session) {
        run_query(session, "CREATE KEYSPACE test_native_types_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        run_query(
            session,
            "CREATE TABLE test_native_types_keyspace.native_types_table (
id int PRIMARY KEY,
uuid_test UUID,
ascii_test ascii,
bigint_test bigint,
blob_test blob,
boolean_test boolean,
decimal_test decimal,
double_test double,
float_test float,
inet_test inet,
smallint_test smallint,
text_test text,
time_test time,
timestamp_test timestamp,
timeuuid_test timeuuid,
tinyint_test tinyint,
varchar_test varchar,
varint_test varint,
date_test date,
);",
        );

        insert(session);
        select(session);
    }
}

mod collections {
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
}

mod functions {
    use crate::helpers::cassandra::{assert_query_result, run_query, ResultValue};
    use cassandra_cpp::{stmt, Session};

    fn drop_function(session: &Session) {
        assert_query_result(session, "SELECT test_function_keyspace.my_function(x, y) FROM test_function_keyspace.test_function_table WHERE id=1;", &[&[ResultValue::Int(4)]]);
        run_query(session, "DROP FUNCTION test_function_keyspace.my_function");

        let statement = stmt!("SELECT test_function_keyspace.my_function(x) FROM test_function_keyspace.test_function_table WHERE id=1;");
        let result = session.execute(&statement).wait().unwrap_err().to_string();

        assert_eq!(result, "Cassandra detailed error SERVER_INVALID_QUERY: Unknown function 'test_function_keyspace.my_function'");
    }

    fn create_function(session: &Session) {
        run_query(
            session,
            "CREATE FUNCTION test_function_keyspace.my_function (a int, b int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE javascript AS 'a * b';",
        );
        assert_query_result(session, "SELECT test_function_keyspace.my_function(x, y) FROM test_function_keyspace.test_function_table;",&[&[ResultValue::Int(4)], &[ResultValue::Int(9)], &[ResultValue::Int(16)]]);
    }

    pub fn test(session: &Session) {
        run_query(session, "CREATE KEYSPACE test_function_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        run_query(
            session,
            "CREATE TABLE test_function_keyspace.test_function_table (id int PRIMARY KEY, x int, y int);",
        );
        run_query(
            session,
            r#"BEGIN BATCH
INSERT INTO test_function_keyspace.test_function_table (id, x, y) VALUES (1, 2, 2);
INSERT INTO test_function_keyspace.test_function_table (id, x, y) VALUES (2, 3, 3);
INSERT INTO test_function_keyspace.test_function_table (id, x, y) VALUES (3, 4, 4);
APPLY BATCH;"#,
        );

        create_function(session);
        drop_function(session);
    }
}

mod prepared_statements {
    use crate::helpers::cassandra::{assert_query_result, assert_rows, run_query, ResultValue};
    use cassandra_cpp::Session;

    fn delete(session: &Session) {
        let prepared = session
            .prepare("DELETE FROM test_prepare_statements.table_1 WHERE id = ?;")
            .unwrap()
            .wait()
            .unwrap();

        let mut statement = prepared.bind();
        statement.bind_int32(0, 1).unwrap();
        session.execute(&statement).wait().unwrap();

        assert_query_result(
            session,
            "SELECT * FROM test_prepare_statements.table_1 where id = 1;",
            &[],
        );
    }

    fn insert(session: &Session) {
        let prepared = session
            .prepare("INSERT INTO test_prepare_statements.table_1 (id, x, name) VALUES (?, ?, ?);")
            .unwrap()
            .wait()
            .unwrap();

        let mut statement = prepared.bind();
        statement.bind_int32(0, 1).unwrap();
        statement.bind_int32(1, 11).unwrap();
        statement.bind_string(2, "foo").unwrap();
        session.execute(&statement).wait().unwrap();

        statement = prepared.bind();
        statement.bind_int32(0, 2).unwrap();
        statement.bind_int32(1, 12).unwrap();
        statement.bind_string(2, "bar").unwrap();
        session.execute(&statement).wait().unwrap();

        statement = prepared.bind();
        statement.bind_int32(0, 2).unwrap();
        statement.bind_int32(1, 13).unwrap();
        statement.bind_string(2, "baz").unwrap();
        session.execute(&statement).wait().unwrap();
    }

    fn select(session: &Session) {
        let prepared = session
            .prepare("SELECT id, x, name FROM test_prepare_statements.table_1 WHERE id = ?")
            .unwrap()
            .wait()
            .unwrap();

        let mut statement = prepared.bind();
        statement.bind_int32(0, 1).unwrap();

        let result_rows = session
            .execute(&statement)
            .wait()
            .unwrap()
            .into_iter()
            .map(|x| x.into_iter().map(ResultValue::new).collect())
            .collect();

        assert_rows(
            result_rows,
            &[&[
                ResultValue::Int(1),
                ResultValue::Int(11),
                ResultValue::Varchar("foo".into()),
            ]],
        );
    }

    pub fn test(session: &Session) {
        run_query(session, "CREATE KEYSPACE test_prepare_statements WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        run_query(session, "CREATE TABLE test_prepare_statements.table_1 (id int PRIMARY KEY, x int, name varchar);");

        insert(session);
        select(session);
        delete(session);
    }
}

mod cache {
    use crate::helpers::cassandra::{assert_query_result, run_query, ResultValue};
    use cassandra_cpp::Session;
    use metrics_util::debugging::{DebugValue, Snapshotter};
    use redis::Commands;
    use std::collections::HashSet;
    use tracing_log::log::info;

    pub fn test(
        cassandra_session: &Session,
        redis_connection: &mut redis::Connection,
        snapshotter: &Snapshotter,
    ) {
        test_batch_insert(cassandra_session, redis_connection, snapshotter);
        test_simple(cassandra_session, redis_connection, snapshotter);
    }

    /// gets the current miss count from the cache instrumentation.
    fn get_cache_miss_value(snapshotter: &Snapshotter) -> u64 {
        let mut result = 0_u64;
        for (x, _, _, v) in snapshotter.snapshot().into_vec().iter() {
            if let DebugValue::Counter(vv) = v {
                if x.key().name().eq("cache_miss") {
                    //return *vv;
                    info!("Cache value: {}", vv);
                    if *vv > result {
                        result = *vv;
                    }
                }
            }
        }
        result
    }

    /// The first time a query hits the cache it should not be found, the second time it should.
    /// This function verifies that case by utilizing the cache miss instrumentation.
    fn double_query(
        snapshotter: &Snapshotter,
        session: &Session,
        query: &str,
        expected_rows: &[&[ResultValue]],
    ) {
        let before = get_cache_miss_value(snapshotter);
        // first query should miss the cache
        assert_query_result(session, query, expected_rows);
        let after = get_cache_miss_value(snapshotter);
        assert_eq!(
            before + 1,
            get_cache_miss_value(snapshotter),
            "first {}",
            query
        );

        let before = after;
        assert_query_result(session, query, expected_rows);
        let after = get_cache_miss_value(snapshotter);
        assert_eq!(before, after, "second {}", query);
    }

    fn test_batch_insert(
        cassandra_session: &Session,
        redis_connection: &mut redis::Connection,
        snapshotter: &Snapshotter,
    ) {
        redis::cmd("FLUSHDB").execute(redis_connection);

        run_query(cassandra_session, "CREATE KEYSPACE test_cache_keyspace_batch_insert WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        run_query(
            cassandra_session,
            "CREATE TABLE test_cache_keyspace_batch_insert.test_table (id int PRIMARY KEY, x int, name varchar);",
        );
        run_query(
            cassandra_session,
            r#"BEGIN BATCH
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (1, 11, 'foo');
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (2, 12, 'bar');
                INSERT INTO test_cache_keyspace_batch_insert.test_table (id, x, name) VALUES (3, 13, 'baz');
            APPLY BATCH;"#,
        );

        // selects without where clauses do not hit the cache
        let before = get_cache_miss_value(snapshotter);
        assert_query_result(
            cassandra_session,
            "SELECT id, x, name FROM test_cache_keyspace_batch_insert.test_table",
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
        );
        assert_eq!(before, get_cache_miss_value(snapshotter));

        // query against the primary key
        double_query(
            snapshotter,
            cassandra_session,
            "SELECT id, x, name FROM test_cache_keyspace_batch_insert.test_table WHERE id=1",
            &[&[
                ResultValue::Int(1),
                ResultValue::Int(11),
                ResultValue::Varchar("foo".into()),
            ]],
        );

        let before = get_cache_miss_value(snapshotter);
        // queries without key are not cached
        assert_query_result(
            cassandra_session,
            "SELECT id, x, name FROM test_cache_keyspace_batch_insert.test_table WHERE x=11 ALLOW FILTERING",
            &[&[
                ResultValue::Int(1),
                ResultValue::Int(11),
                ResultValue::Varchar("foo".into()),
            ],],
        );
        assert_eq!(before, get_cache_miss_value(snapshotter));

        // Insert a dummy key to ensure the keys command is working correctly, we can remove this later.
        redis_connection
            .set::<&str, i32, ()>("dummy_key", 1)
            .unwrap();
        let mut result: Vec<String> = redis_connection.keys("*").unwrap();
        result.sort();
        assert_eq!(
            result,
            [
                "dummy_key".to_string(),
                "test_cache_keyspace_batch_insert.test_table:1".to_string(),
            ]
        );
    }

    fn test_simple(
        cassandra_session: &Session,
        redis_connection: &mut redis::Connection,
        snapshotter: &Snapshotter,
    ) {
        redis::cmd("FLUSHDB").execute(redis_connection);

        run_query(cassandra_session, "CREATE KEYSPACE test_cache_keyspace_simple WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        run_query(
            cassandra_session,
            "CREATE TABLE test_cache_keyspace_simple.test_table (id int PRIMARY KEY, x int, name varchar);",
        );

        run_query(
            cassandra_session,
            "INSERT INTO test_cache_keyspace_simple.test_table (id, x, name) VALUES (1, 11, 'foo');",
        );
        run_query(
            cassandra_session,
            "INSERT INTO test_cache_keyspace_simple.test_table (id, x, name) VALUES (2, 12, 'bar');",
        );
        run_query(
            cassandra_session,
            "INSERT INTO test_cache_keyspace_simple.test_table (id, x, name) VALUES (3, 13, 'baz');",
        );

        // selects without where clauses do not hit the cache
        let before = get_cache_miss_value(snapshotter);
        assert_query_result(
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
        );
        assert_eq!(before, get_cache_miss_value(snapshotter));

        // query against the primary key
        double_query(
            snapshotter,
            cassandra_session,
            "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE id=1",
            &[&[
                ResultValue::Int(1),
                ResultValue::Int(11),
                ResultValue::Varchar("foo".into()),
            ]],
        );

        // ensure key 2 and 3 are also loaded
        double_query(
            snapshotter,
            cassandra_session,
            "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE id=2",
            &[&[
                ResultValue::Int(2),
                ResultValue::Int(12),
                ResultValue::Varchar("bar".into()),
            ]],
        );

        double_query(
            snapshotter,
            cassandra_session,
            "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE id=3",
            &[&[
                ResultValue::Int(3),
                ResultValue::Int(13),
                ResultValue::Varchar("baz".into()),
            ]],
        );

        // query without primary key does not hit the cache
        let before = get_cache_miss_value(snapshotter);
        assert_query_result(
            cassandra_session,
            "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE x=11 ALLOW FILTERING",
            &[
                &[
                    ResultValue::Int(1),
                    ResultValue::Int(11),
                    ResultValue::Varchar("foo".into()),
                ],
            ],
        );
        assert_eq!(before, get_cache_miss_value(snapshotter));

        let result: HashSet<String> = redis_connection.keys("*").unwrap();
        let expected: HashSet<String> = [
            "test_cache_keyspace_simple.test_table:1",
            "test_cache_keyspace_simple.test_table:2",
            "test_cache_keyspace_simple.test_table:3",
        ]
        .into_iter()
        .map(|x| x.to_string())
        .collect();
        assert_eq!(result, expected);

        assert_sorted_set_equals(
            redis_connection,
            "test_cache_keyspace_simple.test_table:1",
            &["id, x, name WHERE "],
        );
        assert_sorted_set_equals(
            redis_connection,
            "test_cache_keyspace_simple.test_table:2",
            &["id, x, name WHERE "],
        );
        assert_sorted_set_equals(
            redis_connection,
            "test_cache_keyspace_simple.test_table:3",
            &["id, x, name WHERE "],
        );
    }

    fn assert_sorted_set_equals(
        redis_connection: &mut redis::Connection,
        key: &str,
        expected_values: &[&str],
    ) {
        let expected_values: HashSet<String> =
            expected_values.iter().map(|x| x.to_string()).collect();
        let values: HashSet<String> = redis_connection.hkeys(key).unwrap();
        assert_eq!(values, expected_values)
    }
}

#[cfg(feature = "alpha-transforms")]
mod protect {
    use crate::helpers::cassandra::{execute_query, run_query, ResultValue};
    use cassandra_cpp::Session;
    use regex::Regex;

    pub fn test(shotover_session: &Session, direct_session: &Session) {
        run_query(shotover_session, "CREATE KEYSPACE test_protect_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        run_query(
            shotover_session,
            "CREATE TABLE test_protect_keyspace.test_table (pk varchar PRIMARY KEY, cluster varchar, col1 varchar, col2 int, col3 boolean);",
        );

        run_query(
            shotover_session,
            "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 42, true);"
        );

        // assert that data is decrypted by shotover
        // assert_query_result(
        //     shotover_session,
        //     "SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table",
        //     &[&[
        //         ResultValue::Varchar("pk1".into()),
        //         ResultValue::Varchar("cluster".into()),
        //         ResultValue::Varchar("I am gonna get encrypted!!".into()),
        //         ResultValue::Int(42),
        //         ResultValue::Boolean(true),
        //     ]],
        // );
        // TODO: this should fail, protect currently manages to write the encrypted value but fails to decrypt it.
        let result = execute_query(
            shotover_session,
            "SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table",
        );
        if let ResultValue::Varchar(value) = &result[0][2] {
            assert_eq!("I am gonna get encrypted!!", value);
            //assert!(value.starts_with("{\"Ciphertext"));
        } else {
            panic!("expected 3rd column to be ResultValue::Varchar in {result:?}");
        }

        // assert that data is encrypted on cassandra side
        let result = execute_query(
            direct_session,
            "SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table",
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 5);
        assert_eq!(result[0][0], ResultValue::Varchar("pk1".into()));
        assert_eq!(result[0][1], ResultValue::Varchar("cluster".into()));
        if let ResultValue::Varchar(value) = &result[0][2] {
            let re = Regex::new(r"^[0-9a-fA-F]+$").unwrap();
            assert!(re.is_match(value));
        } else {
            panic!("expected 3rd column to be ResultValue::Varchar in {result:?}");
        }
        assert_eq!(result[0][3], ResultValue::Int(42));
        assert_eq!(result[0][4], ResultValue::Boolean(true));
    }
}

fn test_batch_statements(connection: &Session) {
    // setup keyspace and table for the batch statement tests
    {
        run_query(connection, "CREATE KEYSPACE batch_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        run_query(connection, "CREATE TABLE batch_keyspace.batch_table (id int PRIMARY KEY, lastname text, firstname text);");
    }

    {
        let mut batch = Batch::new(BatchType::LOGGED);
        for i in 0..2 {
            let statement = format!("INSERT INTO batch_keyspace.batch_table (id, lastname, firstname) VALUES ({}, 'text1', 'text2')", i);
            batch.add_statement(&stmt!(statement.as_str())).unwrap();
        }
        connection.execute_batch(&batch).wait().unwrap();

        assert_query_result(
            connection,
            "SELECT id, lastname, firstname FROM batch_keyspace.batch_table;",
            &[
                &[
                    ResultValue::Int(0),
                    ResultValue::Varchar("text1".into()),
                    ResultValue::Varchar("text2".into()),
                ],
                &[
                    ResultValue::Int(1),
                    ResultValue::Varchar("text1".into()),
                    ResultValue::Varchar("text2".into()),
                ],
            ],
        );
    }

    {
        let mut batch = Batch::new(BatchType::LOGGED);
        for i in 0..2 {
            let statement = format!(
                "UPDATE batch_keyspace.batch_table SET lastname = 'text3' WHERE id = {};",
                i
            );
            batch.add_statement(&stmt!(statement.as_str())).unwrap();
        }
        connection.execute_batch(&batch).wait().unwrap();

        assert_query_result(
            connection,
            "SELECT id, lastname, firstname FROM batch_keyspace.batch_table;",
            &[
                &[
                    ResultValue::Int(0),
                    ResultValue::Varchar("text3".into()),
                    ResultValue::Varchar("text2".into()),
                ],
                &[
                    ResultValue::Int(1),
                    ResultValue::Varchar("text3".into()),
                    ResultValue::Varchar("text2".into()),
                ],
            ],
        );
    }

    {
        let mut batch = Batch::new(BatchType::LOGGED);
        for i in 0..2 {
            let statement = format!("DELETE FROM batch_keyspace.batch_table WHERE id = {};", i);
            batch.add_statement(&stmt!(statement.as_str())).unwrap();
        }
        connection.execute_batch(&batch).wait().unwrap();
        assert_query_result(connection, "SELECT * FROM batch_keyspace.batch_table;", &[]);
    }

    {
        let batch = Batch::new(BatchType::LOGGED);
        connection.execute_batch(&batch).wait().unwrap();
    }

    // test batch statements over QUERY PROTOCOL
    {
        let insert_statement = stmt!(
            "BEGIN BATCH
INSERT INTO batch_keyspace.batch_table (id, lastname, firstname) VALUES (2, 'text1', 'text2');
INSERT INTO batch_keyspace.batch_table (id, lastname, firstname) VALUES (3, 'text1', 'text2');
APPLY BATCH;"
        );

        connection.execute(&insert_statement).wait().unwrap();

        assert_query_result(
            connection,
            "SELECT id, lastname, firstname FROM batch_keyspace.batch_table;",
            &[
                &[
                    ResultValue::Int(2),
                    ResultValue::Varchar("text1".into()),
                    ResultValue::Varchar("text2".into()),
                ],
                &[
                    ResultValue::Int(3),
                    ResultValue::Varchar("text1".into()),
                    ResultValue::Varchar("text2".into()),
                ],
            ],
        );

        let select_statement = stmt!("BEGIN BATCH UPDATE batch_keyspace.batch_table SET lastname = 'text3' WHERE id = 2; UPDATE batch_keyspace.batch_table SET lastname = 'text3' WHERE id = 3; APPLY BATCH;");

        connection.execute(&select_statement).wait().unwrap();

        assert_query_result(
            connection,
            "SELECT id, lastname, firstname FROM batch_keyspace.batch_table;",
            &[
                &[
                    ResultValue::Int(2),
                    ResultValue::Varchar("text3".into()),
                    ResultValue::Varchar("text2".into()),
                ],
                &[
                    ResultValue::Int(3),
                    ResultValue::Varchar("text3".into()),
                    ResultValue::Varchar("text2".into()),
                ],
            ],
        );
    }
}

#[test]
#[serial]
fn test_passthrough() {
    let _compose = DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yml");

    let shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-passthrough/topology.yaml");

    let connection = shotover_manager.cassandra_connection("127.0.0.1", 9042);

    keyspace::test(&connection);
    table::test(&connection);
    udt::test(&connection);
    native_types::test(&connection);
    collections::test(&connection);
    functions::test(&connection);
    prepared_statements::test(&connection);
    test_batch_statements(&connection);
}

#[test]
#[serial]
fn test_source_tls_and_single_tls() {
    let _compose = DockerCompose::new("example-configs/cassandra-tls/docker-compose.yml");

    let shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-tls/topology.yaml");

    let ca_cert = "example-configs/cassandra-tls/certs/localhost_CA.crt";

    {
        // Run a quick test straight to Cassandra to check our assumptions that Shotover and Cassandra TLS are behaving exactly the same
        let direct_connection =
            shotover_manager.cassandra_connection_tls("127.0.0.1", 9042, ca_cert);
        assert_query_result(
            &direct_connection,
            "SELECT bootstrapped FROM system.local",
            &[&[ResultValue::Varchar("COMPLETED".into())]],
        );
    }

    let connection = shotover_manager.cassandra_connection_tls("127.0.0.1", 9043, ca_cert);

    keyspace::test(&connection);
    table::test(&connection);
    udt::test(&connection);
    native_types::test(&connection);
    collections::test(&connection);
    functions::test(&connection);
    prepared_statements::test(&connection);
    test_batch_statements(&connection);
}

#[test]
#[serial]
fn test_cassandra_redis_cache() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();
    let _compose = DockerCompose::new("example-configs/cassandra-redis-cache/docker-compose.yml");

    let shotover_manager = ShotoverManager::from_topology_file_without_observability(
        "example-configs/cassandra-redis-cache/topology.yaml",
    );

    let mut redis_connection = shotover_manager.redis_connection(6379);
    let connection = shotover_manager.cassandra_connection("127.0.0.1", 9042);

    keyspace::test(&connection);
    table::test(&connection);
    udt::test(&connection);
    functions::test(&connection);
    cache::test(&connection, &mut redis_connection, &snapshotter);
    prepared_statements::test(&connection);
    test_batch_statements(&connection);
}

#[test]
#[serial]
#[cfg(feature = "alpha-transforms")]
fn test_cassandra_protect_transform_local() {
    let _compose = DockerCompose::new("example-configs/cassandra-protect-local/docker-compose.yml");

    let shotover_manager = ShotoverManager::from_topology_file(
        "example-configs/cassandra-protect-local/topology.yaml",
    );

    let shotover_connection = shotover_manager.cassandra_connection("127.0.0.1", 9042);
    let direct_connection = shotover_manager.cassandra_connection("127.0.0.1", 9043);

    keyspace::test(&shotover_connection);
    table::test(&shotover_connection);
    udt::test(&shotover_connection);
    native_types::test(&shotover_connection);
    collections::test(&shotover_connection);
    functions::test(&shotover_connection);
    protect::test(&shotover_connection, &direct_connection);
    test_batch_statements(&shotover_connection);
}

#[test]
#[serial]
#[cfg(feature = "alpha-transforms")]
fn test_cassandra_protect_transform_aws() {
    let _compose = DockerCompose::new("example-configs/cassandra-protect-aws/docker-compose.yml");
    let _compose_aws = DockerCompose::new_moto();

    let shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-protect-aws/topology.yaml");

    let shotover_connection = shotover_manager.cassandra_connection("127.0.0.1", 9042);
    let direct_connection = shotover_manager.cassandra_connection("127.0.0.1", 9043);

    keyspace::test(&shotover_connection);
    table::test(&shotover_connection);
    udt::test(&shotover_connection);
    native_types::test(&shotover_connection);
    collections::test(&shotover_connection);
    functions::test(&shotover_connection);
    protect::test(&shotover_connection, &direct_connection);
    test_batch_statements(&shotover_connection);
}

#[test]
#[serial]
fn test_cassandra_peers_rewrite() {
    let _docker_compose =
        DockerCompose::new("tests/test-configs/cassandra-peers-rewrite/docker-compose.yml");

    let shotover_manager = ShotoverManager::from_topology_file(
        "tests/test-configs/cassandra-peers-rewrite/topology.yaml",
    );

    let normal_connection = shotover_manager.cassandra_connection("127.0.0.1", 9043);

    let rewrite_port_connection = shotover_manager.cassandra_connection("127.0.0.1", 9044);
    table::test(&rewrite_port_connection); // run some basic tests to confirm it works as normal

    {
        assert_query_result(
            &normal_connection,
            "SELECT data_center, native_port, rack FROM system.peers_v2;",
            &[&[
                ResultValue::Varchar("dc1".into()),
                ResultValue::Int(9042),
                ResultValue::Varchar("West".into()),
            ]],
        );
        assert_query_result(
            &normal_connection,
            "SELECT native_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9042)]],
        );
    }

    {
        assert_query_result(
            &rewrite_port_connection,
            "SELECT data_center, native_port, rack FROM system.peers_v2;",
            &[&[
                ResultValue::Varchar("dc1".into()),
                ResultValue::Int(9044),
                ResultValue::Varchar("West".into()),
            ]],
        );

        assert_query_result(
            &rewrite_port_connection,
            "SELECT native_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9044)]],
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cassandra_request_throttling() {
    let _docker_compose =
        DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yml");

    let shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/cassandra-request-throttling.yaml");

    let connection = shotover_manager.cassandra_connection("127.0.0.1", 9042);
    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window and not trigger the rate limiter with client's startup reqeusts
    let connection_2 = shotover_manager.cassandra_connection("127.0.0.1", 9042);
    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window again

    let statement = stmt!("SELECT * FROM system.peers");

    // these should all be let through the request throttling
    {
        let mut futures = vec![];
        for _ in 0..25 {
            futures.push(connection.execute(&statement));
            futures.push(connection_2.execute(&statement));
        }
        try_join_all(futures).await.unwrap();
    }

    // sleep to reset the window
    std::thread::sleep(std::time::Duration::from_secs(1));

    // only around half of these should be let through the request throttling
    {
        let mut futures = vec![];
        for _ in 0..50 {
            futures.push(connection.execute(&statement));
            futures.push(connection_2.execute(&statement));
        }
        let mut results = join_all(futures).await;
        results.retain(|result| match result {
            Ok(_) => true,
            Err(Error(
                ErrorKind::CassErrorResult(cassandra_cpp::CassErrorCode::SERVER_OVERLOADED, ..),
                _,
            )) => false,
            Err(e) => panic!(
                "wrong error returned, got {:?}, expected SERVER_OVERLOADED",
                e
            ),
        });

        let len = results.len();
        assert!(50 < len && len <= 60, "got {len}");
    }

    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window

    // setup keyspace and table for the batch statement tests
    {
        run_query(&connection, "CREATE KEYSPACE test_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        run_query(&connection, "CREATE TABLE test_keyspace.my_table (id int PRIMARY KEY, lastname text, firstname text);");
    }

    // this batch set should be allowed through
    {
        let mut batch = Batch::new(BatchType::LOGGED);
        for i in 0..25 {
            let statement = format!("INSERT INTO test_keyspace.my_table (id, lastname, firstname) VALUES ({}, 'text', 'text')", i);
            batch.add_statement(&stmt!(statement.as_str())).unwrap();
        }
        connection.execute_batch(&batch).wait().unwrap();
    }

    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window

    // this batch set should not be allowed through
    {
        let mut batch = Batch::new(BatchType::LOGGED);
        for i in 0..60 {
            let statement = format!("INSERT INTO test_keyspace.my_table (id, lastname, firstname) VALUES ({}, 'text', 'text')", i);
            batch.add_statement(&stmt!(statement.as_str())).unwrap();
        }
        let result = connection.execute_batch(&batch).wait().unwrap_err();
        assert!(matches!(
            result,
            Error(
                ErrorKind::CassErrorResult(cassandra_cpp::CassErrorCode::SERVER_OVERLOADED, ..),
                ..
            )
        ));
    }

    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window

    test_batch_statements(&connection);
}
