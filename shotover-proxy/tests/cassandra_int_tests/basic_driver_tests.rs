use crate::helpers::ShotoverManager;
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;

use crate::cassandra_int_tests::cassandra_connection;

mod keyspace {
    use cassandra_cpp::Session;

    use crate::cassandra_int_tests::{
        assert_query_result, assert_query_result_contains_row, run_query, ResultValue,
    };

    fn test_create_keyspace(session: &Session) {
        run_query(session, "CREATE KEYSPACE keyspace_tests_create WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        assert_query_result(
            session,
            "SELECT release_version FROM system.local",
            &[&[ResultValue::Varchar("3.11.10".into())]],
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
            "SELECT release_version FROM local",
            &[&[ResultValue::Varchar("3.11.10".into())]],
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
    use cassandra_cpp::Session;

    use crate::cassandra_int_tests::{
        assert_query_result, assert_query_result_contains_row,
        assert_query_result_not_contains_row, run_query, ResultValue,
    };

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
    use cassandra_cpp::{stmt, Session};

    use crate::cassandra_int_tests::run_query;

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
    use cassandra_cpp::Session;

    use crate::cassandra_int_tests::{assert_query_result, run_query, ResultValue};

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
    use cassandra_cpp::Session;
    use cassandra_protocol::frame::frame_result::ColType;

    use crate::cassandra_int_tests::{assert_query_result, run_query, ResultValue};

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
                run_query(session, format!("CREATE TABLE test_collections_keyspace.test_list_table_map_{} (id int PRIMARY KEY, my_list frozen<list<frozen<map<int, {}>>>>);", i, get_type_str(*col_type)).as_str());
            }
        }

        fn insert(session: &Session) {
            // insert lists of native types
            for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
                let query = format!("INSERT INTO test_collections_keyspace.test_list_table_{} (id, my_list) VALUES ({}, [{}]);", i, i, get_type_example(*col_type));
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

            fn get_map_example(value: &str) -> String {
                format!("{{0 : {}}}", value)
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
            // insert lists of native types
            for (i, col_type) in NATIVE_COL_TYPES.iter().enumerate() {
                let query = format!("INSERT INTO test_collections_keyspace.test_set_table_{} (id, my_set) VALUES ({}, {{{}}});", i, i, get_type_example(*col_type));
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

            fn get_map_example(_key: usize, value: &str) -> String {
                format!("{{0 : {}}}", value)
            }

            // test inserting set of maps
            for (i, native_col_type) in NATIVE_COL_TYPES.iter().enumerate() {
                run_query(
                    session,
                    format!(
                        "INSERT INTO test_collections_keyspace.test_set_table_map_{} (id, my_set) VALUES ({}, {{{}}});",
                        i,
                        i,
                        get_map_example(i, get_type_example(*native_col_type))
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

            // test selecting set  of maps
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

    pub fn test(session: &Session) {
        run_query(session, "CREATE KEYSPACE test_collections_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");

        list::test(session);
        set::test(session);
    }
}

mod functions {
    use cassandra_cpp::{stmt, Session};

    use crate::cassandra_int_tests::{assert_query_result, run_query, ResultValue};

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

mod cache {
    use cassandra_cpp::Session;
    use redis::Commands;
    use std::collections::HashSet;

    use crate::cassandra_int_tests::{assert_query_result, run_query, ResultValue};

    pub fn test(cassandra_session: &Session, redis_connection: &mut redis::Connection) {
        test_batch_insert(cassandra_session, redis_connection);
        test_simple(cassandra_session, redis_connection);
    }

    fn test_batch_insert(cassandra_session: &Session, redis_connection: &mut redis::Connection) {
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

        // TODO: SELECTS without a WHERE do not get cached
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

        // query against the primary key
        assert_query_result(
            cassandra_session,
            "SELECT id, x, name FROM test_cache_keyspace_batch_insert.test_table WHERE id=1",
            &[],
        );

        // query against some other field
        assert_query_result(
            cassandra_session,
            "SELECT id, x, name FROM test_cache_keyspace_batch_insert.test_table WHERE x=11",
            &[],
        );

        // Insert a dummy key to ensure the keys command is working correctly, we can remove this later.
        redis_connection
            .set::<&str, i32, ()>("dummy_key", 1)
            .unwrap();
        let result: Vec<String> = redis_connection.keys("*").unwrap();
        assert_eq!(result, ["dummy_key".to_string()]);
    }

    fn test_simple(cassandra_session: &Session, redis_connection: &mut redis::Connection) {
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

        // TODO: SELECTS without a WHERE do not get cached
        assert_query_result(
            cassandra_session,
            "SELECT id, x, name FROM test_cache_keyspace_simple.test_table",
            &[],
        );

        // query against the primary key
        assert_query_result(
            cassandra_session,
            "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE id=1",
            &[],
        );

        // query against some other field
        assert_query_result(
            cassandra_session,
            "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE x=11",
            &[],
        );

        let result: HashSet<String> = redis_connection.keys("*").unwrap();
        let expected: HashSet<String> =
            ["1", "2", "3"].into_iter().map(|x| x.to_string()).collect();
        assert_eq!(result, expected);

        assert_sorted_set_equals(redis_connection, "1", &["1:11", "1:foo"]);
        assert_sorted_set_equals(redis_connection, "2", &["2:12", "2:bar"]);
        assert_sorted_set_equals(redis_connection, "3", &["3:13", "3:baz"]);
    }

    fn assert_sorted_set_equals(
        redis_connection: &mut redis::Connection,
        key: &str,
        expected_values: &[&str],
    ) {
        let expected_values: HashSet<String> =
            expected_values.iter().map(|x| x.to_string()).collect();
        let values = redis_connection
            .zrange::<&str, HashSet<String>>(key, 0, -1)
            .unwrap();
        assert_eq!(values, expected_values)
    }
}

#[test]
#[serial]
fn test_cluster() {
    let _compose = DockerCompose::new("examples/cassandra-cluster/docker-compose.yml")
        .wait_for_n_t("Startup complete", 3, 90);

    let _handles: Vec<_> = [
        "examples/cassandra-cluster/topology1.yaml",
        "examples/cassandra-cluster/topology2.yaml",
        "examples/cassandra-cluster/topology3.yaml",
    ]
    .into_iter()
    .map(ShotoverManager::from_topology_file_without_observability)
    .collect();

    let connection = cassandra_connection("127.0.0.1", 9042);

    keyspace::test(&connection);
    table::test(&connection);
    udt::test(&connection);
    native_types::test(&connection);
    collections::test(&connection);
    functions::test(&connection);
}

#[test]
#[serial]
fn test_passthrough() {
    let _compose = DockerCompose::new("examples/cassandra-passthrough/docker-compose.yml")
        .wait_for_n_t("Startup complete", 1, 90);

    let _shotover_manager =
        ShotoverManager::from_topology_file("examples/cassandra-passthrough/topology.yaml");

    let connection = cassandra_connection("127.0.0.1", 9042);

    keyspace::test(&connection);
    table::test(&connection);
    udt::test(&connection);
    native_types::test(&connection);
    collections::test(&connection);
    functions::test(&connection);
}

#[test]
#[serial]
fn test_cassandra_redis_cache() {
    let _compose = DockerCompose::new("examples/cassandra-redis-cache/docker-compose.yml")
        .wait_for_n_t("Startup complete", 1, 90);

    let shotover_manager =
        ShotoverManager::from_topology_file("examples/cassandra-redis-cache/topology.yaml");

    let mut redis_connection = shotover_manager.redis_connection(6379);
    let connection = cassandra_connection("127.0.0.1", 9042);

    keyspace::test(&connection);
    table::test(&connection);
    udt::test(&connection);
    functions::test(&connection);
    cache::test(&connection, &mut redis_connection);
}
