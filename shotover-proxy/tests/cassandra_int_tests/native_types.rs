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
