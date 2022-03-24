use cassandra_cpp::{stmt, Cluster, Error, Session, Value, ValueType};
use ordered_float::OrderedFloat;

pub fn cassandra_connection(contact_points: &str, port: u16) -> Session {
    for contact_point in contact_points.split(',') {
        test_helpers::wait_for_socket_to_open(contact_point, port);
    }
    let mut cluster = Cluster::default();
    cluster.set_contact_points(contact_points).unwrap();
    cluster.set_credentials("cassandra", "cassandra").unwrap();
    cluster.set_port(port).ok();
    cluster.set_load_balance_round_robin();
    cluster.connect().unwrap()
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub enum ResultValue {
    Text(String),
    Varchar(String),
    Int(i32),
    Boolean(bool),
    Uuid(uuid::Uuid),
    Ascii(String),
    BigInt(i64),
    Blob(Vec<u8>),
    Decimal(Vec<u8>),
    Double(OrderedFloat<f64>),
    Duration(Vec<u8>), // TODO should be i32
    Float(OrderedFloat<f32>),
    Inet(String),
    SmallInt(i16),
    Time(Vec<u8>), // TODO shoulbe be String
    Timestamp(i64),
    TimeUuid(uuid::Uuid),
    Counter(i64),
    TinyInt(i8),
    VarInt(Vec<u8>),
    Date(Vec<u8>), // TODO should be string
    List(Vec<ResultValue>),
    Set(Vec<ResultValue>),
    Map(Vec<(ResultValue, ResultValue)>),
}

impl ResultValue {
    #[allow(unused)]
    pub fn new(value: Value) -> ResultValue {
        match value.get_type() {
            ValueType::TEXT => ResultValue::Text(value.get_string().unwrap()),
            ValueType::VARCHAR => ResultValue::Varchar(value.get_string().unwrap()),
            ValueType::INT => ResultValue::Int(value.get_i32().unwrap()),
            ValueType::BOOLEAN => ResultValue::Boolean(value.get_bool().unwrap()),
            ValueType::UUID => ResultValue::Uuid(
                uuid::Uuid::parse_str(&value.get_uuid().unwrap().to_string()).unwrap(),
            ),
            ValueType::ASCII => ResultValue::Ascii(value.get_string().unwrap()),
            ValueType::BIGINT => ResultValue::BigInt(value.get_i64().unwrap()),
            ValueType::BLOB => ResultValue::Blob(value.get_bytes().unwrap().to_vec()),
            ValueType::DATE => ResultValue::Date(value.get_bytes().unwrap().to_vec()),
            ValueType::DECIMAL => ResultValue::Decimal(value.get_bytes().unwrap().to_vec()),
            ValueType::DOUBLE => ResultValue::Double(value.get_f64().unwrap().into()),
            ValueType::DURATION => ResultValue::Duration(value.get_bytes().unwrap().to_vec()),
            ValueType::FLOAT => ResultValue::Float(value.get_f32().unwrap().into()),
            ValueType::INET => ResultValue::Inet(value.get_inet().unwrap().to_string()),
            ValueType::SMALL_INT => ResultValue::SmallInt(value.get_i16().unwrap()),
            ValueType::TIME => ResultValue::Time(value.get_bytes().unwrap().to_vec()),
            ValueType::TIMESTAMP => ResultValue::Timestamp(value.get_i64().unwrap()),
            ValueType::TIMEUUID => ResultValue::TimeUuid(
                uuid::Uuid::parse_str(&value.get_uuid().unwrap().to_string()).unwrap(),
            ),
            ValueType::UNKNOWN => todo!(),
            ValueType::CUSTOM => todo!(),
            ValueType::COUNTER => ResultValue::Counter(value.get_i64().unwrap()),
            ValueType::VARINT => ResultValue::VarInt(value.get_bytes().unwrap().to_vec()),
            ValueType::TINY_INT => ResultValue::TinyInt(value.get_i8().unwrap()),
            ValueType::LIST => {
                let mut list = Vec::new();
                for i in value.get_set().unwrap() {
                    list.push(ResultValue::new(i));
                }
                ResultValue::List(list)
            }
            ValueType::MAP => {
                let mut map = Vec::new();
                for (k, v) in value.get_map().unwrap() {
                    map.push((ResultValue::new(k), ResultValue::new(v)));
                }
                ResultValue::Map(map)
            }
            ValueType::SET => {
                let mut set = Vec::new();
                for i in value.get_set().unwrap() {
                    set.push(ResultValue::new(i));
                }
                ResultValue::Set(set)
            }
            ValueType::UDT => todo!(),
            ValueType::TUPLE => todo!(),
        }
    }
}

/// Execute a `query` against the `session` and return result rows
#[allow(unused)]
pub fn execute_query(session: &Session, query: &str) -> Vec<Vec<ResultValue>> {
    let statement = stmt!(query);
    match session.execute(&statement).wait() {
        Ok(result) => result
            .into_iter()
            .map(|x| x.into_iter().map(ResultValue::new).collect())
            .collect(),
        Err(Error(err, _)) => panic!("The CSQL query: {query}\nFailed with: {err}"),
    }
}

/// Execute a `query` against the `session` and assert that the result rows match `expected_rows`
#[allow(unused)]
pub fn assert_query_result(session: &Session, query: &str, expected_rows: &[&[ResultValue]]) {
    let mut result_rows = execute_query(session, query);
    result_rows.sort();
    assert_rows(result_rows, expected_rows);
}

/// Assert that the results from an integration test match the expected rows
#[allow(unused)]
pub fn assert_rows(result_rows: Vec<Vec<ResultValue>>, expected_rows: &[&[ResultValue]]) {
    let mut expected_rows: Vec<_> = expected_rows.iter().map(|x| x.to_vec()).collect();
    expected_rows.sort();

    assert_eq!(result_rows, expected_rows);
}

/// Execute a `query` against the `session` and assert the result rows contain `row`
#[allow(unused)]
pub fn assert_query_result_contains_row(session: &Session, query: &str, row: &[ResultValue]) {
    let result_rows = execute_query(session, query);
    if !result_rows.contains(&row.to_vec()) {
        panic!(
            "expected row: {:?} missing from actual rows: {:?}",
            row, result_rows
        );
    }
}

/// Execute a `query` against the `session` and assert the result rows does not contain `row`
#[allow(unused)]
pub fn assert_query_result_not_contains_row(session: &Session, query: &str, row: &[ResultValue]) {
    let result_rows = execute_query(session, query);
    if result_rows.contains(&row.to_vec()) {
        panic!(
            "unexpected row: {:?} was found in actual rows: {:?}",
            row, result_rows
        );
    }
}

/// Execute a `query` against the `session` and assert that no rows were returned
#[allow(unused)]
pub fn run_query(session: &Session, query: &str) {
    assert_query_result(session, query, &[]);
}
