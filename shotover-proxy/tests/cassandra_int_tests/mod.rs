use cassandra_cpp::{stmt, Cluster, Error, Session, Value, ValueType};

mod basic_driver_tests;

pub fn cassandra_connection(contact_points: &str, port: u16) -> Session {
    for contact_point in contact_points.split(',') {
        crate::helpers::wait_for_socket_to_open(contact_point, port);
    }
    let mut cluster = Cluster::default();
    cluster.set_contact_points(contact_points).unwrap();
    cluster.set_port(port).ok();
    cluster.set_load_balance_round_robin();
    cluster.connect().unwrap()
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
enum ResultValue {
    Text(String),
    Varchar(String),
    Int(i32),
    Boolean(bool),
}

impl ResultValue {
    fn new(value: Value) -> ResultValue {
        match value.get_type() {
            ValueType::TEXT => ResultValue::Text(value.get_string().unwrap()),
            ValueType::VARCHAR => ResultValue::Varchar(value.get_string().unwrap()),
            ValueType::INT => ResultValue::Int(value.get_i32().unwrap()),
            ValueType::BOOLEAN => ResultValue::Boolean(value.get_bool().unwrap()),
            ty => todo!(
                "The test infrastructure hasnt implemented the type {} yet, you should add it.",
                ty
            ),
        }
    }
}

/// Execute a `query` against the `session` and return result rows
fn execute_query(session: &Session, query: &str) -> Vec<Vec<ResultValue>> {
    let statement = stmt!(query);
    match session.execute(&statement).wait() {
        Ok(result) => result
            .into_iter()
            .map(|x| x.into_iter().map(ResultValue::new).collect())
            .collect(),
        Err(Error(err, _)) => panic!("The CSQL query: {}\nFailed with: {}", query, err),
    }
}

/// Execute a `query` against the `session` and assert that the result rows match `expected_rows`
fn assert_query_result(session: &Session, query: &str, expected_rows: &[&[ResultValue]]) {
    let mut result_rows = execute_query(session, query);
    result_rows.sort();

    let mut expected_rows: Vec<_> = expected_rows.iter().map(|x| x.to_vec()).collect();
    expected_rows.sort();

    assert_eq!(result_rows, expected_rows);
}

/// Execute a `query` against the `session` and assert the result rows contain `row`
fn assert_query_result_contains_row(session: &Session, query: &str, row: &[ResultValue]) {
    let result_rows = execute_query(session, query);
    if !result_rows.contains(&row.to_vec()) {
        panic!(
            "expected row: {:?} missing from actual rows: {:?}",
            row, result_rows
        );
    }
}

/// Execute a `query` against the `session` and assert the result rows does not contain `row`
fn assert_query_result_not_contains_row(session: &Session, query: &str, row: &[ResultValue]) {
    let result_rows = execute_query(session, query);
    if result_rows.contains(&row.to_vec()) {
        panic!(
            "unexpected row: {:?} was found in actual rows: {:?}",
            row, result_rows
        );
    }
}

/// Execute a `query` against the `session` and assert that no rows were returned
fn run_query(session: &Session, query: &str) {
    assert_query_result(session, query, &[]);
}
