use cassandra_cpp::{Cluster, Session, Statement, Value, ValueType};

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

#[derive(PartialEq, Debug)]
enum ResultValue {
    Text(String),
    Varchar(String),
    Int(i64),
}

impl ResultValue {
    fn new(value: Value) -> ResultValue {
        match value.get_type() {
            ValueType::TEXT => ResultValue::Text(value.get_string().unwrap()),
            ValueType::VARCHAR => ResultValue::Varchar(value.get_string().unwrap()),
            ValueType::INT => ResultValue::Int(value.get_i64().unwrap()),
            ty => todo!(
                "The test infrastructure hasnt implemented the type {} yet, you should add it.",
                ty
            ),
        }
    }
}

fn assert_query_result(session: &Session, query: Statement, expected_rows: &[&[ResultValue]]) {
    let result = session.execute(&query).wait().unwrap();
    let result_rows: Vec<Vec<ResultValue>> = result
        .into_iter()
        .map(|x| x.into_iter().map(ResultValue::new).collect())
        .collect();
    assert_eq!(result_rows, expected_rows);
}
