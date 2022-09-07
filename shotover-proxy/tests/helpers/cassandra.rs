use cassandra_cpp::Error as CassandraError;
use cassandra_cpp::{
    stmt, Batch, CassFuture, CassResult, Cluster, Error, PreparedStatement, Session, Statement,
    Value, ValueType,
};
use openssl::ssl::{SslContext, SslMethod};
use ordered_float::OrderedFloat;
use scylla::{Session as SessionScylla, SessionBuilder as SessionBuilderScylla};

pub enum CassandraConnection {
    Datastax {
        session: Session,
        schema_awaiter: Option<SessionScylla>,
    },
}

impl CassandraConnection {
    #[allow(unused)]
    pub async fn new(contact_points: &str, port: u16) -> CassandraConnection {
        for contact_point in contact_points.split(',') {
            test_helpers::wait_for_socket_to_open(contact_point, port);
        }
        let mut cluster = Cluster::default();
        cluster.set_contact_points(contact_points).unwrap();
        cluster.set_credentials("cassandra", "cassandra").unwrap();
        cluster.set_port(port).unwrap();
        cluster.set_load_balance_round_robin();

        CassandraConnection::Datastax {
            // By default unwrap uses the Debug formatter `{:?}` which is extremely noisy for the error type returned by `connect()`.
            // So we instead force the Display formatter `{}` on the error.
            session: cluster.connect().map_err(|err| format!("{err}")).unwrap(),
            schema_awaiter: None,
        }
    }

    #[allow(unused)]
    pub async fn enable_schema_awaiter(&mut self, direct_node: &str, ca_cert: Option<&str>) {
        let context = ca_cert.map(|ca_cert| {
            let mut context = SslContext::builder(SslMethod::tls()).unwrap();
            context.set_ca_file(ca_cert);
            context.build()
        });
        match self {
            CassandraConnection::Datastax { schema_awaiter, .. } => {
                *schema_awaiter = Some(
                    SessionBuilderScylla::new()
                        .known_node(direct_node)
                        .user("cassandra", "cassandra")
                        .ssl_context(context)
                        .build()
                        .await
                        .unwrap(),
                );
            }
        }
    }

    #[allow(unused)]
    pub async fn execute(&self, query: &str) -> Vec<Vec<ResultValue>> {
        let result = match self {
            CassandraConnection::Datastax { session, .. } => {
                let statement = stmt!(query);
                match session.execute(&statement).wait() {
                    Ok(result) => result
                        .into_iter()
                        .map(|x| x.into_iter().map(ResultValue::new).collect())
                        .collect(),
                    Err(Error(err, _)) => panic!("The CQL query: {query}\nFailed with: {err}"),
                }
            }
        };

        let query = query.to_uppercase();
        let query = query.trim();
        if query.starts_with("CREATE") || query.starts_with("ALTER") {
            match self {
                CassandraConnection::Datastax {
                    session,
                    schema_awaiter,
                } => {
                    if let Some(schema_awaiter) = schema_awaiter {
                        schema_awaiter.await_schema_agreement().await;
                    }
                }
            }
        }

        result
    }

    #[allow(unused)]
    pub fn execute_async(&self, query: &str) -> CassFuture<CassResult> {
        match self {
            CassandraConnection::Datastax { session, .. } => {
                let statement = stmt!(query);
                session.execute(&statement)
            }
        }
    }

    #[allow(unused)]
    pub fn execute_expect_err(&self, query: &str) -> CassandraError {
        match self {
            CassandraConnection::Datastax { session, .. } => {
                let statement = stmt!(query);
                session.execute(&statement).wait().unwrap_err()
            }
        }
    }

    #[allow(unused)]
    pub fn execute_expect_err_contains(&self, query: &str, contains: &str) {
        let result = self.execute_expect_err(query).to_string();
        assert!(
            result.contains(contains),
            "Expected the error to contain '{contains}' but it did not and was instead '{result}'"
        );
    }

    #[allow(unused)]
    pub fn prepare(&self, query: &str) -> PreparedStatement {
        match self {
            CassandraConnection::Datastax { session, .. } => {
                session.prepare(query).unwrap().wait().unwrap()
            }
        }
    }

    #[allow(unused)]
    pub fn execute_prepared(&self, statement: &Statement) -> Vec<Vec<ResultValue>> {
        match self {
            CassandraConnection::Datastax { session, .. } => {
                match session.execute(statement).wait() {
                    Ok(result) => result
                        .into_iter()
                        .map(|x| x.into_iter().map(ResultValue::new).collect())
                        .collect(),
                    Err(Error(err, _)) => {
                        panic!("The statement: {statement:?}\nFailed with: {err}")
                    }
                }
            }
        }
    }

    #[allow(unused)]
    pub fn execute_batch(&self, batch: &Batch) {
        match self {
            CassandraConnection::Datastax { session, .. } => {
                match session.execute_batch(batch).wait() {
                    Ok(result) => assert_eq!(
                        result.into_iter().count(),
                        0,
                        "Batches should never return results",
                    ),
                    Err(Error(err, _)) => panic!("The batch: {batch:?}\nFailed with: {err}"),
                }
            }
        }
    }

    #[allow(unused)]
    pub fn execute_batch_expect_err(&self, batch: &Batch) -> CassandraError {
        match self {
            CassandraConnection::Datastax { session, .. } => {
                session.execute_batch(batch).wait().unwrap_err()
            }
        }
    }
}

#[derive(Debug, Clone, PartialOrd, Eq, Ord)]
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
    Time(Vec<u8>), // TODO should be String
    Timestamp(i64),
    TimeUuid(uuid::Uuid),
    Counter(i64),
    TinyInt(i8),
    VarInt(Vec<u8>),
    Date(Vec<u8>), // TODO should be string
    Map(Vec<(ResultValue, ResultValue)>),
    List(Vec<ResultValue>),
    Set(Vec<ResultValue>),
    Tuple(Vec<ResultValue>),
    Null,
    /// Never output by the DB
    /// Can be used by the user in assertions to allow any value.
    #[allow(unused)]
    Any,
}

impl PartialEq for ResultValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Text(l0), Self::Text(r0)) => l0 == r0,
            (Self::Varchar(l0), Self::Varchar(r0)) => l0 == r0,
            (Self::Int(l0), Self::Int(r0)) => l0 == r0,
            (Self::Boolean(l0), Self::Boolean(r0)) => l0 == r0,
            (Self::Uuid(l0), Self::Uuid(r0)) => l0 == r0,
            (Self::Ascii(l0), Self::Ascii(r0)) => l0 == r0,
            (Self::BigInt(l0), Self::BigInt(r0)) => l0 == r0,
            (Self::Blob(l0), Self::Blob(r0)) => l0 == r0,
            (Self::Decimal(l0), Self::Decimal(r0)) => l0 == r0,
            (Self::Double(l0), Self::Double(r0)) => l0 == r0,
            (Self::Duration(l0), Self::Duration(r0)) => l0 == r0,
            (Self::Float(l0), Self::Float(r0)) => l0 == r0,
            (Self::Inet(l0), Self::Inet(r0)) => l0 == r0,
            (Self::SmallInt(l0), Self::SmallInt(r0)) => l0 == r0,
            (Self::Time(l0), Self::Time(r0)) => l0 == r0,
            (Self::Timestamp(l0), Self::Timestamp(r0)) => l0 == r0,
            (Self::TimeUuid(l0), Self::TimeUuid(r0)) => l0 == r0,
            (Self::Counter(l0), Self::Counter(r0)) => l0 == r0,
            (Self::TinyInt(l0), Self::TinyInt(r0)) => l0 == r0,
            (Self::VarInt(l0), Self::VarInt(r0)) => l0 == r0,
            (Self::Date(l0), Self::Date(r0)) => l0 == r0,
            (Self::List(l0), Self::List(r0)) => l0 == r0,
            (Self::Set(l0), Self::Set(r0)) => l0 == r0,
            (Self::Map(l0), Self::Map(r0)) => l0 == r0,
            (Self::Null, Self::Null) => true,
            (Self::Any, _) => true,
            (_, Self::Any) => true,
            _ => false,
        }
    }
}

impl ResultValue {
    #[allow(unused)]
    pub fn new(value: Value) -> ResultValue {
        let result = match value.get_type() {
            ValueType::TEXT => value.get_string().map(ResultValue::Text),
            ValueType::VARCHAR => value.get_string().map(ResultValue::Varchar),
            ValueType::INT => value.get_i32().map(ResultValue::Int),
            ValueType::BOOLEAN => value.get_bool().map(ResultValue::Boolean),
            ValueType::UUID => value
                .get_uuid()
                .map(|x| ResultValue::Uuid(uuid::Uuid::parse_str(&x.to_string()).unwrap())),
            ValueType::ASCII => value.get_string().map(ResultValue::Ascii),
            ValueType::BIGINT => value.get_i64().map(ResultValue::BigInt),
            ValueType::BLOB => value.get_bytes().map(|x| ResultValue::Blob(x.to_vec())),
            ValueType::DATE => value.get_bytes().map(|x| ResultValue::Date(x.to_vec())),
            ValueType::DECIMAL => value.get_bytes().map(|x| ResultValue::Decimal(x.to_vec())),
            ValueType::DOUBLE => value.get_f64().map(|x| ResultValue::Double(x.into())),
            ValueType::DURATION => value.get_bytes().map(|x| ResultValue::Duration(x.to_vec())),
            ValueType::FLOAT => value.get_f32().map(|x| ResultValue::Float(x.into())),
            ValueType::INET => value.get_inet().map(|x| ResultValue::Inet(x.to_string())),
            ValueType::SMALL_INT => value.get_i16().map(ResultValue::SmallInt),
            ValueType::TIME => value.get_bytes().map(|x| ResultValue::Time(x.to_vec())),
            ValueType::TIMESTAMP => value.get_i64().map(ResultValue::Timestamp),
            ValueType::TIMEUUID => value
                .get_uuid()
                .map(|x| ResultValue::TimeUuid(uuid::Uuid::parse_str(&x.to_string()).unwrap())),
            ValueType::COUNTER => value.get_i64().map(ResultValue::Counter),
            ValueType::VARINT => value.get_bytes().map(|x| ResultValue::VarInt(x.to_vec())),
            ValueType::TINY_INT => value.get_i8().map(ResultValue::TinyInt),
            ValueType::MAP => value.get_map().map(|map| {
                ResultValue::Map(
                    map.map(|(k, v)| (ResultValue::new(k), ResultValue::new(v)))
                        .collect(),
                )
            }),
            ValueType::SET => value
                .get_set()
                .map(|set| ResultValue::Set(set.map(ResultValue::new).collect())),
            // LIST and TUPLE also use get_set
            ValueType::LIST => value
                .get_set()
                .map(|list| ResultValue::List(list.map(ResultValue::new).collect())),
            ValueType::TUPLE => value
                .get_set()
                .map(|tuple| ResultValue::Tuple(tuple.map(ResultValue::new).collect())),
            ValueType::UDT => todo!(),
            ValueType::UNKNOWN => todo!(),
            ValueType::CUSTOM => todo!(),
        };
        match result {
            Ok(result) => result,
            Err(NULL) => ResultValue::Null,
            Err(err) => panic!("Failed to process value {:?}", value),
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
pub async fn assert_query_result(
    session: &CassandraConnection,
    query: &str,
    expected_rows: &[&[ResultValue]],
) {
    let mut result_rows = session.execute(query).await;
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
pub async fn assert_query_result_contains_row(
    session: &CassandraConnection,
    query: &str,
    row: &[ResultValue],
) {
    let result_rows = session.execute(query).await;
    if !result_rows.contains(&row.to_vec()) {
        panic!(
            "expected row: {:?} missing from actual rows: {:?}",
            row, result_rows
        );
    }
}

/// Execute a `query` against the `session` and assert the result rows does not contain `row`
#[allow(unused)]
pub async fn assert_query_result_not_contains_row(
    session: &CassandraConnection,
    query: &str,
    row: &[ResultValue],
) {
    let result_rows = session.execute(query).await;
    if result_rows.contains(&row.to_vec()) {
        panic!(
            "unexpected row: {:?} was found in actual rows: {:?}",
            row, result_rows
        );
    }
}

#[allow(unused)]
pub async fn run_query(session: &CassandraConnection, query: &str) {
    assert_query_result(session, query, &[]).await;
}
