use bytes::BufMut;
#[cfg(feature = "cassandra-cpp-driver-tests")]
use cassandra_cpp::{
    stmt, Batch, BatchType, CassErrorCode, CassResult, Cluster, Error, ErrorKind,
    PreparedStatement as PreparedStatementCpp, Session as DatastaxSession, Ssl, Value, ValueType,
};
#[cfg(feature = "cassandra-cpp-driver-tests")]
use cassandra_protocol::frame::message_error::ErrorType;
use cassandra_protocol::types::IntoRustByIndex;
use cassandra_protocol::{
    frame::message_error::ErrorBody,
    types::cassandra_type::{wrapper_fn, CassandraType},
};
use cdrs_tokio::query::QueryParamsBuilder;
use cdrs_tokio::statement::StatementParams;
use cdrs_tokio::{
    authenticators::StaticPasswordAuthenticatorProvider,
    cluster::session::{Session as CdrsTokioSession, SessionBuilder, TcpSessionBuilder},
    cluster::{NodeAddress, NodeTcpConfigBuilder, TcpConnectionManager},
    frame::{
        message_response::ResponseBody, message_result::ResResultBody, Envelope, Serialize, Version,
    },
    load_balancing::RoundRobinLoadBalancingStrategy,
    query::{BatchQueryBuilder, PreparedQuery as CdrsTokioPreparedQuery},
    query_values,
    transport::TransportTcp,
    types::prelude::Error as CdrsError,
};
use openssl::ssl::{SslContext, SslMethod};
use ordered_float::OrderedFloat;
use scylla::batch::Batch as ScyllaBatch;
use scylla::frame::response::result::CqlValue;
use scylla::frame::types::Consistency;
use scylla::prepared_statement::PreparedStatement as PreparedStatementScylla;
use scylla::{Session as SessionScylla, SessionBuilder as SessionBuilderScylla};
#[cfg(feature = "cassandra-cpp-driver-tests")]
use std::fs::read_to_string;
use std::net::IpAddr;
use std::sync::Arc;

#[derive(Debug)]
pub enum PreparedQuery {
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    Datastax(PreparedStatementCpp),
    CdrsTokio(CdrsTokioPreparedQuery),
    Scylla(PreparedStatementScylla),
}

impl PreparedQuery {
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    pub fn as_datastax(&self) -> &PreparedStatementCpp {
        match self {
            PreparedQuery::Datastax(p) => p,
            _ => panic!("Not PreparedQuery::Datastax"),
        }
    }

    pub fn as_cdrs(&self) -> &CdrsTokioPreparedQuery {
        match self {
            PreparedQuery::CdrsTokio(p) => p,
            _ => panic!("Not PreparedQuery::CdrsTokio"),
        }
    }

    pub fn as_scylla(&self) -> &PreparedStatementScylla {
        match self {
            PreparedQuery::Scylla(s) => s,
            _ => panic!("Not PreparedQuery::Scylla"),
        }
    }
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
fn cpp_error_to_cdrs(code: CassErrorCode, message: String) -> ErrorBody {
    ErrorBody {
        ty: match code {
            CassErrorCode::SERVER_INVALID_QUERY => ErrorType::Invalid,
            CassErrorCode::SERVER_OVERLOADED => ErrorType::Overloaded,
            _ => unimplemented!("{code:?} is not implemented"),
        },
        message,
    }
}

#[allow(dead_code)]
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum CassandraDriver {
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    Datastax,
    CdrsTokio,
    Scylla,
}

type CdrsTokioSessionInstance = CdrsTokioSession<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

pub enum CassandraConnection {
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    Datastax {
        session: DatastaxSession,
        schema_awaiter: Option<SessionScylla>,
    },
    CdrsTokio {
        session: CdrsTokioSessionInstance,
        schema_awaiter: Option<SessionScylla>,
    },
    Scylla {
        session: SessionScylla,
        schema_awaiter: Option<SessionScylla>,
    },
}

impl CassandraConnection {
    #[allow(dead_code)]
    pub async fn new(contact_points: &str, port: u16, driver: CassandraDriver) -> Self {
        for contact_point in contact_points.split(',') {
            test_helpers::wait_for_socket_to_open(contact_point, port);
        }

        match driver {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            CassandraDriver::Datastax => {
                cassandra_cpp::set_log_logger();
                let mut cluster = Cluster::default();
                cluster.set_contact_points(contact_points).unwrap();
                cluster.set_credentials("cassandra", "cassandra").unwrap();
                cluster.set_port(port).unwrap();
                cluster.set_load_balance_round_robin();

                CassandraConnection::Datastax {
                    session: cluster
                        .connect_async()
                        .await
                        // By default unwrap uses the Debug formatter `{:?}` which is extremely noisy for the error type returned by `connect()`.
                        // So we instead force the Display formatter `{}` on the error.
                        .map_err(|err| format!("{err}"))
                        .unwrap(),
                    schema_awaiter: None,
                }
            }
            CassandraDriver::CdrsTokio => {
                let user = "cassandra";
                let password = "cassandra";
                let auth = StaticPasswordAuthenticatorProvider::new(&user, &password);

                let node_addresses = contact_points
                    .split(',')
                    .map(|contact_point| NodeAddress::from(format!("{contact_point}:{port}")))
                    .collect::<Vec<NodeAddress>>();

                let config = NodeTcpConfigBuilder::new()
                    .with_contact_points(node_addresses)
                    .with_authenticator_provider(Arc::new(auth))
                    .build()
                    .await
                    .unwrap();

                let session =
                    TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), config)
                        .build()
                        .unwrap();
                CassandraConnection::CdrsTokio {
                    session,
                    schema_awaiter: None,
                }
            }
            CassandraDriver::Scylla => {
                let session = SessionBuilderScylla::new()
                    .known_nodes(
                        &contact_points
                            .split(',')
                            .map(|contact_point| format!("{contact_point}:{port}"))
                            .collect::<Vec<String>>(),
                    )
                    .user("cassandra", "cassandra")
                    .default_consistency(Consistency::One)
                    .build()
                    .await
                    .unwrap();

                CassandraConnection::Scylla {
                    session,
                    schema_awaiter: None,
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn as_cdrs(&self) -> &CdrsTokioSessionInstance {
        match self {
            Self::CdrsTokio { session, .. } => session,
            _ => panic!("Not CdrsTokio"),
        }
    }

    #[allow(dead_code)]
    pub fn is(&self, drivers: &[CassandraDriver]) -> bool {
        match self {
            Self::CdrsTokio { .. } => drivers.contains(&CassandraDriver::CdrsTokio),
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { .. } => drivers.contains(&CassandraDriver::Datastax),
            Self::Scylla { .. } => drivers.contains(&CassandraDriver::Scylla),
        }
    }

    #[cfg(feature = "cassandra-cpp-driver-tests")]
    #[allow(dead_code)]
    pub fn as_datastax(&self) -> &DatastaxSession {
        match self {
            Self::Datastax { session, .. } => session,
            _ => panic!("Not Datastax"),
        }
    }

    #[allow(dead_code, unused_variables)]
    pub async fn new_tls(
        contact_points: &str,
        port: u16,
        ca_cert_path: &str,
        driver: CassandraDriver,
    ) -> Self {
        match driver {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            CassandraDriver::Datastax => {
                cassandra_cpp::set_log_logger();
                let ca_cert = read_to_string(ca_cert_path).unwrap();
                let mut ssl = Ssl::default();
                Ssl::add_trusted_cert(&mut ssl, &ca_cert).unwrap();

                for contact_point in contact_points.split(',') {
                    test_helpers::wait_for_socket_to_open(contact_point, port);
                }

                let mut cluster = Cluster::default();
                cluster.set_credentials("cassandra", "cassandra").unwrap();
                cluster.set_contact_points(contact_points).unwrap();
                cluster.set_port(port).ok();
                cluster.set_load_balance_round_robin();
                cluster.set_ssl(&mut ssl);

                CassandraConnection::Datastax {
                    session: cluster.connect_async().await.unwrap(),
                    schema_awaiter: None,
                }
            }
            // TODO actually implement TLS for cdrs-tokio
            CassandraDriver::CdrsTokio => todo!(),
            CassandraDriver::Scylla => todo!(),
        }
    }

    #[allow(dead_code)]
    pub async fn enable_schema_awaiter(&mut self, direct_node: &str, ca_cert: Option<&str>) {
        let context = ca_cert.map(|ca_cert| {
            let mut context = SslContext::builder(SslMethod::tls()).unwrap();
            context.set_ca_file(ca_cert).unwrap();
            context.build()
        });

        let schema_awaiter = match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { schema_awaiter, .. } => schema_awaiter,
            Self::CdrsTokio { schema_awaiter, .. } => schema_awaiter,
            Self::Scylla { schema_awaiter, .. } => schema_awaiter,
        };

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

    pub async fn await_schema_agreement(&self) {
        let schema_awaiter = match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { schema_awaiter, .. } => schema_awaiter,
            Self::CdrsTokio { schema_awaiter, .. } => schema_awaiter,
            Self::Scylla { schema_awaiter, .. } => schema_awaiter,
        };
        if let Some(schema_awaiter) = schema_awaiter {
            schema_awaiter.await_schema_agreement().await.unwrap();
        }
    }

    #[allow(dead_code)]
    pub async fn execute(&self, query: &str) -> Vec<Vec<ResultValue>> {
        let result = match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                let statement = stmt!(query);
                match session.execute(&statement).await {
                    Ok(result) => result
                        .into_iter()
                        .map(|x| x.into_iter().map(ResultValue::new_from_cpp).collect())
                        .collect(),
                    Err(Error(err, _)) => panic!("The CQL query: {query}\nFailed with: {err}"),
                }
            }
            Self::CdrsTokio { session, .. } => {
                let response = session.query(query).await.unwrap();
                Self::process_cdrs_response(response)
            }
            Self::Scylla { session, .. } => {
                let rows = session.query(query, ()).await.unwrap().rows;
                match rows {
                    Some(rows) => rows
                        .into_iter()
                        .map(|x| {
                            x.columns
                                .into_iter()
                                .map(ResultValue::new_from_scylla)
                                .collect()
                        })
                        .collect(),
                    None => vec![],
                }
            }
        };

        let query = query.to_uppercase();
        let query = query.trim();
        if query.starts_with("CREATE") || query.starts_with("ALTER") || query.starts_with("DROP") {
            self.await_schema_agreement().await;
        }

        result
    }

    #[allow(dead_code)]
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    pub async fn execute_fallible(&self, query: &str) -> Result<CassResult, cassandra_cpp::Error> {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                let statement = stmt!(query);
                session.execute(&statement).await
            }
            Self::CdrsTokio { .. } => todo!(),
            Self::Scylla { .. } => todo!(),
        }
    }

    #[allow(dead_code)]
    pub async fn execute_expect_err(&self, query: &str) -> ErrorBody {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                let statement = stmt!(query);
                let error = session.execute(&statement).await.unwrap_err();

                if let ErrorKind::CassErrorResult(code, msg, ..) = error.0 {
                    cpp_error_to_cdrs(code, msg)
                } else {
                    panic!("Did not get an error result for {query}");
                }
            }
            Self::CdrsTokio { session, .. } => {
                let error = session.query(query).await.unwrap_err();

                match error {
                    CdrsError::Server { body, .. } => body,
                    _ => todo!(),
                }
            }
            Self::Scylla { .. } => todo!(),
        }
    }

    #[allow(dead_code)]
    pub async fn execute_expect_err_contains(&self, query: &str, contains: &str) {
        let error_msg = self.execute_expect_err(query).await.message;
        assert!(
            error_msg.contains(contains),
            "Expected the error to contain '{contains}' but it did not and was instead '{error_msg}'"
        );
    }

    #[allow(dead_code)]
    pub async fn prepare(&self, query: &str) -> PreparedQuery {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                PreparedQuery::Datastax(session.prepare(query).unwrap().await.unwrap())
            }
            Self::CdrsTokio { session, .. } => {
                let query = session.prepare(query).await.unwrap();
                PreparedQuery::CdrsTokio(query)
            }
            Self::Scylla { session, .. } => {
                let mut prepared = session.prepare(query).await.unwrap();
                prepared.set_tracing(true);
                PreparedQuery::Scylla(prepared)
            }
        }
    }

    #[allow(dead_code)]
    pub async fn execute_prepared_coordinator_node(
        &self,
        prepared_query: &PreparedQuery,
        key: i32,
    ) -> IpAddr {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { .. } => {
                todo!();
            }
            Self::CdrsTokio { session, .. } => {
                let statement = prepared_query.as_cdrs();
                let query_params = QueryParamsBuilder::new()
                    .with_values(query_values!(key))
                    .build();

                let params = StatementParams {
                    query_params,
                    is_idempotent: false,
                    keyspace: None,
                    token: None,
                    routing_key: None,
                    tracing: true,
                    warnings: false,
                    speculative_execution_policy: None,
                    retry_policy: None,
                    beta_protocol: false,
                };

                let response = session.exec_with_params(statement, &params).await.unwrap();

                let tracing_id = response.tracing_id.unwrap();
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // let cassandra finish writing to the tracing table
                let row = session
                    .query(format!(
                        "SELECT coordinator FROM system_traces.sessions WHERE session_id = {}",
                        tracing_id
                    ))
                    .await
                    .unwrap()
                    .response_body()
                    .unwrap()
                    .into_rows()
                    .unwrap();

                row[0].get_by_index(0).unwrap().unwrap()
            }
            Self::Scylla { session, .. } => {
                let statement = prepared_query.as_scylla();
                let response = session.execute(statement, (key,)).await.unwrap();
                let tracing_id = response.tracing_id.unwrap();

                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                session
                    .get_tracing_info(&tracing_id)
                    .await
                    .unwrap()
                    .coordinator
                    .unwrap()
            }
        }
    }

    #[allow(dead_code)]
    pub async fn execute_prepared(
        &self,
        prepared_query: &PreparedQuery,
        value: Option<i32>,
    ) -> Vec<Vec<ResultValue>> {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                let mut statement = prepared_query.as_datastax().bind();
                if let Some(value) = value {
                    statement.bind_int32(0, value).unwrap();
                }
                statement.set_tracing(true).unwrap();
                match session.execute(&statement).await {
                    Ok(result) => result
                        .into_iter()
                        .map(|x| x.into_iter().map(ResultValue::new_from_cpp).collect())
                        .collect(),
                    Err(Error(err, _)) => {
                        panic!("The statement: {statement:?}\nFailed with: {err}")
                    }
                }
            }
            Self::CdrsTokio { session, .. } => {
                let statement = prepared_query.as_cdrs();

                let mut builder = QueryParamsBuilder::new();
                if let Some(value) = value {
                    builder = builder.with_values(query_values!(value));
                }
                let query_params = builder.build();

                let params = StatementParams {
                    query_params,
                    is_idempotent: false,
                    keyspace: None,
                    token: None,
                    routing_key: None,
                    tracing: true,
                    warnings: false,
                    speculative_execution_policy: None,
                    retry_policy: None,
                    beta_protocol: false,
                };

                let response = session.exec_with_params(statement, &params).await.unwrap();

                Self::process_cdrs_response(response)
            }
            Self::Scylla { session, .. } => {
                let statement = prepared_query.as_scylla();
                let response = if let Some(value) = value {
                    session.execute(statement, (value,)).await.unwrap()
                } else {
                    session.execute(statement, ()).await.unwrap()
                };

                match response.rows {
                    Some(rows) => rows
                        .into_iter()
                        .map(|row| {
                            row.columns
                                .into_iter()
                                .map(ResultValue::new_from_scylla)
                                .collect()
                        })
                        .collect(),
                    None => vec![],
                }
            }
        }
    }

    #[allow(dead_code)]
    pub async fn execute_batch(&self, queries: Vec<String>) {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                let mut batch = Batch::new(BatchType::LOGGED);

                for query in queries {
                    batch.add_statement(&stmt!(query.as_str())).unwrap();
                }

                match session.execute_batch(&batch).await {
                    Ok(result) => assert_eq!(
                        result.into_iter().count(),
                        0,
                        "Batches should never return results",
                    ),
                    Err(Error(err, _)) => panic!("The batch: {batch:?}\nFailed with: {err}"),
                }
            }
            Self::CdrsTokio { session, .. } => {
                let mut builder = BatchQueryBuilder::new();

                for query in queries {
                    builder = builder.add_query(query, query_values!());
                }

                let batch = builder.build().unwrap();

                session.batch(batch).await.unwrap();
            }
            Self::Scylla { session, .. } => {
                let mut values = vec![];
                let mut batch: ScyllaBatch = Default::default();
                for query in queries {
                    batch.append_statement(query.as_str());
                    values.push(());
                }

                session.batch(&batch, values).await.unwrap();
            }
        }
    }

    #[allow(dead_code, unused_variables)]
    pub async fn execute_batch_expect_err(&self, queries: Vec<String>) -> ErrorBody {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                let mut batch = Batch::new(BatchType::LOGGED);
                for query in queries {
                    batch.add_statement(&stmt!(query.as_str())).unwrap();
                }
                let error = session.execute_batch(&batch).await.unwrap_err();
                if let ErrorKind::CassErrorResult(code, msg, ..) = error.0 {
                    cpp_error_to_cdrs(code, msg)
                } else {
                    panic!("Did not get an error result for {batch:?}");
                }
            }
            Self::CdrsTokio { .. } => todo!(),
            Self::Scylla { .. } => todo!(),
        }
    }

    fn process_cdrs_response(response: Envelope) -> Vec<Vec<ResultValue>> {
        let version = response.version;
        let response_body = response.response_body().unwrap();

        match response_body {
            ResponseBody::Error(err) => {
                panic!("CQL query Failed with: {err:?}")
            }
            ResponseBody::Result(res_result_body) => match res_result_body {
                ResResultBody::Rows(rows) => {
                    let mut result_values = vec![];

                    for row in &rows.rows_content {
                        let mut row_result_values = vec![];
                        for (i, col_spec) in rows.metadata.col_specs.iter().enumerate() {
                            let wrapper = wrapper_fn(&col_spec.col_type.id);
                            let value = ResultValue::new_from_cdrs(
                                wrapper(&row[i], &col_spec.col_type, version).unwrap(),
                                version,
                            );

                            row_result_values.push(value);
                        }
                        result_values.push(row_result_values);
                    }

                    result_values
                }
                ResResultBody::Prepared(_) => todo!(),
                ResResultBody::SchemaChange(_) => vec![],
                ResResultBody::SetKeyspace(_) => vec![],
                ResResultBody::Void => vec![],
            },
            _ => todo!(),
        }
    }
}

#[derive(Debug, Clone, PartialOrd, Eq, Ord)]
pub enum ResultValue {
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
    Inet(IpAddr),
    SmallInt(i16),
    Time(i64),
    Timestamp(i64),
    TimeUuid(uuid::Uuid),
    Counter(i64),
    TinyInt(i8),
    VarInt(Vec<u8>),
    Date(u32),
    Set(Vec<ResultValue>),
    List(Vec<ResultValue>),
    Tuple(Vec<ResultValue>),
    Map(Vec<(ResultValue, ResultValue)>),
    Null,
    /// Never output by the DB
    /// Can be used by the user in assertions to allow any value.
    #[allow(dead_code)]
    Any,
}

impl PartialEq for ResultValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
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
            (Self::Set(l0), Self::Set(r0)) => l0 == r0,
            (Self::List(l0), Self::List(r0)) => l0 == r0,
            (Self::Tuple(l0), Self::Tuple(r0)) => l0 == r0,
            (Self::Map(l0), Self::Map(r0)) => l0 == r0,
            (Self::Null, Self::Null) => true,
            (Self::Any, _) => true,
            (_, Self::Any) => true,
            _ => false,
        }
    }
}

impl ResultValue {
    #[allow(dead_code)]
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    pub fn new_from_cpp(value: Value) -> Self {
        if value.is_null() {
            ResultValue::Null
        } else {
            match value.get_type() {
                ValueType::VARCHAR => ResultValue::Varchar(value.get_string().unwrap()),
                ValueType::INT => ResultValue::Int(value.get_i32().unwrap()),
                ValueType::BOOLEAN => ResultValue::Boolean(value.get_bool().unwrap()),
                ValueType::UUID => ResultValue::Uuid(value.get_uuid().unwrap().into()),
                ValueType::ASCII => ResultValue::Ascii(value.get_string().unwrap()),
                ValueType::BIGINT => ResultValue::BigInt(value.get_i64().unwrap()),
                ValueType::BLOB => ResultValue::Blob(value.get_bytes().unwrap().to_vec()),
                ValueType::DATE => ResultValue::Date(value.get_u32().unwrap()),
                ValueType::DECIMAL => ResultValue::Decimal(value.get_bytes().unwrap().to_vec()),
                ValueType::DOUBLE => ResultValue::Double(value.get_f64().unwrap().into()),
                ValueType::DURATION => ResultValue::Duration(value.get_bytes().unwrap().to_vec()),
                ValueType::FLOAT => ResultValue::Float(value.get_f32().unwrap().into()),
                ValueType::INET => ResultValue::Inet(value.get_inet().as_ref().unwrap().into()),
                ValueType::SMALL_INT => ResultValue::SmallInt(value.get_i16().unwrap()),
                ValueType::TIME => ResultValue::Time(value.get_i64().unwrap()),
                ValueType::TIMESTAMP => ResultValue::Timestamp(value.get_i64().unwrap()),
                ValueType::TIMEUUID => ResultValue::TimeUuid(value.get_uuid().unwrap().into()),
                ValueType::COUNTER => ResultValue::Counter(value.get_i64().unwrap()),
                ValueType::VARINT => ResultValue::VarInt(value.get_bytes().unwrap().to_vec()),
                ValueType::TINY_INT => ResultValue::TinyInt(value.get_i8().unwrap()),
                ValueType::SET => ResultValue::Set(
                    value
                        .get_set()
                        .unwrap()
                        .map(ResultValue::new_from_cpp)
                        .collect(),
                ),
                // despite the name get_set is used by SET, LIST and TUPLE
                ValueType::LIST => ResultValue::List(
                    value
                        .get_set()
                        .unwrap()
                        .map(ResultValue::new_from_cpp)
                        .collect(),
                ),
                ValueType::TUPLE => ResultValue::Tuple(
                    value
                        .get_set()
                        .unwrap()
                        .map(ResultValue::new_from_cpp)
                        .collect(),
                ),
                ValueType::MAP => ResultValue::Map(
                    value
                        .get_map()
                        .unwrap()
                        .map(|(k, v)| (ResultValue::new_from_cpp(k), ResultValue::new_from_cpp(v)))
                        .collect(),
                ),
                ValueType::UNKNOWN => todo!(),
                ValueType::CUSTOM => todo!(),
                ValueType::UDT => todo!(),
                ValueType::TEXT => unimplemented!("text is represented by the same id as varchar at the protocol level and therefore will never be instantiated by the datastax cpp driver. https://github.com/apache/cassandra/blob/703ccdee29f7e8c39aeb976e72e516415d609cf4/doc/native_protocol_v5.spec#L1184"),
            }
        }
    }

    pub fn new_from_cdrs(value: CassandraType, version: Version) -> Self {
        match value {
            CassandraType::Ascii(ascii) => ResultValue::Ascii(ascii),
            CassandraType::Bigint(big_int) => ResultValue::BigInt(big_int),
            CassandraType::Blob(blob) => ResultValue::Blob(blob.into_vec()),
            CassandraType::Boolean(b) => ResultValue::Boolean(b),
            CassandraType::Counter(counter) => ResultValue::Counter(counter),
            CassandraType::Decimal(decimal) => {
                ResultValue::Decimal(decimal.serialize_to_vec(version))
            }
            CassandraType::Double(double) => ResultValue::Double(double.into()),
            CassandraType::Float(float) => ResultValue::Float(float.into()),
            CassandraType::Int(int) => ResultValue::Int(int),
            CassandraType::Timestamp(timestamp) => ResultValue::Timestamp(timestamp),
            CassandraType::Uuid(uuid) => ResultValue::Uuid(uuid),
            CassandraType::Varchar(varchar) => ResultValue::Varchar(varchar),
            CassandraType::Varint(var_int) => ResultValue::VarInt(var_int.to_signed_bytes_be()),
            CassandraType::Timeuuid(uuid) => ResultValue::TimeUuid(uuid),
            CassandraType::Inet(ip_addr) => ResultValue::Inet(ip_addr),
            CassandraType::Date(date) => ResultValue::Date(date as u32),
            CassandraType::Time(time) => ResultValue::Time(time),
            CassandraType::Smallint(small_int) => ResultValue::SmallInt(small_int),
            CassandraType::Tinyint(tiny_int) => ResultValue::TinyInt(tiny_int),
            CassandraType::Duration(duration) => {
                ResultValue::Duration(duration.serialize_to_vec(version))
            }
            CassandraType::List(list) => ResultValue::List(
                list.into_iter()
                    .map(|element| ResultValue::new_from_cdrs(element, version))
                    .collect(),
            ),
            CassandraType::Map(map) => ResultValue::Map(
                map.into_iter()
                    .map(|(k, v)| {
                        (
                            ResultValue::new_from_cdrs(k, version),
                            ResultValue::new_from_cdrs(v, version),
                        )
                    })
                    .collect(),
            ),
            CassandraType::Set(set) => ResultValue::Set(
                set.into_iter()
                    .map(|element| ResultValue::new_from_cdrs(element, version))
                    .collect(),
            ),
            CassandraType::Udt(_) => todo!(),
            CassandraType::Tuple(tuple) => ResultValue::Tuple(
                tuple
                    .into_iter()
                    .map(|element| ResultValue::new_from_cdrs(element, version))
                    .collect(),
            ),
            CassandraType::Null => ResultValue::Null,
        }
    }

    pub fn new_from_scylla(value: Option<CqlValue>) -> Self {
        match value {
            Some(value) => match value {
                CqlValue::Ascii(ascii) => Self::Ascii(ascii),
                CqlValue::BigInt(big_int) => Self::BigInt(big_int),
                CqlValue::Blob(blob) => Self::Blob(blob),
                CqlValue::Boolean(b) => Self::Boolean(b),
                CqlValue::Counter(_counter) => todo!(),
                CqlValue::Decimal(d) => {
                    let (value, scale) = d.as_bigint_and_exponent();
                    let mut buf = vec![];
                    let serialized = value.to_signed_bytes_be();
                    buf.put_i32(scale.try_into().unwrap());
                    buf.extend_from_slice(&serialized);
                    Self::Decimal(buf)
                }
                CqlValue::Float(float) => Self::Float(float.into()),
                CqlValue::Int(int) => Self::Int(int),
                CqlValue::Timestamp(timestamp) => Self::Timestamp(timestamp.num_milliseconds()),
                CqlValue::Uuid(uuid) => Self::Uuid(uuid),
                CqlValue::Varint(var_int) => {
                    let mut buf = vec![];
                    let serialized = var_int.to_signed_bytes_be();
                    buf.extend_from_slice(&serialized);
                    Self::VarInt(buf)
                }
                CqlValue::Timeuuid(timeuuid) => Self::TimeUuid(timeuuid),
                CqlValue::Inet(ip) => Self::Inet(ip),
                CqlValue::Date(date) => Self::Date(date),
                CqlValue::Time(time) => Self::Time(time.num_nanoseconds().unwrap()),
                CqlValue::SmallInt(small_int) => Self::SmallInt(small_int),
                CqlValue::TinyInt(tiny_int) => Self::TinyInt(tiny_int),
                CqlValue::Duration(_duration) => todo!(),
                CqlValue::Double(double) => Self::Double(double.into()),
                CqlValue::Text(text) => Self::Varchar(text),
                CqlValue::Empty => Self::Null,
                CqlValue::List(list) => Self::List(
                    list.into_iter()
                        .map(|v| Self::new_from_scylla(Some(v)))
                        .collect(),
                ),
                CqlValue::Set(set) => Self::Set(
                    set.into_iter()
                        .map(|v| Self::new_from_scylla(Some(v)))
                        .collect(),
                ),
                CqlValue::Map(map) => Self::Map(
                    map.into_iter()
                        .map(|(k, v)| {
                            (
                                Self::new_from_scylla(Some(k)),
                                Self::new_from_scylla(Some(v)),
                            )
                        })
                        .collect(),
                ),
                CqlValue::Tuple(tuple) => {
                    Self::Tuple(tuple.into_iter().map(Self::new_from_scylla).collect())
                }
                CqlValue::UserDefinedType { .. } => todo!(),
            },
            None => Self::Null,
        }
    }
}

/// Execute a `query` against the `session` and assert that the result rows match `expected_rows`
#[allow(dead_code)]
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
#[allow(dead_code)]
pub fn assert_rows(result_rows: Vec<Vec<ResultValue>>, expected_rows: &[&[ResultValue]]) {
    let mut expected_rows: Vec<_> = expected_rows.iter().map(|x| x.to_vec()).collect();
    expected_rows.sort();

    assert_eq!(result_rows, expected_rows);
}

/// Execute a `query` against the `session` and assert the result rows contain `row`
#[allow(dead_code)]
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
#[allow(dead_code)]
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

#[allow(dead_code)]
pub async fn run_query(session: &CassandraConnection, query: &str) {
    assert_query_result(session, query, &[]).await;
}
