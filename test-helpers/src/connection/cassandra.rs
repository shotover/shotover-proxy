use bytes::BufMut;
#[cfg(feature = "cassandra-cpp-driver-tests")]
use cassandra_cpp::{
    stmt, Batch, BatchType, CassErrorCode, CassResult, Cluster, Error, ErrorKind,
    PreparedStatement as PreparedStatementCpp, Session as DatastaxSession, Ssl,
    Statement as StatementCpp, Value, ValueType,
};
use cassandra_protocol::frame::message_error::ErrorType;
use cassandra_protocol::query::QueryValues;
use cassandra_protocol::types::IntoRustByIndex;
use cassandra_protocol::{
    frame::message_error::ErrorBody,
    types::cassandra_type::{wrapper_fn, CassandraType},
};
use cdrs_tokio::query::{QueryParams, QueryParamsBuilder};
use cdrs_tokio::statement::{StatementParams, StatementParamsBuilder};
use cdrs_tokio::{
    authenticators::StaticPasswordAuthenticatorProvider,
    cluster::session::{Session as CdrsTokioSession, SessionBuilder, TcpSessionBuilder},
    cluster::{NodeAddress, NodeTcpConfigBuilder, TcpConnectionManager},
    frame::{
        message_response::ResponseBody, message_result::ResResultBody, Envelope, Serialize, Version,
    },
    load_balancing::TopologyAwareLoadBalancingStrategy,
    query::{BatchQueryBuilder, PreparedQuery as CdrsTokioPreparedQuery},
    query_values,
    transport::TransportTcp,
};
use openssl::ssl::{SslContext, SslMethod};
use ordered_float::OrderedFloat;
use scylla::batch::Batch as ScyllaBatch;
use scylla::frame::response::result::CqlValue;
use scylla::frame::types::Consistency;
use scylla::frame::value::Value as ScyllaValue;
use scylla::prepared_statement::PreparedStatement as PreparedStatementScylla;
use scylla::statement::query::Query as ScyllaQuery;
use scylla::transport::errors::{DbError, QueryError};
use scylla::{QueryResult, Session as SessionScylla, SessionBuilder as SessionBuilderScylla};
#[cfg(feature = "cassandra-cpp-driver-tests")]
use std::fs::read_to_string;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

pub struct CassandraConnectionBuilder {
    contact_points: String,
    port: u16,
    driver: CassandraDriver,
    ca_cert_path: Option<String>,
    compression: Option<Compression>,
    protocol_version: Option<ProtocolVersion>,
}

impl CassandraConnectionBuilder {
    pub fn new(contact_points: &str, port: u16, driver: CassandraDriver) -> Self {
        Self {
            contact_points: contact_points.into(),
            port,
            driver,
            ca_cert_path: None,
            compression: None,
            protocol_version: None,
        }
    }

    pub fn with_tls(mut self, ca_cert_path: &str) -> Self {
        self.ca_cert_path = Some(ca_cert_path.into());
        self
    }

    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = Some(compression);
        self
    }

    pub fn with_protocol_version(mut self, protocol_version: ProtocolVersion) -> Self {
        self.protocol_version = Some(protocol_version);
        self
    }

    pub async fn build(self) -> CassandraConnection {
        let tls = if let Some(ca_cert_path) = self.ca_cert_path {
            match self.driver {
                #[cfg(feature = "cassandra-cpp-driver-tests")]
                CassandraDriver::Datastax => {
                    let ca_cert = read_to_string(ca_cert_path).unwrap();
                    let mut ssl = Ssl::default();
                    Ssl::add_trusted_cert(&mut ssl, &ca_cert).unwrap();

                    Some(Tls::Datastax(ssl))
                }
                // TODO actually implement TLS for cdrs-tokio
                CassandraDriver::CdrsTokio => todo!(),
                CassandraDriver::Scylla => {
                    let mut context = SslContext::builder(SslMethod::tls()).unwrap();
                    context.set_ca_file(ca_cert_path).unwrap();
                    let ssl_context = context.build();

                    Some(Tls::Scylla(ssl_context))
                }
            }
        } else {
            None
        };

        CassandraConnection::new(
            &self.contact_points,
            self.port,
            self.driver,
            self.compression,
            tls,
            self.protocol_version,
        )
        .await
    }
}

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

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Compression {
    Snappy,
    Lz4,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum ProtocolVersion {
    V3,
    V4,
    V5,
}

pub enum Tls {
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    Datastax(Ssl),
    Scylla(SslContext),
}

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
    TopologyAwareLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
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
    pub async fn new(
        contact_points: &str,
        port: u16,
        driver: CassandraDriver,
        compression: Option<Compression>,
        tls: Option<Tls>,
        protocol: Option<ProtocolVersion>,
    ) -> Self {
        for contact_point in contact_points.split(',') {
            crate::wait_for_socket_to_open(contact_point, port);
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

                if let Some(protocol) = protocol {
                    cluster
                        .set_protocol_version(match protocol {
                            ProtocolVersion::V3 => 3,
                            ProtocolVersion::V4 => 4,
                            ProtocolVersion::V5 => 5,
                        })
                        .unwrap();
                }

                if compression.is_some() {
                    panic!("Cannot set compression with Datastax driver");
                }

                if let Some(Tls::Datastax(mut ssl)) = tls {
                    cluster.set_ssl(&mut ssl);
                }

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

                let mut node_config_builder = NodeTcpConfigBuilder::new()
                    .with_contact_points(node_addresses)
                    .with_authenticator_provider(Arc::new(auth));

                if let Some(protocol) = protocol {
                    node_config_builder = node_config_builder.with_version(match protocol {
                        ProtocolVersion::V3 => Version::V3,
                        ProtocolVersion::V4 => Version::V4,
                        ProtocolVersion::V5 => Version::V5,
                    });
                }

                let config = timeout(Duration::from_secs(10), node_config_builder.build())
                    .await
                    .unwrap()
                    .unwrap();

                let mut session_builder = TcpSessionBuilder::new(
                    TopologyAwareLoadBalancingStrategy::new(None, true),
                    config,
                );

                if let Some(compression) = compression {
                    let compression = match compression {
                        Compression::Snappy => cassandra_protocol::compression::Compression::Snappy,
                        Compression::Lz4 => cassandra_protocol::compression::Compression::Lz4,
                    };

                    session_builder = session_builder.with_compression(compression);
                }

                let session = timeout(Duration::from_secs(10), session_builder.build())
                    .await
                    .unwrap()
                    .unwrap();
                CassandraConnection::CdrsTokio {
                    session,
                    schema_awaiter: None,
                }
            }
            CassandraDriver::Scylla => {
                let mut builder = SessionBuilderScylla::new()
                    .known_nodes(
                        &contact_points
                            .split(',')
                            .map(|contact_point| format!("{contact_point}:{port}"))
                            .collect::<Vec<String>>(),
                    )
                    .user("cassandra", "cassandra")
                    .compression(compression.map(|x| match x {
                        Compression::Snappy => scylla::transport::Compression::Snappy,
                        Compression::Lz4 => scylla::transport::Compression::Lz4,
                    }))
                    .default_consistency(Consistency::One);

                if let Some(compression) = compression {
                    let compression = match compression {
                        Compression::Snappy => scylla::transport::Compression::Snappy,
                        Compression::Lz4 => scylla::transport::Compression::Lz4,
                    };

                    builder = builder.compression(Some(compression));
                }

                if protocol.is_some() {
                    panic!("Cannot set protocol with Scylla");
                }

                if let Some(Tls::Scylla(ssl_context)) = tls {
                    builder = builder.ssl_context(Some(ssl_context));
                }

                let session = builder.build().await.unwrap();

                CassandraConnection::Scylla {
                    session,
                    schema_awaiter: None,
                }
            }
        }
    }

    pub fn as_cdrs(&self) -> &CdrsTokioSessionInstance {
        match self {
            Self::CdrsTokio { session, .. } => session,
            _ => panic!("Not CdrsTokio"),
        }
    }

    pub fn is(&self, drivers: &[CassandraDriver]) -> bool {
        match self {
            Self::CdrsTokio { .. } => drivers.contains(&CassandraDriver::CdrsTokio),
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { .. } => drivers.contains(&CassandraDriver::Datastax),
            Self::Scylla { .. } => drivers.contains(&CassandraDriver::Scylla),
        }
    }

    #[cfg(feature = "cassandra-cpp-driver-tests")]
    pub fn as_datastax(&self) -> &DatastaxSession {
        match self {
            Self::Datastax { session, .. } => session,
            _ => panic!("Not Datastax"),
        }
    }

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

    pub async fn execute(&self, query: &str) -> Vec<Vec<ResultValue>> {
        match self.execute_fallible(query).await {
            Ok(result) => result,
            Err(err) => panic!("The CQL query: {query}\nFailed with: {err:?}"),
        }
    }

    pub async fn execute_fallible(&self, query: &str) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let result = match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                let statement = stmt!(query);
                Self::process_datastax_response(session.execute(&statement).await)
            }
            Self::CdrsTokio { session, .. } => {
                Self::process_cdrs_response(session.query(query).await)
            }
            Self::Scylla { session, .. } => {
                Self::process_scylla_response(session.query(query, ()).await)
            }
        };

        let query = query.to_uppercase();
        let query = query.trim();
        if query.starts_with("CREATE") || query.starts_with("ALTER") || query.starts_with("DROP") {
            self.await_schema_agreement().await;
        }

        result
    }

    pub async fn execute_with_timestamp(
        &self,
        query: &str,
        timestamp: i64,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let result = match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                let mut statement = stmt!(query);
                statement.set_timestamp(timestamp).unwrap();
                Self::process_datastax_response(session.execute(&statement).await)
            }
            Self::CdrsTokio { session, .. } => {
                let statement_params = StatementParamsBuilder::new()
                    .with_timestamp(timestamp)
                    .build();

                let response = session.query_with_params(query, statement_params).await;
                Self::process_cdrs_response(response)
            }
            Self::Scylla { session, .. } => {
                let mut query = ScyllaQuery::new(query);
                query.set_timestamp(Some(timestamp));
                Self::process_scylla_response(session.query(query, ()).await)
            }
        };

        let query = query.to_uppercase();
        let query = query.trim();
        if query.starts_with("CREATE") || query.starts_with("ALTER") || query.starts_with("DROP") {
            self.await_schema_agreement().await;
        }

        result
    }

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

    pub async fn execute_prepared_coordinator_node(
        &self,
        prepared_query: &PreparedQuery,
        values: &[ResultValue],
    ) -> IpAddr {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { .. } => {
                todo!();
            }
            Self::CdrsTokio { session, .. } => {
                let statement = prepared_query.as_cdrs();

                let query_params = Self::bind_values_cdrs(values);

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
                let values = Self::build_values_scylla(values);

                let response = session.execute(statement, values).await.unwrap();
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

    pub async fn execute_prepared(
        &self,
        prepared_query: &PreparedQuery,
        values: &[ResultValue],
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                let mut statement = prepared_query.as_datastax().bind();
                for (i, value) in values.iter().enumerate() {
                    Self::bind_statement_values_cpp(&mut statement, i, value);
                }

                statement.set_tracing(true).unwrap();
                Self::process_datastax_response(session.execute(&statement).await)
            }
            Self::CdrsTokio { session, .. } => {
                let statement = prepared_query.as_cdrs();
                let query_params = Self::bind_values_cdrs(values);

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

                Self::process_cdrs_response(session.exec_with_params(statement, &params).await)
            }
            Self::Scylla { session, .. } => {
                let statement = prepared_query.as_scylla();
                let values = Self::build_values_scylla(values);

                Self::process_scylla_response(session.execute(statement, values).await)
            }
        }
    }

    fn bind_values_cdrs(values: &[ResultValue]) -> QueryParams {
        QueryParamsBuilder::new()
            .with_values(QueryValues::SimpleValues(
                values
                    .iter()
                    .map(|v| match v {
                        ResultValue::Int(v) => (*v).into(),
                        ResultValue::Ascii(v) => v.as_str().into(),
                        ResultValue::BigInt(v) => (*v).into(),
                        ResultValue::Blob(v) => v.clone().into(),
                        ResultValue::Boolean(v) => (*v).into(),
                        ResultValue::Decimal(v) => v.clone().into(),
                        ResultValue::Double(v) => (*v.as_ref()).into(),
                        ResultValue::Float(v) => (*v.as_ref()).into(),
                        ResultValue::Timestamp(v) => (*v).into(),
                        ResultValue::Uuid(v) => (*v).into(),
                        ResultValue::Inet(v) => (*v).into(),
                        ResultValue::Date(v) => (*v).into(),
                        ResultValue::Time(v) => (*v).into(),
                        ResultValue::SmallInt(v) => (*v).into(),
                        ResultValue::TinyInt(v) => (*v).into(),
                        ResultValue::Varchar(v) => v.as_str().into(),
                        value => todo!("Implement handling of {value:?} for cdrs-tokio"),
                    })
                    .collect(),
            ))
            .build()
    }

    fn build_values_scylla(values: &[ResultValue]) -> Vec<Box<dyn ScyllaValue + '_>> {
        values
            .iter()
            .map(|v| match v {
                ResultValue::Int(v) => Box::new(v) as Box<dyn ScyllaValue>,
                ResultValue::Ascii(v) => Box::new(v) as Box<dyn ScyllaValue>,
                ResultValue::BigInt(v) => Box::new(v) as Box<dyn ScyllaValue>,
                ResultValue::Blob(v) => Box::new(v) as Box<dyn ScyllaValue>,
                ResultValue::Boolean(v) => Box::new(v) as Box<dyn ScyllaValue>,
                ResultValue::Decimal(v) => Box::new(v) as Box<dyn ScyllaValue>,
                ResultValue::Double(v) => Box::new(*v.as_ref()) as Box<dyn ScyllaValue>,
                ResultValue::Float(v) => Box::new(*v.as_ref()) as Box<dyn ScyllaValue>,
                ResultValue::Timestamp(v) => Box::new(v) as Box<dyn ScyllaValue>,
                ResultValue::Uuid(v) => Box::new(v) as Box<dyn ScyllaValue>,
                ResultValue::Inet(v) => Box::new(v) as Box<dyn ScyllaValue>,
                ResultValue::Date(v) => Box::new(*v as i32) as Box<dyn ScyllaValue>,
                ResultValue::Time(v) => Box::new(v) as Box<dyn ScyllaValue>,
                ResultValue::SmallInt(v) => Box::new(v) as Box<dyn ScyllaValue>,
                ResultValue::TinyInt(v) => Box::new(v) as Box<dyn ScyllaValue>,
                ResultValue::Varchar(v) => Box::new(v) as Box<dyn ScyllaValue>,
                value => todo!("Implement handling of {value:?} for scylla"),
            })
            .collect()
    }

    #[cfg(feature = "cassandra-cpp-driver-tests")]
    fn bind_statement_values_cpp(statement: &mut StatementCpp, i: usize, value: &ResultValue) {
        match value {
            ResultValue::Int(v) => statement.bind_int32(i, *v).unwrap(),
            ResultValue::Ascii(v) => statement.bind_string(i, v).unwrap(),
            ResultValue::BigInt(v) => statement.bind_int64(i, *v).unwrap(),
            ResultValue::Blob(v) => statement.bind_bytes(i, v.clone()).unwrap(),
            ResultValue::Boolean(v) => statement.bind_bool(i, *v).unwrap(),
            ResultValue::Decimal(_) => {
                todo!("cassandra-cpp wrapper does not expose bind_decimal yet")
            }
            ResultValue::Double(v) => statement.bind_double(i, **v).unwrap(),
            ResultValue::Float(v) => statement.bind_float(i, **v).unwrap(),
            ResultValue::Timestamp(v) => statement.bind_int64(i, *v).unwrap(),
            ResultValue::Uuid(v) => statement.bind_uuid(i, (*v).into()).unwrap(),
            ResultValue::Inet(v) => statement.bind_inet(i, v.into()).unwrap(),
            ResultValue::Date(v) => statement.bind_uint32(i, *v).unwrap(),
            ResultValue::Time(v) => statement.bind_int64(i, *v).unwrap(),
            ResultValue::SmallInt(v) => statement.bind_int16(i, *v).unwrap(),
            ResultValue::TinyInt(v) => statement.bind_int8(i, *v).unwrap(),
            ResultValue::Varchar(v) => statement.bind_string(i, v).unwrap(),
            value => todo!("Implement handling of {value:?} for datastax"),
        };
    }

    pub async fn execute_batch_fallible(
        &self,
        queries: Vec<String>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                let mut batch = Batch::new(BatchType::LOGGED);
                for query in queries {
                    batch.add_statement(&stmt!(query.as_str())).unwrap();
                }

                Self::process_datastax_response(session.execute_batch(&batch).await)
            }
            Self::CdrsTokio { session, .. } => {
                let mut builder = BatchQueryBuilder::new();
                for query in queries {
                    builder = builder.add_query(query, query_values!());
                }
                let batch = builder.build().unwrap();

                Self::process_cdrs_response(session.batch(batch).await)
            }
            Self::Scylla { session, .. } => {
                let mut values = vec![];
                let mut batch: ScyllaBatch = Default::default();
                for query in queries {
                    batch.append_statement(query.as_str());
                    values.push(());
                }

                Self::process_scylla_response(session.batch(&batch, values).await)
            }
        }
    }

    pub async fn execute_batch(&self, queries: Vec<String>) {
        let result = self.execute_batch_fallible(queries).await.unwrap();
        assert_eq!(result.len(), 0, "Batches should never return results");
    }

    #[cfg(feature = "cassandra-cpp-driver-tests")]
    fn process_datastax_response(
        response: Result<CassResult, cassandra_cpp::Error>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        match response {
            Ok(result) => Ok(result
                .into_iter()
                .map(|x| x.into_iter().map(ResultValue::new_from_cpp).collect())
                .collect()),
            Err(Error(ErrorKind::CassErrorResult(code, message, ..), _)) => Err(ErrorBody {
                ty: match code {
                    CassErrorCode::SERVER_OVERLOADED => ErrorType::Overloaded,
                    CassErrorCode::SERVER_SERVER_ERROR => ErrorType::Server,
                    CassErrorCode::SERVER_INVALID_QUERY => ErrorType::Invalid,
                    code => todo!("Implement handling for cassandra_cpp err: {code:?}"),
                },
                message,
            }),
            Err(err) => panic!("Unexpected cassandra_cpp error: {err}"),
        }
    }

    fn process_scylla_response(
        response: Result<QueryResult, QueryError>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        match response {
            Ok(value) => Ok(match value.rows {
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
            }),
            Err(QueryError::DbError(code, message)) => Err(ErrorBody {
                ty: match code {
                    DbError::Overloaded => ErrorType::Overloaded,
                    DbError::ServerError => ErrorType::Server,
                    DbError::Invalid => ErrorType::Invalid,
                    code => todo!("Implement handling for cassandra_cpp err: {code:?}"),
                },
                message,
            }),
            Err(err) => panic!("Unexpected scylla error: {err:?}"),
        }
    }

    fn process_cdrs_response(
        response: Result<Envelope, cassandra_protocol::Error>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        match response {
            Ok(response) => {
                let version = response.version;
                let response_body = response.response_body().unwrap();

                Ok(match response_body {
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
                        _ => unreachable!(),
                    },
                    _ => todo!(),
                })
            }
            Err(cassandra_protocol::Error::Server { body, .. }) => Err(body),
            Err(err) => panic!("Unexpected cdrs-tokio error: {err:?}"),
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
            _ => unreachable!(),
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
pub fn assert_rows(result_rows: Vec<Vec<ResultValue>>, expected_rows: &[&[ResultValue]]) {
    let mut expected_rows: Vec<_> = expected_rows.iter().map(|x| x.to_vec()).collect();
    expected_rows.sort();

    assert_eq!(result_rows, expected_rows);
}

/// Execute a `query` against the `session` and assert the result rows contain `row`
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

pub async fn run_query(session: &CassandraConnection, query: &str) {
    assert_query_result(session, query, &[]).await;
}
