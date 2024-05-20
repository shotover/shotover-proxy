#[cfg(feature = "cassandra-cpp-driver-tests")]
use cassandra_cpp::{
    BatchType, CassErrorCode, CassResult, Cluster, Consistency as DatastaxConsistency, Error,
    ErrorKind, LendingIterator, PreparedStatement as PreparedStatementCpp,
    Session as DatastaxSession, Ssl, Statement as StatementCpp,
};
use cassandra_protocol::frame::message_error::ErrorType;
use cassandra_protocol::query::QueryValues;
use cassandra_protocol::types::IntoRustByIndex;
use cassandra_protocol::{frame::message_error::ErrorBody, types::cassandra_type::wrapper_fn};
use cdrs_tokio::query::{QueryParams, QueryParamsBuilder};
use cdrs_tokio::statement::{StatementParams, StatementParamsBuilder};
use cdrs_tokio::{
    authenticators::StaticPasswordAuthenticatorProvider,
    cluster::session::{Session as CdrsTokioSession, SessionBuilder, TcpSessionBuilder},
    cluster::{NodeAddress, NodeTcpConfigBuilder, TcpConnectionManager},
    consistency::Consistency as CdrsConsistency,
    frame::{message_response::ResponseBody, message_result::ResResultBody, Envelope, Version},
    load_balancing::TopologyAwareLoadBalancingStrategy,
    query::{BatchQueryBuilder, PreparedQuery as CdrsTokioPreparedQuery},
    query_values,
    transport::TransportTcp,
};
use openssl::ssl::{SslContext, SslMethod};
use scylla::batch::Batch as ScyllaBatch;
use scylla::frame::types::Consistency as ScyllaConsistency;
use scylla::frame::value::{CqlDate, CqlDecimal, CqlTime, CqlTimestamp};
use scylla::prepared_statement::PreparedStatement as PreparedStatementScylla;
use scylla::serialize::value::SerializeCql;
use scylla::statement::query::Query as ScyllaQuery;
use scylla::transport::errors::{DbError, QueryError};
use scylla::{
    ExecutionProfile, QueryResult, Session as SessionScylla, SessionBuilder as SessionBuilderScylla,
};
#[cfg(feature = "cassandra-cpp-driver-tests")]
use std::fs::read_to_string;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use super::result_value::ResultValue;

const TIMEOUT: u64 = 10;

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

pub enum Consistency {
    One,
    All,
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

                if let Some(Tls::Datastax(ssl)) = tls {
                    cluster.set_ssl(ssl);
                }

                CassandraConnection::Datastax {
                    session: cluster
                        .connect()
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

                let config = timeout(Duration::from_secs(TIMEOUT), node_config_builder.build())
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

                let session = timeout(Duration::from_secs(TIMEOUT), session_builder.build())
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
                        contact_points
                            .split(',')
                            .map(|contact_point| format!("{contact_point}:{port}"))
                            .collect::<Vec<String>>(),
                    )
                    .user("cassandra", "cassandra")
                    // We do not need to refresh metadata as there is nothing else fiddling with the topology or schema.
                    // By default the metadata refreshes every 60s and that can cause performance issues so we disable it by using an absurdly high refresh interval
                    .cluster_metadata_refresh_interval(Duration::from_secs(10000000000))
                    .compression(compression.map(|x| match x {
                        Compression::Snappy => scylla::transport::Compression::Snappy,
                        Compression::Lz4 => scylla::transport::Compression::Lz4,
                    }))
                    .default_execution_profile_handle(
                        ExecutionProfile::builder()
                            .consistency(ScyllaConsistency::One)
                            .build()
                            .into_handle(),
                    );

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
        tokio::time::timeout(
            Duration::from_secs(TIMEOUT),
            self.execute_fallible_inner(query),
        )
        .await
        .unwrap_or_else(|_| panic!("The CQL query: {query}\nTimed out after 10s"))
    }

    pub async fn execute_fallible_inner(
        &self,
        query: &str,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let result = match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                Self::process_datastax_response(session.execute(query).await)
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
        tokio::time::timeout(
            Duration::from_secs(TIMEOUT),
            self.execute_with_timestamp_inner(query, timestamp),
        )
        .await
        .unwrap_or_else(|_| panic!("The CQL query: {query}\nTimed out after 10s"))
    }
    pub async fn execute_with_timestamp_inner(
        &self,
        query: &str,
        timestamp: i64,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let result = match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                let mut statement = session.statement(query);
                statement.set_timestamp(timestamp).unwrap();
                Self::process_datastax_response(statement.execute().await)
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
        tokio::time::timeout(Duration::from_secs(TIMEOUT), self.prepare_inner(query))
            .await
            .unwrap_or_else(|_| panic!("Preparing the CQL query: {query}\nTimed out after 10s"))
    }

    pub async fn prepare_inner(&self, query: &str) -> PreparedQuery {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                PreparedQuery::Datastax(session.prepare(query).await.unwrap())
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
        tokio::time::timeout(
            Duration::from_secs(TIMEOUT),
            self.execute_prepared_coordinator_node_inner(prepared_query, values),
        )
        .await
        .expect("Attempted to execute a CQL prepared query but timed out after 10s")
    }

    pub async fn execute_prepared_coordinator_node_inner(
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
        consistency: Consistency,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        tokio::time::timeout(
            Duration::from_secs(TIMEOUT),
            self.execute_prepared_inner(prepared_query, values, consistency),
        )
        .await
        .expect("Attempted to execute a CQL prepared query but timed out after 10s")
    }

    pub async fn execute_prepared_inner(
        &self,
        prepared_query: &PreparedQuery,
        values: &[ResultValue],
        consistency: Consistency,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { .. } => {
                let mut statement = prepared_query.as_datastax().bind();
                for (i, value) in values.iter().enumerate() {
                    Self::bind_statement_values_cpp(&mut statement, i, value);
                }

                statement.set_tracing(true).unwrap();
                statement
                    .set_consistency(match consistency {
                        Consistency::All => DatastaxConsistency::ALL,
                        Consistency::One => DatastaxConsistency::ONE,
                    })
                    .unwrap();
                Self::process_datastax_response(statement.execute().await)
            }
            Self::CdrsTokio { session, .. } => {
                let statement = prepared_query.as_cdrs();
                let mut query_params = Self::bind_values_cdrs(values);
                query_params.consistency = match consistency {
                    Consistency::All => CdrsConsistency::All,
                    Consistency::One => CdrsConsistency::One,
                };

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
                // clone to avoid changing default consistency
                let mut statement = prepared_query.as_scylla().clone();
                statement.set_consistency(match consistency {
                    Consistency::All => ScyllaConsistency::All,
                    Consistency::One => ScyllaConsistency::One,
                });
                let values = Self::build_values_scylla(values);

                Self::process_scylla_response(session.execute(&statement, values).await)
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

    // TODO: lets return Vec<CqlValue> instead, as it provides better guarantees for correctness
    fn build_values_scylla(values: &[ResultValue]) -> Vec<Box<dyn SerializeCql + '_>> {
        values
            .iter()
            .map(|v| match v {
                ResultValue::Int(v) => Box::new(v) as Box<dyn SerializeCql>,
                ResultValue::Ascii(v) => Box::new(v),
                ResultValue::BigInt(v) => Box::new(v),
                ResultValue::Blob(v) => Box::new(v),
                ResultValue::Boolean(v) => Box::new(v),
                ResultValue::Decimal(buf) => {
                    let scale = i32::from_be_bytes(buf[0..4].try_into().unwrap());
                    let bytes = &buf[4..];
                    Box::new(CqlDecimal::from_signed_be_bytes_slice_and_exponent(
                        bytes, scale,
                    ))
                }
                ResultValue::Double(v) => Box::new(*v.as_ref()),
                ResultValue::Float(v) => Box::new(*v.as_ref()),
                ResultValue::Timestamp(v) => Box::new(CqlTimestamp(*v)),
                ResultValue::Uuid(v) => Box::new(v),
                ResultValue::Inet(v) => Box::new(v),
                ResultValue::Date(v) => Box::new(CqlDate(*v)),
                ResultValue::Time(v) => Box::new(CqlTime(*v)),
                ResultValue::SmallInt(v) => Box::new(v),
                ResultValue::TinyInt(v) => Box::new(v),
                ResultValue::Varchar(v) => Box::new(v),
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
        tokio::time::timeout(
            Duration::from_secs(TIMEOUT),
            self.execute_batch_fallible_inner(queries.clone()),
        )
        .await
        .expect("timed out after executing cassandra batch for 10s")
    }

    pub async fn execute_batch_fallible_inner(
        &self,
        queries: Vec<String>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Datastax { session, .. } => {
                let mut batch = session.batch(BatchType::LOGGED);
                for query in queries {
                    batch
                        .add_statement(session.statement(query.as_str()))
                        .unwrap();
                }

                Self::process_datastax_response(batch.execute().await)
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
            Ok(response) => {
                let mut response_result = vec![];
                let mut iter = response.iter();
                while let Some(row) = iter.next() {
                    let mut row_result = vec![];
                    let mut iter = row.iter();
                    while let Some(value) = iter.next() {
                        row_result.push(ResultValue::new_from_cpp(value));
                    }
                    response_result.push(row_result)
                }
                Ok(response_result)
            }
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
                    code => todo!("Implement handling for scylla err: {code:?}"),
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
