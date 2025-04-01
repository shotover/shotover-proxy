use super::result_value::ResultValue;
use cassandra_protocol::frame::message_error::ErrorBody;
use cdrs::CdrsTokioPreparedQuery;
use cdrs::{CdrsConnection, CdrsTokioSessionInstance};
#[cfg(feature = "cassandra-cpp-driver-tests")]
use cpp::{CppConnection, PreparedStatementCpp, SslCpp};
use java::{JavaConnection, PreparedStatementJava};
use openssl::ssl::{SslContext, SslMethod};
use pretty_assertions::assert_eq;
use scylla::{PreparedStatementScylla, ScyllaConnection, SessionBuilderScylla};
#[cfg(feature = "cassandra-cpp-driver-tests")]
use std::fs::read_to_string;
use std::net::IpAddr;
use std::time::Duration;

mod cdrs;
#[cfg(feature = "cassandra-cpp-driver-tests")]
mod cpp;
mod java;
mod scylla;

const TIMEOUT: Duration = Duration::from_secs(10);

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

    // TODO: move this logic into the driver specific modules
    pub async fn build(self) -> CassandraConnection {
        let tls = if let Some(ca_cert_path) = self.ca_cert_path {
            match self.driver {
                #[cfg(feature = "cassandra-cpp-driver-tests")]
                CassandraDriver::Cpp => {
                    let ca_cert = read_to_string(ca_cert_path).unwrap();
                    let mut ssl = SslCpp::default();
                    SslCpp::add_trusted_cert(&mut ssl, &ca_cert).unwrap();

                    Some(Tls::Cpp(ssl))
                }
                // TODO actually implement TLS for cdrs-tokio
                CassandraDriver::Cdrs => todo!(),
                // TODO actually implement TLS for java
                CassandraDriver::Java => todo!(),
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
    Cpp(PreparedStatementCpp),
    CdrsTokio(CdrsTokioPreparedQuery),
    Scylla(PreparedStatementScylla),
    Java(PreparedStatementJava),
}

impl PreparedQuery {
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    pub fn as_datastax(&self) -> &PreparedStatementCpp {
        match self {
            PreparedQuery::Cpp(p) => p,
            _ => panic!("Not PreparedQuery::Cpp"),
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

    pub fn as_java(&self) -> &PreparedStatementJava {
        match self {
            PreparedQuery::Java(j) => j,
            _ => panic!("Not PreparedQuery::Java"),
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
    Cpp(SslCpp),
    Scylla(SslContext),
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum CassandraDriver {
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    Cpp,
    Cdrs,
    Scylla,
    Java,
}

pub enum CassandraConnection {
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    Cpp(CppConnection),
    Cdrs(CdrsConnection),
    Scylla(ScyllaConnection),
    Java(JavaConnection),
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
            CassandraDriver::Cpp => CassandraConnection::Cpp(
                CppConnection::new(contact_points, port, compression, tls, protocol).await,
            ),
            CassandraDriver::Cdrs => CassandraConnection::Cdrs(
                CdrsConnection::new(contact_points, port, compression, tls, protocol).await,
            ),
            CassandraDriver::Scylla => CassandraConnection::Scylla(
                ScyllaConnection::new(contact_points, port, compression, tls, protocol).await,
            ),
            CassandraDriver::Java => CassandraConnection::Java(
                JavaConnection::new(contact_points, port, compression, tls, protocol).await,
            ),
        }
    }

    pub fn as_cdrs(&self) -> &CdrsTokioSessionInstance {
        match self {
            Self::Cdrs(connection) => &connection.session,
            _ => panic!("Not CdrsTokio"),
        }
    }

    pub fn is(&self, drivers: &[CassandraDriver]) -> bool {
        match self {
            Self::Cdrs { .. } => drivers.contains(&CassandraDriver::Cdrs),
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Cpp { .. } => drivers.contains(&CassandraDriver::Cpp),
            Self::Scylla { .. } => drivers.contains(&CassandraDriver::Scylla),
            Self::Java { .. } => drivers.contains(&CassandraDriver::Java),
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
            Self::Cpp(connection) => &mut connection.schema_awaiter,
            Self::Cdrs(connection) => &mut connection.schema_awaiter,
            Self::Scylla(connection) => &mut connection.schema_awaiter,
            Self::Java(connection) => &mut connection.schema_awaiter,
        };

        *schema_awaiter = Some(
            SessionBuilderScylla::new()
                .known_node(direct_node)
                .user("cassandra", "cassandra")
                .tls_context(context)
                .build()
                .await
                .unwrap(),
        );
    }

    pub async fn await_schema_agreement(&self) {
        let schema_awaiter = match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Cpp(connection) => &connection.schema_awaiter,
            Self::Cdrs(connection) => &connection.schema_awaiter,
            Self::Scylla(connection) => &connection.schema_awaiter,
            Self::Java(connection) => &connection.schema_awaiter,
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
        tokio::time::timeout(TIMEOUT, self.execute_fallible_inner(query))
            .await
            .unwrap_or_else(|_| panic!("The CQL query: {query}\nTimed out after 10s"))
    }

    pub async fn execute_fallible_inner(
        &self,
        query: &str,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let result = match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Cpp(connection) => connection.execute_fallible(query).await,
            Self::Cdrs(connection) => connection.execute_fallible(query).await,
            Self::Scylla(connection) => connection.execute_fallible(query).await,
            Self::Java(connection) => connection.execute_fallible(query).await,
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
        tokio::time::timeout(TIMEOUT, self.execute_with_timestamp_inner(query, timestamp))
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
            Self::Cpp(connection) => connection.execute_with_timestamp(query, timestamp).await,
            Self::Cdrs(connection) => connection.execute_with_timestamp(query, timestamp).await,
            Self::Scylla(connection) => connection.execute_with_timestamp(query, timestamp).await,
            Self::Java(connection) => connection.execute_with_timestamp(query, timestamp).await,
        };

        let query = query.to_uppercase();
        let query = query.trim();
        if query.starts_with("CREATE") || query.starts_with("ALTER") || query.starts_with("DROP") {
            self.await_schema_agreement().await;
        }

        result
    }

    pub async fn prepare(&self, query: &str) -> PreparedQuery {
        tokio::time::timeout(TIMEOUT, self.prepare_inner(query))
            .await
            .unwrap_or_else(|_| panic!("Preparing the CQL query: {query}\nTimed out after 10s"))
    }

    pub async fn prepare_inner(&self, query: &str) -> PreparedQuery {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Cpp(connection) => connection.prepare(query).await,
            Self::Cdrs(connection) => connection.prepare(query).await,
            Self::Scylla(connection) => connection.prepare(query).await,
            Self::Java(connection) => connection.prepare(query).await,
        }
    }

    pub async fn execute_prepared_coordinator_node(
        &self,
        prepared_query: &PreparedQuery,
        values: &[ResultValue],
    ) -> IpAddr {
        tokio::time::timeout(
            TIMEOUT,
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
            Self::Cpp(_connection) => todo!(),
            Self::Cdrs(connection) => {
                connection
                    .execute_prepared_coordinator_node(prepared_query, values)
                    .await
            }
            Self::Scylla(connection) => {
                connection
                    .execute_prepared_coordinator_node(prepared_query, values)
                    .await
            }
            Self::Java(connection) => {
                connection
                    .execute_prepared_coordinator_node(prepared_query, values)
                    .await
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
            TIMEOUT,
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
            Self::Cpp(connection) => {
                connection
                    .execute_prepared(prepared_query, values, consistency)
                    .await
            }
            Self::Cdrs(connection) => {
                connection
                    .execute_prepared(prepared_query, values, consistency)
                    .await
            }
            Self::Scylla(connection) => {
                connection
                    .execute_prepared(prepared_query, values, consistency)
                    .await
            }
            Self::Java(connection) => {
                connection
                    .execute_prepared(prepared_query, values, consistency)
                    .await
            }
        }
    }

    pub async fn execute_batch_fallible(
        &self,
        queries: Vec<String>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        tokio::time::timeout(TIMEOUT, self.execute_batch_fallible_inner(queries.clone()))
            .await
            .expect("timed out after executing cassandra batch for 10s")
    }

    pub async fn execute_batch_fallible_inner(
        &self,
        queries: Vec<String>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        match self {
            #[cfg(feature = "cassandra-cpp-driver-tests")]
            Self::Cpp(connection) => connection.execute_batch_fallible(queries).await,
            Self::Cdrs(connection) => connection.execute_batch_fallible(queries).await,
            Self::Scylla(connection) => connection.execute_batch_fallible(queries).await,
            Self::Java(connection) => connection.execute_batch_fallible(queries).await,
        }
    }

    pub async fn execute_batch(&self, queries: Vec<String>) {
        let result = self.execute_batch_fallible(queries).await.unwrap();
        assert_eq!(result.len(), 0, "Batches should never return results");
    }
}
