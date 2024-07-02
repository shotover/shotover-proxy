use super::scylla::SessionScylla;
use super::{Compression, Consistency, PreparedQuery, ProtocolVersion, Tls};
use crate::connection::cassandra::ResultValue;
use cassandra_cpp::{
    BatchType, CassErrorCode, CassResult, Cluster, Consistency as ConsistencyCpp, Error, ErrorKind,
    LendingIterator, Session, Statement as StatementCpp,
};
use cdrs_tokio::frame::message_error::{ErrorBody, ErrorType};

pub use cassandra_cpp::{PreparedStatement as PreparedStatementCpp, Ssl as SslCpp};

pub struct CppConnection {
    pub session: Session,
    pub schema_awaiter: Option<SessionScylla>,
}

impl CppConnection {
    pub async fn new(
        contact_points: &str,
        port: u16,
        compression: Option<Compression>,
        tls: Option<Tls>,
        protocol: Option<ProtocolVersion>,
    ) -> Self {
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

        if let Some(Tls::Cpp(ssl)) = tls {
            cluster.set_ssl(ssl);
        }

        CppConnection {
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

    pub async fn execute_fallible(&self, query: &str) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        Self::process_datastax_response(self.session.execute(query).await)
    }

    pub async fn execute_with_timestamp(
        &self,
        query: &str,
        timestamp: i64,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let mut statement = self.session.statement(query);
        statement.set_timestamp(timestamp).unwrap();
        Self::process_datastax_response(statement.execute().await)
    }

    pub async fn prepare(&self, query: &str) -> PreparedQuery {
        PreparedQuery::Cpp(self.session.prepare(query).await.unwrap())
    }

    pub async fn execute_prepared(
        &self,
        prepared_query: &PreparedQuery,
        values: &[ResultValue],
        consistency: Consistency,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let mut statement = prepared_query.as_datastax().bind();
        for (i, value) in values.iter().enumerate() {
            Self::bind_statement_values_cpp(&mut statement, i, value);
        }

        statement.set_tracing(true).unwrap();
        statement
            .set_consistency(match consistency {
                Consistency::All => ConsistencyCpp::ALL,
                Consistency::One => ConsistencyCpp::ONE,
            })
            .unwrap();
        Self::process_datastax_response(statement.execute().await)
    }
    pub async fn execute_batch_fallible(
        &self,
        queries: Vec<String>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let mut batch = self.session.batch(BatchType::LOGGED);
        for query in queries {
            batch
                .add_statement(self.session.statement(query.as_str()))
                .unwrap();
        }

        Self::process_datastax_response(batch.execute().await)
    }

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
            value => todo!("Implement handling of {value:?} for Cpp"),
        };
    }

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
}
