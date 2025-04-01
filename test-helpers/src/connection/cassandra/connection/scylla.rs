use super::{Compression, Consistency, PreparedQuery, ProtocolVersion, Tls};
use crate::connection::cassandra::ResultValue;
use cassandra_protocol::frame::message_error::ErrorType;
use cdrs_tokio::frame::message_error::ErrorBody;
use scylla::client::execution_profile::ExecutionProfile;
pub use scylla::client::session::Session as SessionScylla;
pub use scylla::client::session_builder::SessionBuilder as SessionBuilderScylla;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use scylla::frame::types::Consistency as ScyllaConsistency;
use scylla::response::query_result::QueryResult;
use scylla::serialize::value::SerializeValue;
use scylla::statement::Statement;
use scylla::statement::batch::Batch;
pub use scylla::statement::prepared::PreparedStatement as PreparedStatementScylla;
use scylla::value::{CqlDate, CqlDecimal, CqlTime, CqlTimestamp, Row};
use std::net::IpAddr;
use std::time::Duration;

pub struct ScyllaConnection {
    session: SessionScylla,
    pub schema_awaiter: Option<SessionScylla>,
}

impl ScyllaConnection {
    pub async fn new(
        contact_points: &str,
        port: u16,
        compression: Option<Compression>,
        tls: Option<Tls>,
        protocol: Option<ProtocolVersion>,
    ) -> Self {
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
                Compression::Snappy => scylla::frame::Compression::Snappy,
                Compression::Lz4 => scylla::frame::Compression::Lz4,
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
            builder = builder.tls_context(Some(ssl_context));
        }

        let session = builder.build().await.unwrap();

        ScyllaConnection {
            session,
            schema_awaiter: None,
        }
    }

    pub async fn execute_prepared_coordinator_node(
        &self,
        prepared_query: &PreparedQuery,
        values: &[ResultValue],
    ) -> IpAddr {
        let statement = prepared_query.as_scylla();
        let values = Self::build_values_scylla(values);

        let response = self
            .session
            .execute_unpaged(statement, values)
            .await
            .unwrap();
        let tracing_id = response.tracing_id().unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        self.session
            .get_tracing_info(&tracing_id)
            .await
            .unwrap()
            .coordinator
            .unwrap()
    }

    pub async fn execute_batch_fallible(
        &self,
        queries: Vec<String>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let mut values = vec![];
        let mut batch: Batch = Default::default();
        for query in queries {
            batch.append_statement(query.as_str());
            values.push(());
        }

        Self::process_scylla_response(self.session.batch(&batch, values).await)
    }

    pub async fn execute_fallible(&self, query: &str) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        Self::process_scylla_response(self.session.query_unpaged(query, ()).await)
    }

    pub async fn execute_with_timestamp(
        &self,
        query: &str,
        timestamp: i64,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let mut statement = Statement::new(query);
        statement.set_timestamp(Some(timestamp));
        Self::process_scylla_response(self.session.query_unpaged(statement, ()).await)
    }

    pub async fn prepare(&self, query: &str) -> PreparedQuery {
        let mut prepared = self.session.prepare(query).await.unwrap();
        prepared.set_tracing(true);
        PreparedQuery::Scylla(prepared)
    }

    pub async fn execute_prepared(
        &self,
        prepared_query: &PreparedQuery,
        values: &[ResultValue],
        consistency: Consistency,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        // clone to avoid changing default consistency
        let mut statement = prepared_query.as_scylla().clone();
        statement.set_consistency(match consistency {
            Consistency::All => ScyllaConsistency::All,
            Consistency::One => ScyllaConsistency::One,
        });
        let values = Self::build_values_scylla(values);

        Self::process_scylla_response(self.session.execute_unpaged(&statement, values).await)
    }

    fn process_scylla_response(
        response: Result<QueryResult, ExecutionError>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        match response {
            Ok(value) => {
                if value.is_rows() {
                    Ok(value
                        .into_rows_result()
                        .unwrap()
                        .rows::<Row>()
                        .unwrap()
                        .map(|x| {
                            x.unwrap()
                                .columns
                                .into_iter()
                                .map(ResultValue::new_from_scylla)
                                .collect()
                        })
                        .collect())
                } else {
                    value.result_not_rows().unwrap();
                    Ok(vec![])
                }
            }
            Err(ExecutionError::LastAttemptError(RequestAttemptError::DbError(code, message))) => {
                Err(ErrorBody {
                    ty: match code {
                        DbError::Overloaded => ErrorType::Overloaded,
                        DbError::ServerError => ErrorType::Server,
                        DbError::Invalid => ErrorType::Invalid,
                        code => todo!("Implement handling for scylla err: {code:?}"),
                    },
                    message,
                })
            }
            Err(err) => panic!("Unexpected scylla error: {err:?}"),
        }
    }

    // TODO: lets return Vec<CqlValue> instead, as it provides better guarantees for correctness
    fn build_values_scylla(values: &[ResultValue]) -> Vec<Box<dyn SerializeValue + '_>> {
        values
            .iter()
            .map(|v| match v {
                ResultValue::Int(v) => Box::new(v) as Box<dyn SerializeValue>,
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
}
