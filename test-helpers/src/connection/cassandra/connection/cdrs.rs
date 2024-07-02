use super::{Compression, Consistency, PreparedQuery, ProtocolVersion, Tls, TIMEOUT};
use crate::connection::cassandra::ResultValue;
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
    query::BatchQueryBuilder,
    query_values,
    transport::TransportTcp,
};
use scylla::Session as SessionScylla;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

pub use cdrs_tokio::query::PreparedQuery as CdrsTokioPreparedQuery;

pub type CdrsTokioSessionInstance = CdrsTokioSession<
    TransportTcp,
    TcpConnectionManager,
    TopologyAwareLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

pub struct CdrsConnection {
    pub session: CdrsTokioSessionInstance,
    pub schema_awaiter: Option<SessionScylla>,
}

impl CdrsConnection {
    pub async fn new(
        contact_points: &str,
        port: u16,
        compression: Option<Compression>,
        _tls: Option<Tls>,
        protocol: Option<ProtocolVersion>,
    ) -> Self {
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

        let mut session_builder =
            TcpSessionBuilder::new(TopologyAwareLoadBalancingStrategy::new(None, true), config);

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
        CdrsConnection {
            session,
            schema_awaiter: None,
        }
    }

    pub async fn execute_batch_fallible(
        &self,
        queries: Vec<String>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let mut builder = BatchQueryBuilder::new();
        for query in queries {
            builder = builder.add_query(query, query_values!());
        }
        let batch = builder.build().unwrap();

        Self::process_cdrs_response(self.session.batch(batch).await)
    }

    pub async fn execute_with_timestamp(
        &self,
        query: &str,
        timestamp: i64,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let statement_params = StatementParamsBuilder::new()
            .with_timestamp(timestamp)
            .build();

        let response = self
            .session
            .query_with_params(query, statement_params)
            .await;
        Self::process_cdrs_response(response)
    }

    pub async fn prepare(&self, query: &str) -> PreparedQuery {
        let query = self.session.prepare(query).await.unwrap();
        PreparedQuery::CdrsTokio(query)
    }

    pub async fn execute_prepared_coordinator_node(
        &self,
        prepared_query: &PreparedQuery,
        values: &[ResultValue],
    ) -> IpAddr {
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

        let response = self
            .session
            .exec_with_params(statement, &params)
            .await
            .unwrap();

        let tracing_id = response.tracing_id.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // let cassandra finish writing to the tracing table
        let row = self
            .session
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

    pub async fn execute_prepared(
        &self,
        prepared_query: &PreparedQuery,
        values: &[ResultValue],
        consistency: Consistency,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
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

        Self::process_cdrs_response(self.session.exec_with_params(statement, &params).await)
    }
    pub async fn execute_fallible(&self, query: &str) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        Self::process_cdrs_response(self.session.query(query).await)
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
}
