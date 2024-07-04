use super::scylla::SessionScylla;
use super::{Compression, Consistency, PreparedQuery, ProtocolVersion, Tls};
use crate::connection::cassandra::ResultValue;
use crate::connection::java::{Jvm, Value};
use cdrs_tokio::frame::message_error::ErrorBody;
use cdrs_tokio::frame::message_result::ColType;
use std::net::IpAddr;

pub struct JavaConnection {
    jvm: Jvm,
    session: Value,
    pub schema_awaiter: Option<SessionScylla>,
}

impl JavaConnection {
    pub async fn new(
        contact_points: &str,
        port: u16,
        _compression: Option<Compression>,
        _tls: Option<Tls>,
        _protocol: Option<ProtocolVersion>,
    ) -> Self {
        // specify maven dep for java-driver-core-shaded and all of its dependencies since j4rs does not support dependency resolution
        // The list of dependencies can be found here: https://mvnrepository.com/artifact/org.apache.cassandra/java-driver-core-shaded/4.18.1
        // These are deployed to and loaded from a path like target/debug/jassets
        let jvm = Jvm::new(&[
            "org.apache.cassandra:java-driver-core-shaded:4.18.1",
            "com.datastax.oss:java-driver-shaded-guava:24.1-jre",
            "com.datastax.oss:native-protocol:1.5.1",
            "org.slf4j:slf4j-api:1.7.36",
            "org.slf4j:slf4j-simple:1.7.36",
            "com.typesafe:config:1.4.1",
            "com.github.jnr:jnr-posix:3.1.15",
            "org.reactivestreams:reactive-streams:1.0.3",
        ]);

        let session_builder =
            jvm.construct("com.datastax.oss.driver.api.core.CqlSessionBuilder", vec![]);

        for address in contact_points.split(',') {
            let point = jvm.construct(
                "java.net.InetSocketAddress",
                vec![jvm.new_string(address), jvm.new_int(port as i32)],
            );
            session_builder.call("addContactPoint", vec![point]);
        }
        let session = session_builder
            .call("withLocalDatacenter", vec![jvm.new_string("datacenter1")])
            .call("buildAsync", vec![])
            .call_async("toCompletableFuture", vec![])
            .await;

        JavaConnection {
            jvm,
            session,
            schema_awaiter: None,
        }
    }
    pub async fn execute_prepared_coordinator_node(
        &self,
        _prepared_query: &PreparedQuery,
        _values: &[ResultValue],
    ) -> IpAddr {
        todo!()
    }

    pub async fn execute_fallible(&self, query: &str) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        match self
            .session
            .call("executeAsync", vec![self.jvm.new_string(query)])
            .call_async_fallible("toCompletableFuture", vec![])
            .await
        {
            Ok(result) => {
                let types: Vec<ColType> = result
                    .cast("com.datastax.oss.driver.api.core.cql.AsyncResultSet")
                    .call("getColumnDefinitions", vec![])
                    .call("iterator", vec![])
                    .into_iter()
                    .map(|col_def| {
                        let code: i32 = col_def
                            .cast("com.datastax.oss.driver.api.core.cql.ColumnDefinition")
                            .call("getType", vec![])
                            .call("getProtocolCode", vec![])
                            .into_rust();
                        ColType::try_from(code as i16).unwrap()
                    })
                    .collect();

                let mut rows = vec![];
                for record in result.call("currentPage", vec![]).call("iterator", vec![]) {
                    let record = record.cast("com.datastax.oss.driver.api.core.cql.Row");
                    let row = types
                        .iter()
                        .enumerate()
                        .map(|(i, ty)| Self::java_value_to_rust(&self.jvm, i, &record, *ty))
                        .collect();

                    rows.push(row);
                }
                Ok(rows)
            }
            Err(err) => todo!("return ErrorBody {err:?}"),
        }
    }

    fn java_value_to_rust(jvm: &Jvm, i: usize, record: &Value, ty: ColType) -> ResultValue {
        let args = vec![jvm.new_int(i as i32)];
        let container_args = vec![jvm.new_int(i as i32)];
        match ty {
            ColType::Varchar => ResultValue::Varchar(record.call("getString", args).into_rust()),
            ColType::Ascii => ResultValue::Ascii(record.call("getString", args).into_rust()),
            ColType::Boolean => ResultValue::Boolean(record.call("getBool", args).into_rust()),
            ColType::Bigint => ResultValue::BigInt(record.call("getLong", args).into_rust()),
            ColType::Int => ResultValue::Int(record.call("getInt", args).into_rust()),
            ColType::Smallint => ResultValue::SmallInt(record.call("getShort", args).into_rust()),
            ColType::Tinyint => ResultValue::TinyInt(record.call("getByte", args).into_rust()),
            ColType::Blob => ResultValue::Blob({
                let bytes: Vec<i8> = record
                    .call("getByteBuffer", args)
                    .call("array", vec![])
                    .into_rust();

                bytes.into_iter().map(|x| x as u8).collect()
            }),
            ColType::Date => ResultValue::Date(
                record
                    .call("getLocalDate", args)
                    .call("toEpochDay", vec![])
                    .into_rust::<u32>()
                    // as per cassandra spec, dates are represented with epoch starting at 2**31
                    + 2u32.pow(31u32),
            ),
            ColType::Decimal => ResultValue::Decimal({
                let bytes: Vec<i8> = record
                    .call("getBytesUnsafe", args)
                    .call("array", vec![])
                    .into_rust();
                bytes.into_iter().map(|x| x as u8).collect()
            }),
            ColType::Double => ResultValue::Double(record.call("getDouble", args).into_rust()),
            ColType::Float => ResultValue::Float(record.call("getFloat", args).into_rust()),
            ColType::Timestamp => ResultValue::Timestamp(
                record
                    .call("getInstant", args)
                    .call("toEpochMilli", vec![])
                    .into_rust(),
            ),
            ColType::Inet => ResultValue::Inet({
                let string: String = record
                    .call("getInetAddress", args)
                    .call("toString", vec![])
                    .into_rust();
                string[1..]
                    .parse()
                    .map_err(|_| format!("Failed to parse IP {string}"))
                    .unwrap()
            }),
            ColType::Time => ResultValue::Time(
                record
                    .call("getLocalTime", args)
                    .call("toNanoOfDay", vec![])
                    .into_rust(),
            ),
            ColType::Timeuuid => ResultValue::TimeUuid({
                let string: String = record
                    .call("getUuid", args)
                    .call("toString", vec![])
                    .into_rust();
                string.parse().unwrap()
            }),
            ColType::Uuid => ResultValue::Uuid({
                let string: String = record
                    .call("getUuid", args)
                    .call("toString", vec![])
                    .into_rust();
                string.parse().unwrap()
            }),
            ColType::Varint => ResultValue::VarInt({
                let bytes: Vec<i8> = record
                    .call("getBytesUnsafe", args)
                    .call("array", vec![])
                    .into_rust();
                bytes.into_iter().map(|x| x as u8).collect()
            }),
            ColType::List => ResultValue::List(
                record
                    .call("getList", container_args)
                    .call("iterator", vec![])
                    .into_iter()
                    .map(|_| ResultValue::Any)
                    .collect(),
            ),
            ColType::Set => ResultValue::Set(Default::default()),
            ColType::Map => ResultValue::Map(Default::default()),
            ty => todo!("{ty}"),
        }
    }

    pub async fn execute_with_timestamp(
        &self,
        _query: &str,
        _timestamp: i64,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        todo!()
    }

    pub async fn prepare(&self, _query: &str) -> PreparedQuery {
        todo!()
    }

    pub async fn execute_prepared(
        &self,
        _prepared_query: &PreparedQuery,
        _values: &[ResultValue],
        _consistency: Consistency,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        todo!()
    }

    pub async fn execute_batch_fallible(
        &self,
        _queries: Vec<String>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        todo!()
    }
}
