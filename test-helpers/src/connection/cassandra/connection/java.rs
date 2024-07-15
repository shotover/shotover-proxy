use super::scylla::SessionScylla;
use super::{Compression, Consistency, PreparedQuery, ProtocolVersion, Tls};
use crate::connection::cassandra::ResultValue;
use crate::connection::java::{Jvm, Value};
use cdrs_tokio::frame::message_error::ErrorBody;
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
        compression: Option<Compression>,
        tls: Option<Tls>,
        protocol: Option<ProtocolVersion>,
    ) -> Self {
        if tls.is_some() {
            todo!("Java driver TLS not yet implemented");
        }
        if compression.is_some() {
            todo!("Java driver compression not yet implemented");
        }
        if protocol.is_some() {
            todo!("Java driver protocol not yet implemented");
        }

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
        prepared_query: &PreparedQuery,
        values: &[ResultValue],
    ) -> IpAddr {
        let _result = self
            .execute_prepared(prepared_query, values, Consistency::All)
            .await
            .unwrap();
        todo!()
    }

    pub async fn execute_fallible(&self, query: &str) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        self.execute(self.jvm.new_string(query)).await
    }

    /// Due to java function overloading this method can execute both a prepared statement a simple string query.
    async fn execute(&self, statement: Value) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        match self
            .session
            .call("executeAsync", vec![statement])
            .call_async_fallible("toCompletableFuture", vec![])
            .await
        {
            Ok(result) => {
                let types: Vec<DataType> = result
                    .cast("com.datastax.oss.driver.api.core.cql.AsyncResultSet")
                    .call("getColumnDefinitions", vec![])
                    .call("iterator", vec![])
                    .into_iter()
                    .map(|col_def| {
                        DataType(
                            col_def
                                .cast("com.datastax.oss.driver.api.core.cql.ColumnDefinition")
                                .call("getType", vec![]),
                        )
                    })
                    .collect();

                let mut rows = vec![];
                for record in result.call("currentPage", vec![]).call("iterator", vec![]) {
                    let record = record.cast("com.datastax.oss.driver.api.core.cql.Row");
                    let row = types
                        .iter()
                        .enumerate()
                        .map(|(i, ty)| {
                            let value = record.call("getObject", vec![self.jvm.new_int(i as i32)]);
                            let raw_bytes =
                                record.call("getBytesUnsafe", vec![self.jvm.new_int(i as i32)]);
                            Self::java_value_to_rust(value, &raw_bytes, ty)
                        })
                        .collect();

                    rows.push(row);
                }
                Ok(rows)
            }
            Err(err) => todo!("return ErrorBody {err:?}"),
        }
    }

    fn java_value_to_rust(value: Value, raw_bytes: &Value, ty: &DataType) -> ResultValue {
        match ty.col_type() {
            ColType::Ascii => ResultValue::Ascii(value.cast("java.lang.String").into_rust()),
            ColType::Varchar => ResultValue::Varchar(value.cast("java.lang.String").into_rust()),
            ColType::Boolean => ResultValue::Boolean(value.cast("java.lang.Boolean").into_rust()),
            ColType::Bigint => ResultValue::BigInt(value.cast("java.lang.Long").into_rust()),
            ColType::Int => ResultValue::Int(value.cast("java.lang.Integer").into_rust()),
            ColType::Smallint => ResultValue::SmallInt(value.cast("java.lang.Short").into_rust()),
            ColType::Tinyint => ResultValue::TinyInt(value.cast("java.lang.Byte").into_rust()),
            ColType::Double => ResultValue::Double(value.cast("java.lang.Double").into_rust()),
            ColType::Float => ResultValue::Float(value.cast("java.lang.Float").into_rust()),
            ColType::Decimal => ResultValue::Decimal({
                let bytes: Vec<i8> = raw_bytes.call("array", vec![]).cast("[B").into_rust();
                bytes.into_iter().map(|x| x as u8).collect()
            }),
            ColType::Blob => ResultValue::Blob({
                let value = value.cast("java.nio.ByteBuffer");
                let bytes: Vec<i8> = value.call("array", vec![]).cast("[B").into_rust();
                let start: i32 = value.call("arrayOffset", vec![]).into_rust();
                let limit: i32 = value.call("limit", vec![]).into_rust();

                bytes[start as usize..start as usize + limit as usize]
                    .iter()
                    .map(|x| *x as u8)
                    .collect()
            }),
            ColType::Date => ResultValue::Date(
                (value
                .cast("java.time.LocalDate")
                .call("toEpochDay", vec![])
                .into_rust::<i64>()
                // as per cassandra spec, dates are represented with epoch starting at 2**31
                + 2i64.pow(31)) as u32,
            ),
            ColType::Timestamp => ResultValue::Timestamp(
                value
                    .cast("java.time.Instant")
                    .call("toEpochMilli", vec![])
                    .into_rust(),
            ),
            ColType::Time => ResultValue::Time(
                value
                    .cast("java.time.LocalTime")
                    .call("toNanoOfDay", vec![])
                    .into_rust(),
            ),
            ColType::Timeuuid => ResultValue::TimeUuid({
                let string: String = value
                    .cast("java.util.UUID")
                    .call("toString", vec![])
                    .into_rust();
                string.parse().unwrap()
            }),
            ColType::Uuid => ResultValue::Uuid({
                let string: String = value
                    .cast("java.util.UUID")
                    .call("toString", vec![])
                    .into_rust();
                string.parse().unwrap()
            }),
            ColType::Inet => ResultValue::Inet({
                let string: String = value
                    .cast("java.net.InetAddress")
                    .call("toString", vec![])
                    .into_rust();
                string[1..]
                    .parse()
                    .map_err(|_| format!("Failed to parse IP {string}"))
                    .unwrap()
            }),
            ColType::Varint => ResultValue::VarInt({
                let bytes: Vec<i8> = raw_bytes.call("array", vec![]).cast("[B").into_rust();
                bytes.into_iter().map(|x| x as u8).collect()
            }),
            ColType::List => ResultValue::List(
                value
                    .cast("java.util.List")
                    .call("iterator", vec![])
                    .into_iter()
                    // TODO: no way to provide a correct raw_bytes value here,
                    // need to change tests to not use raw_bytes instead.
                    .map(|value| Self::java_value_to_rust(value, raw_bytes, &ty.element_col_type()))
                    .collect(),
            ),
            ColType::Set => ResultValue::Set(
                value
                    .cast("java.util.Set")
                    .call("iterator", vec![])
                    .into_iter()
                    // TODO: no way to provide a correct raw_bytes value here,
                    // need to change tests to not use raw_bytes instead.
                    .map(|value| Self::java_value_to_rust(value, raw_bytes, &ty.element_col_type()))
                    .collect(),
            ),
            ColType::Map => ResultValue::Map({
                let (ty_key, ty_value) = ty.key_value_type();
                value
                    .cast("java.util.Map")
                    .call("entrySet", vec![])
                    .call("iterator", vec![])
                    .into_iter()
                    .map(|value| {
                        let value = value.cast("java.util.Map$Entry");
                        (
                            Self::java_value_to_rust(
                                value.call("getKey", vec![]),
                                // TODO: no way to provide a correct raw_bytes value here,
                                // need to change tests to not use raw_bytes instead.
                                raw_bytes,
                                &ty_key,
                            ),
                            Self::java_value_to_rust(
                                value.call("getValue", vec![]),
                                // TODO: no way to provide a correct raw_bytes value here,
                                // need to change tests to not use raw_bytes instead.
                                raw_bytes,
                                &ty_value,
                            ),
                        )
                    })
                    .collect()
            }),
            ColType::Vector => ResultValue::Vector({
                value
                    .cast("com.datastax.oss.driver.api.core.data.CqlVector")
                    .call("iterator", vec![])
                    .into_iter()
                    // TODO: no way to provide a correct raw_bytes value here,
                    // need to change tests to not use raw_bytes instead.
                    .map(|value| Self::java_value_to_rust(value, raw_bytes, &ty.element_col_type()))
                    .collect()
            }),
            ty => todo!("{ty:?}"),
        }
    }

    pub async fn execute_with_timestamp(
        &self,
        query: &str,
        timestamp: i64,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        self.execute(
            self.jvm
                .construct(
                    "com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder",
                    vec![self.jvm.new_string(query)],
                )
                .call("setQueryTimestamp", vec![self.jvm.new_long(timestamp)])
                .call("build", vec![]),
        )
        .await
    }

    pub async fn prepare(&self, query: &str) -> PreparedQuery {
        PreparedQuery::Java(PreparedStatementJava(
            self.session
                .call("prepareAsync", vec![self.jvm.new_string(query)])
                .call_async("toCompletableFuture", vec![])
                .await,
        ))
    }

    fn values_to_java(&self, values: &[ResultValue]) -> Vec<Value> {
        values
            .iter()
            .map(|x| {
                match x {
                    ResultValue::TinyInt(x) => self.jvm.new_byte_object(*x),
                    ResultValue::SmallInt(x) => self.jvm.new_short_object(*x),
                    ResultValue::Int(x) => self.jvm.new_int_object(*x),
                    ResultValue::BigInt(x) => self.jvm.new_long_object(*x),
                    ResultValue::Boolean(x) => self.jvm.new_bool_object(*x),
                    ResultValue::Float(x) => self.jvm.new_float_object(x.0),
                    ResultValue::Double(x) => self.jvm.new_double_object(x.0),
                    ResultValue::Timestamp(x) => self.jvm.call_static(
                        "java.time.Instant",
                        "ofEpochMilli",
                        vec![self.jvm.new_long(*x)],
                    ),
                    ResultValue::Date(x) => self.jvm.call_static(
                        "java.time.LocalDate",
                        "ofEpochDay",
                        vec![self.jvm.new_long(*x as i64 - 2i64.pow(31))],
                    ),
                    ResultValue::Time(x) => self.jvm.call_static(
                        "java.time.LocalTime",
                        "ofNanoOfDay",
                        vec![self.jvm.new_long(*x)],
                    ),
                    ResultValue::Decimal(_) => panic!(r#"Java driver does support decimal.
However in order to use it we need to store ResultValue::Decimal as unscaledval + scale rather than a bunch of bytes.
Then we can call BigDecimal(BigInteger unscaledVal, int scale) constructor"#),
                    ResultValue::Uuid(x) | ResultValue::TimeUuid(x) => self.jvm.call_static(
                        "java.util.UUID",
                        "fromString",
                        vec![self.jvm.new_string(&x.to_string())],
                    ),
                    ResultValue::Inet(x) => self.jvm.call_static(
                        "java.net.InetAddress",
                        "getByName",
                        vec![self.jvm.new_string(&x.to_string())],
                    ),
                    ResultValue::Blob(x) => self.jvm.call_static(
                        "java.nio.ByteBuffer",
                        "wrap",
                        vec![self
                            .jvm
                            // WARN: If we ever try to pass in a large amount of binary data this will be terribly slow.
                            //       For now its fine and j4rs doesnt currently provide a better way.
                            .new_array(
                                "byte",
                                x.iter().map(|x| self.jvm.new_byte(*x as i8)).collect(),
                            )],
                    ),
                    ResultValue::Ascii(x) | ResultValue::Varchar(x) => self.jvm.new_string(x),
                    value => todo!("Implement handling of {value:?} for java"),
                }
                .cast("java.lang.Object")
            })
            .collect()
    }

    pub async fn execute_prepared(
        &self,
        prepared_query: &PreparedQuery,
        values: &[ResultValue],
        consistency: Consistency,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let consistency_level_java = self
            .jvm
            .class("com.datastax.oss.driver.api.core.ConsistencyLevel");

        let values = self.values_to_java(values);

        let statement = prepared_query
            .as_java()
            .0
            .call(
                "boundStatementBuilder",
                vec![self.jvm.new_array("java.lang.Object", values)],
            )
            .call(
                "setConsistencyLevel",
                vec![match consistency {
                    Consistency::One => consistency_level_java.field("ONE"),
                    Consistency::All => consistency_level_java.field("ALL"),
                }],
            )
            .call("build", vec![]);
        self.execute(statement).await
    }

    pub async fn execute_batch_fallible(
        &self,
        queries: Vec<String>,
    ) -> Result<Vec<Vec<ResultValue>>, ErrorBody> {
        let batch_type = self
            .jvm
            .class("com.datastax.oss.driver.api.core.cql.BatchType");

        let queries: Vec<Value> = queries
            .iter()
            .map(|query| {
                self.jvm
                    .construct(
                        "com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder",
                        vec![self.jvm.new_string(query)],
                    )
                    .call("build", vec![])
                    .cast("com.datastax.oss.driver.api.core.cql.BatchableStatement")
            })
            .collect();
        let batch_builder = self.jvm.construct(
            "com.datastax.oss.driver.api.core.cql.BatchStatementBuilder",
            vec![batch_type.field("LOGGED")],
        );
        for query in queries {
            batch_builder.call("addStatement", vec![query]);
        }
        let batch = batch_builder.call("build", vec![]);
        self.execute(batch).await
    }
}

#[derive(Debug)]
pub struct PreparedStatementJava(Value);

/// Wrapper around https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/type/DataType.html
struct DataType(Value);

impl DataType {
    fn col_type(&self) -> ColType {
        let code: i32 = self.0.call("getProtocolCode", vec![]).into_rust();
        match code {
            0x00 => {
                if self
                    .0
                    .cast_fallible("com.datastax.oss.driver.api.core.type.VectorType")
                    .is_ok()
                {
                    ColType::Vector
                } else {
                    ColType::Custom
                }
            }
            0x01 => ColType::Ascii,
            0x02 => ColType::Bigint,
            0x03 => ColType::Blob,
            0x04 => ColType::Boolean,
            0x05 => ColType::Counter,
            0x06 => ColType::Decimal,
            0x07 => ColType::Double,
            0x08 => ColType::Float,
            0x09 => ColType::Int,
            0x0B => ColType::Timestamp,
            0x0C => ColType::Uuid,
            0x0D => ColType::Varchar,
            0x0E => ColType::Varint,
            0x0F => ColType::Timeuuid,
            0x10 => ColType::Inet,
            0x11 => ColType::Date,
            0x12 => ColType::Time,
            0x13 => ColType::Smallint,
            0x14 => ColType::Tinyint,
            0x15 => ColType::Duration,
            0x20 => ColType::List,
            0x21 => ColType::Map,
            0x22 => ColType::Set,
            0x30 => ColType::Udt,
            0x31 => ColType::Tuple,
            0x80 => ColType::Varchar,
            code => panic!("unknown type code {code:?}"),
        }
    }

    fn element_col_type(&self) -> DataType {
        DataType(
            self.0
                .cast("com.datastax.oss.driver.api.core.type.ContainerType")
                .call("getElementType", vec![]),
        )
    }

    fn key_value_type(&self) -> (DataType, DataType) {
        let map_type = self.0.cast("com.datastax.oss.driver.api.core.type.MapType");
        (
            DataType(map_type.call("getKeyType", vec![])),
            DataType(map_type.call("getValueType", vec![])),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ColType {
    Custom,
    Ascii,
    Bigint,
    Blob,
    Boolean,
    Counter,
    Decimal,
    Double,
    Float,
    Int,
    Timestamp,
    Uuid,
    Varchar,
    Varint,
    Timeuuid,
    Inet,
    Date,
    Time,
    Smallint,
    Tinyint,
    Duration,
    List,
    Map,
    Set,
    Udt,
    Tuple,
    Vector,
}
