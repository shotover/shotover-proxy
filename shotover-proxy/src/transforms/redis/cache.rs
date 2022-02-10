use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use tracing::info;

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::frame::cassandra::{CassandraOperation, CassandraResult};
use crate::frame::{CassandraFrame, Frame};
use crate::message::{ASTHolder, MessageDetails, MessageValue, Messages, QueryType};
use crate::transforms::chain::TransformChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, Wrapper,
};
use bytes::{BufMut, Bytes, BytesMut};
use cassandra_protocol::frame::Version;
use itertools::Itertools;
use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, Value};
use std::borrow::Borrow;

const TRUE: [u8; 1] = [0x1];
const FALSE: [u8; 1] = [0x0];

#[derive(Deserialize, Debug, Clone)]
pub struct RedisConfig {
    pub caching_schema: HashMap<String, TableCacheSchema>,
    pub chain: Vec<TransformsConfig>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TableCacheSchema {
    partition_key: Vec<String>,
    range_key: Vec<String>,
}

impl TableCacheSchema {
    #[cfg(test)]
    fn get_compound_key(&self) -> Vec<String> {
        [self.partition_key.as_slice(), self.range_key.as_slice()].concat()
    }
}

impl RedisConfig {
    pub async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::RedisCache(SimpleRedisCache {
            cache_chain: build_chain_from_config("cache_chain".to_string(), &self.chain, topics)
                .await?,
            caching_schema: self.caching_schema.clone(),
        }))
    }
}

#[derive(Clone)]
pub struct SimpleRedisCache {
    cache_chain: TransformChain,
    caching_schema: HashMap<String, TableCacheSchema>,
}

impl SimpleRedisCache {
    fn get_name(&self) -> &'static str {
        "SimpleRedisCache"
    }

    async fn get_or_update_from_cache(&mut self, mut messages: Messages) -> ChainResponse {
        let mut stream_ids = Vec::with_capacity(messages.len());
        for message in &mut messages {
            if let Frame::Cassandra(frame) = &message.original {
                stream_ids.push(frame.stream_id);
            }
            match &mut message.details {
                MessageDetails::Query(ref mut qm) => {
                    let table_name = qm.namespace.join(".");
                    let table_cache_schema = self
                        .caching_schema
                        .get(&table_name)
                        .ok_or_else(|| anyhow!("{} not a caching table", table_name))?;

                    let ast = qm
                        .ast
                        .as_ref()
                        .ok_or_else(|| anyhow!("No AST to convert query to cache query"))?
                        .clone();

                    qm.ast = Some(build_redis_ast_from_sql(
                        ast,
                        &qm.primary_key,
                        table_cache_schema,
                        &qm.query_values,
                    )?);
                }
                details => return Err(anyhow!("cannot fetch {:?} from cache", details)),
            };
            message.modified = true;
        }

        let mut messages = self
            .cache_chain
            .process_request(
                Wrapper::new_with_chain_name(messages, self.cache_chain.name.clone()),
                "clientdetailstodo".to_string(),
            )
            .await?;
        for message in &mut messages {
            info!("Received reply from redis cache {:?}", message);
            // TODO: Translate the redis reply into cassandra
            message.original = Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                operation: CassandraOperation::Result(CassandraResult::Void),
                stream_id: stream_ids.remove(0),
                tracing_id: None,
                warnings: vec![],
            });
            message.modified = true;
        }
        Ok(messages)
    }
}

// TODO: We don't need to do it this way and allocate another struct
struct ValueHelper(Value);

impl ValueHelper {
    fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            Value::Number(v, false) => v.as_bytes(),
            Value::SingleQuotedString(v) => v.as_bytes(),
            Value::NationalStringLiteral(v) => v.as_bytes(),
            Value::HexStringLiteral(v) => v.as_bytes(),
            Value::Boolean(v) => {
                if *v {
                    &TRUE
                } else {
                    &FALSE
                }
            }
            Value::Null => &[],
            _ => unreachable!(),
        }
    }
}

fn append_prefix_min(min: &mut Vec<u8>) {
    if min.is_empty() {
        min.push(b'[');
    } else {
        min.push(b':');
    }
}

fn append_prefix_max(max: &mut Vec<u8>) {
    if max.is_empty() {
        max.push(b']');
    } else {
        max.push(b':');
    }
}

fn build_zrangebylex_min_max_from_sql(
    expr: &Expr,
    pks: &[String],
    min: &mut Vec<u8>,
    max: &mut Vec<u8>,
) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // first check if this is a related to PK
            if let Expr::Identifier(i) = left.borrow() {
                if pks.iter().any(|v| *v == i.value) {
                    //Ignore this as we build the pk constraint elsewhere
                    return Ok(());
                }
            }

            match op {
                BinaryOperator::Gt => {
                    // we shift the value for Gt so that it works with other GtEq operators
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let mut minrv = Vec::from(vh.as_bytes());
                        let last_byte = minrv.last_mut().unwrap();
                        *last_byte += 1;

                        append_prefix_min(min);
                        min.extend(minrv.iter());
                    }
                }
                BinaryOperator::Lt => {
                    // we shift the value for Lt so that it works with other LtEq operators
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let mut maxrv = Vec::from(vh.as_bytes());
                        let last_byte = maxrv.last_mut().unwrap();
                        *last_byte -= 1;

                        append_prefix_max(max);
                        max.extend(maxrv.iter());
                    }
                }
                BinaryOperator::GtEq => {
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let minrv = Vec::from(vh.as_bytes());

                        append_prefix_min(min);
                        min.extend(minrv.iter());
                    }
                }
                BinaryOperator::LtEq => {
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let maxrv = Vec::from(vh.as_bytes());

                        append_prefix_max(max);
                        max.extend(maxrv.iter());
                    }
                }
                BinaryOperator::Eq => {
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let vh_bytes = vh.as_bytes();

                        append_prefix_min(min);
                        append_prefix_max(max);
                        min.extend(vh_bytes.iter());
                        max.extend(vh_bytes.iter());
                    }
                }
                BinaryOperator::And => {
                    build_zrangebylex_min_max_from_sql(left, pks, min, max)?;
                    build_zrangebylex_min_max_from_sql(right, pks, min, max)?;
                }
                _ => {
                    return Err(anyhow!("Couldn't build query"));
                }
            }
        }
        _ => {
            return Err(anyhow!("Couldn't build query"));
        }
    }
    Ok(())
}

fn build_redis_ast_from_sql(
    mut ast: ASTHolder,
    primary_key_values: &HashMap<String, MessageValue>,
    table_cache_schema: &TableCacheSchema,
    query_values: &Option<HashMap<String, MessageValue>>,
) -> Result<ASTHolder> {
    match &mut ast {
        ASTHolder::SQL(sql) => match &mut **sql {
            Statement::Query(q) => match &mut q.body {
                SetExpr::Select(s) if s.selection.is_some() => {
                    let expr = s.selection.as_mut().unwrap();
                    let mut min: Vec<u8> = Vec::new();
                    let mut max: Vec<u8> = Vec::new();

                    build_zrangebylex_min_max_from_sql(
                        expr,
                        &table_cache_schema.partition_key,
                        &mut min,
                        &mut max,
                    )?;

                    let min = if min.is_empty() {
                        Bytes::from_static(b"-")
                    } else {
                        Bytes::from(min)
                    };
                    let max = if max.is_empty() {
                        Bytes::from_static(b"+")
                    } else {
                        Bytes::from(max)
                    };

                    let pk = table_cache_schema
                        .partition_key
                        .iter()
                        .map(|k| primary_key_values.get(k).unwrap())
                        .fold(BytesMut::new(), |mut acc, v| {
                            acc.extend(v.clone().into_str_bytes());
                            acc
                        });

                    let commands_buffer = vec![
                        MessageValue::Bytes("ZRANGEBYLEX".into()),
                        MessageValue::Bytes(pk.freeze()),
                        MessageValue::Bytes(min),
                        MessageValue::Bytes(max),
                    ];
                    Ok(ASTHolder::Commands(MessageValue::List(commands_buffer)))
                }
                expr => Err(anyhow!("Can't build query from expr: {}", expr)),
            },
            Statement::Insert { .. } | Statement::Update { .. } => {
                let mut commands_buffer: Vec<MessageValue> =
                    vec![MessageValue::Bytes("ZADD".into())];

                let pk = table_cache_schema
                    .partition_key
                    .iter()
                    .map(|k| primary_key_values.get(k).unwrap())
                    .fold(BytesMut::new(), |mut acc, v| {
                        acc.extend(v.clone().into_str_bytes());
                        acc
                    });
                commands_buffer.push(MessageValue::Bytes(pk.freeze()));

                let clustering = table_cache_schema
                    .range_key
                    .iter()
                    .map(|k| primary_key_values.get(k).unwrap())
                    .fold(BytesMut::new(), |mut acc, v| {
                        acc.extend(v.clone().into_str_bytes());
                        acc
                    });

                let values = query_values
                    .as_ref()
                    .ok_or_else(|| anyhow!("query_values is None"))?
                    .iter()
                    .filter_map(|(p, v)| {
                        if !table_cache_schema.partition_key.contains(p)
                            && !table_cache_schema.range_key.contains(p)
                        {
                            Some(v)
                        } else {
                            None
                        }
                    })
                    .collect_vec();

                for v in values {
                    commands_buffer.push(MessageValue::Bytes(Bytes::from("0")));
                    let mut value = clustering.clone();
                    if !value.is_empty() {
                        value.put_u8(b':');
                    }
                    value.extend(v.clone().into_str_bytes());
                    commands_buffer.push(MessageValue::Bytes(value.freeze()));
                }

                Ok(ASTHolder::Commands(MessageValue::List(commands_buffer)))
            }
            statement => Err(anyhow!("Cant build query from statement: {}", statement)),
        },
        ASTHolder::Commands(_) => Ok(ast),
    }
}

#[async_trait]
impl Transform for SimpleRedisCache {
    fn validate(&self) -> Vec<String> {
        let mut errors = self
            .cache_chain
            .validate()
            .iter()
            .map(|x| format!("  {x}"))
            .collect::<Vec<String>>();

        if !errors.is_empty() {
            errors.insert(0, format!("{}:", self.get_name()));
        }

        errors
    }

    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        let mut updates = false;

        for m in &mut message_wrapper.messages {
            if let Frame::Cassandra(CassandraFrame {
                operation: CassandraOperation::Query { .. },
                ..
            }) = &m.original
            {
                m.generate_message_details_query();
                if m.get_query_type() == QueryType::Write {
                    updates = true;
                    break;
                }
            }
        }

        // If there are no write queries (all queries are reads) we can use the cache
        if !updates {
            match self
                .get_or_update_from_cache(message_wrapper.messages.clone())
                .await
            {
                Ok(cr) => Ok(cr),
                Err(e) => {
                    tracing::error!("failed to fetch from cache: {:?}", e);
                    message_wrapper.call_next_transform().await
                }
            }
        } else {
            let (_cache_res, upstream) = tokio::join!(
                self.get_or_update_from_cache(message_wrapper.messages.clone()),
                message_wrapper.call_next_transform()
            );
            upstream
        }
    }
}

#[cfg(test)]
mod test {
    use crate::codec::cassandra::CassandraCodec;
    use crate::codec::redis::{DecodeType, RedisCodec};
    use crate::message::{ASTHolder, MessageDetails};
    use crate::message::{IntSize as MessageIntSize, MessageValue};
    use crate::transforms::chain::TransformChain;
    use crate::transforms::debug::printer::DebugPrinter;
    use crate::transforms::null::Null;
    use crate::transforms::redis::cache::{
        build_redis_ast_from_sql, SimpleRedisCache, TableCacheSchema,
    };
    use crate::transforms::{Transform, Transforms};
    use bytes::BytesMut;
    use itertools::Itertools;
    use std::collections::HashMap;
    use tokio_util::codec::Decoder;

    fn build_query(
        query_string: &str,
        pk_col_map: &HashMap<String, Vec<String>>,
    ) -> (ASTHolder, Option<HashMap<String, MessageValue>>) {
        let res = CassandraCodec::parse_query_string(query_string, pk_col_map);
        (ASTHolder::SQL(Box::new(res.ast.unwrap())), res.colmap)
    }

    fn build_redis_query_frame(query: &str) -> ASTHolder {
        let mut codec = RedisCodec::new(DecodeType::Query);

        let mut final_command_bytes: BytesMut = build_redis_string(query).as_str().into();
        let mut messages = codec.decode(&mut final_command_bytes).unwrap().unwrap();
        for message in &mut messages {
            message.generate_message_details_query();
        }

        match messages.remove(0).details {
            MessageDetails::Query(qm) => qm.ast.unwrap(),
            details => panic!(
                "Exepected message details to be a query but was: {:?}",
                details
            ),
        }
    }

    fn build_redis_string(query: &str) -> String {
        let mut command_buffer = String::new();

        let tokens = query.split_ascii_whitespace().collect_vec();
        command_buffer.push_str(&format!("*{}\r\n", tokens.len()));

        for token in tokens {
            command_buffer.push_str(&format!("${}\r\n", token.len()));
            command_buffer.push_str(&format!("{token}\r\n"));
        }

        command_buffer
    }

    #[test]
    fn test_build_redis_query_string() {
        assert_eq!(
            "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n".to_string(),
            build_redis_string("LLEN mylist")
        );
    }

    #[test]
    fn equal_test() {
        let mut pks = HashMap::new();
        pks.insert(
            "z".to_string(),
            MessageValue::Integer(1, MessageIntSize::I32),
        );

        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map = HashMap::new();
        pk_col_map.insert("foo".to_string(), table_cache_schema.get_compound_key());

        let (ast, query_values) = build_query(
            "SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965",
            &pk_col_map,
        );

        let query =
            build_redis_ast_from_sql(ast, &pks, &table_cache_schema, &query_values).unwrap();

        let expected = build_redis_query_frame("ZRANGEBYLEX 1 [123:965 ]123:965");

        assert_eq!(expected, query);
    }

    #[test]
    fn insert_simple_test() {
        let mut pks = HashMap::new();
        pks.insert(
            "z".to_string(),
            MessageValue::Integer(1, MessageIntSize::I32),
        );

        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map = HashMap::new();
        pk_col_map.insert("foo".to_string(), table_cache_schema.get_compound_key());

        let (ast, query_values) =
            build_query("INSERT INTO foo (z, v) VALUES (1, 123)", &pk_col_map);

        let query =
            build_redis_ast_from_sql(ast, &pks, &table_cache_schema, &query_values).unwrap();

        let expected = build_redis_query_frame("ZADD 1 0 123");

        assert_eq!(expected, query);
    }

    #[test]
    fn insert_simple_clustering_test() {
        let mut pks = HashMap::new();
        pks.insert(
            "z".to_string(),
            MessageValue::Integer(1, MessageIntSize::I32),
        );
        pks.insert("c".to_string(), MessageValue::Strings("yo".to_string()));

        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec!["c".to_string()],
        };

        let mut pk_col_map = HashMap::new();
        pk_col_map.insert("foo".to_string(), table_cache_schema.get_compound_key());

        let (ast, query_values) = build_query(
            "INSERT INTO foo (z, c, v) VALUES (1, 'yo' , 123)",
            &pk_col_map,
        );

        let query =
            build_redis_ast_from_sql(ast, &pks, &table_cache_schema, &query_values).unwrap();

        let expected = build_redis_query_frame("ZADD 1 0 yo:123");

        assert_eq!(expected, query);
    }

    #[test]
    fn update_simple_clustering_test() {
        let mut pks = HashMap::new();
        pks.insert(
            "z".to_string(),
            MessageValue::Integer(1, MessageIntSize::I32),
        );
        pks.insert("c".to_string(), MessageValue::Strings("yo".to_string()));

        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec!["c".to_string()],
        };

        let mut pk_col_map = HashMap::new();
        pk_col_map.insert("foo".to_string(), table_cache_schema.get_compound_key());

        let (ast, query_values) =
            build_query("UPDATE foo SET c = 'yo', v = 123 WHERE z = 1", &pk_col_map);

        let query =
            build_redis_ast_from_sql(ast, &pks, &table_cache_schema, &query_values).unwrap();

        let expected = build_redis_query_frame("ZADD 1 0 yo:123");

        assert_eq!(expected, query);
    }

    #[test]
    fn check_deterministic_order_test() {
        let mut pks = HashMap::new();
        pks.insert(
            "z".to_string(),
            MessageValue::Integer(1, MessageIntSize::I32),
        );
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map = HashMap::new();
        pk_col_map.insert("foo".to_string(), table_cache_schema.get_compound_key());

        let (ast, query_values) = build_query(
            "SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965",
            &pk_col_map,
        );

        let query_one =
            build_redis_ast_from_sql(ast, &pks, &table_cache_schema, &query_values).unwrap();

        let (ast, query_values) = build_query(
            "SELECT * FROM foo WHERE y = 965 AND z = 1 AND x = 123",
            &pk_col_map,
        );

        let query_two =
            build_redis_ast_from_sql(ast, &pks, &table_cache_schema, &query_values).unwrap();

        // Semantically databases treat the order of AND clauses differently, Cassandra however requires clustering key predicates be in order
        // So here we will just expect the order is correct in the query. TODO: we may need to revisit this as support for other databases is added
        assert_ne!(query_one, query_two);
    }

    #[test]
    fn range_exclusive_test() {
        let mut pks = HashMap::new();
        pks.insert(
            "z".to_string(),
            MessageValue::Integer(1, MessageIntSize::I32),
        );
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map = HashMap::new();
        pk_col_map.insert("foo".to_string(), table_cache_schema.get_compound_key());

        let (ast, query_values) = build_query(
            "SELECT * FROM foo WHERE z = 1 AND x > 123 AND x < 999",
            &pk_col_map,
        );

        let query =
            build_redis_ast_from_sql(ast, &pks, &table_cache_schema, &query_values).unwrap();

        let expected = build_redis_query_frame("ZRANGEBYLEX 1 [124 ]998");

        assert_eq!(expected, query);
    }

    #[test]
    fn range_inclusive_test() {
        let mut pks = HashMap::new();
        pks.insert(
            "z".to_string(),
            MessageValue::Integer(1, MessageIntSize::I32),
        );
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map = HashMap::new();
        pk_col_map.insert("foo".to_string(), table_cache_schema.get_compound_key());

        let (ast, query_values) = build_query(
            "SELECT * FROM foo WHERE z = 1 AND x >= 123 AND x <= 999",
            &pk_col_map,
        );

        let query =
            build_redis_ast_from_sql(ast, &pks, &table_cache_schema, &query_values).unwrap();

        let expected = build_redis_query_frame("ZRANGEBYLEX 1 [123 ]999");

        assert_eq!(expected, query);
    }

    #[test]
    fn single_pk_only_test() {
        let mut pks = HashMap::new();
        pks.insert(
            "z".to_string(),
            MessageValue::Integer(1, MessageIntSize::I32),
        );
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map = HashMap::new();
        pk_col_map.insert("foo".to_string(), table_cache_schema.get_compound_key());

        let (ast, query_values) = build_query("SELECT * FROM foo WHERE z = 1", &pk_col_map);

        let query =
            build_redis_ast_from_sql(ast, &pks, &table_cache_schema, &query_values).unwrap();

        let expected = build_redis_query_frame("ZRANGEBYLEX 1 - +");

        assert_eq!(expected, query);
    }

    #[test]
    fn compound_pk_only_test() {
        let mut pks = HashMap::new();
        pks.insert(
            "z".to_string(),
            MessageValue::Integer(1, MessageIntSize::I32),
        );
        pks.insert(
            "y".to_string(),
            MessageValue::Integer(2, MessageIntSize::I32),
        );
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string(), "y".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map = HashMap::new();
        pk_col_map.insert("foo".to_string(), table_cache_schema.get_compound_key());

        let (ast, query_values) =
            build_query("SELECT * FROM foo WHERE z = 1 AND y = 2", &pk_col_map);

        let query =
            build_redis_ast_from_sql(ast, &pks, &table_cache_schema, &query_values).unwrap();

        let expected = build_redis_query_frame("ZRANGEBYLEX 12 - +");

        assert_eq!(expected, query);
    }

    #[test]
    fn open_range_test() {
        let mut pks = HashMap::new();
        pks.insert(
            "z".to_string(),
            MessageValue::Integer(1, MessageIntSize::I32),
        );
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map = HashMap::new();
        pk_col_map.insert("foo".to_string(), table_cache_schema.get_compound_key());

        let (ast, query_values) =
            build_query("SELECT * FROM foo WHERE z = 1 AND x >= 123", &pk_col_map);

        let query =
            build_redis_ast_from_sql(ast, &pks, &table_cache_schema, &query_values).unwrap();

        let expected = build_redis_query_frame("ZRANGEBYLEX 1 [123 +");

        assert_eq!(expected, query);

        let mut pk_col_map = HashMap::new();
        pk_col_map.insert("foo".to_string(), table_cache_schema.get_compound_key());

        let (ast, query_values) =
            build_query("SELECT * FROM foo WHERE z = 1 AND x <= 123", &pk_col_map);

        let query =
            build_redis_ast_from_sql(ast, &pks, &table_cache_schema, &query_values).unwrap();

        let expected = build_redis_query_frame("ZRANGEBYLEX 1 - ]123");

        assert_eq!(expected, query);
    }

    #[tokio::test]
    async fn test_validate_invalid_chain() {
        let chain = TransformChain::new(vec![], "test-chain".to_string());
        let transform = SimpleRedisCache {
            cache_chain: chain,
            caching_schema: HashMap::new(),
        };

        assert_eq!(
            transform.validate(),
            vec![
                "SimpleRedisCache:",
                "  test-chain:",
                "    Chain cannot be empty"
            ]
        );
    }

    #[tokio::test]
    async fn test_validate_valid_chain() {
        let chain = TransformChain::new(
            vec![
                Transforms::DebugPrinter(DebugPrinter::new()),
                Transforms::DebugPrinter(DebugPrinter::new()),
                Transforms::Null(Null::default()),
            ],
            "test-chain".to_string(),
        );
        let transform = SimpleRedisCache {
            cache_chain: chain,
            caching_schema: HashMap::new(),
        };

        assert_eq!(transform.validate(), Vec::<String>::new());
    }
}
