use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame, RedisFrame, CQL};
use crate::message::{Message, MessageValue, Messages, QueryType};
use crate::transforms::chain::TransformChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, Wrapper,
};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use cassandra_protocol::frame::Version;
use itertools::Itertools;
use serde::Deserialize;
use sqlparser::ast::{Assignment, BinaryOperator, Expr, Ident, Query, SetExpr, Statement, Value};
use std::borrow::Borrow;
use std::collections::HashMap;
use tracing::info;

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
            if let Some(Frame::Cassandra(frame)) = message.frame() {
                stream_ids.push(frame.stream_id);
            } else {
                bail!("Failed to parse cassandra message");
            }
            if let Some(table_name) = message.namespace().map(|x| x.join(".")) {
                *message = match message.frame() {
                    Some(Frame::Cassandra(CassandraFrame {
                        operation: CassandraOperation::Query { query, .. },
                        ..
                    })) => {
                        let table_cache_schema = self
                            .caching_schema
                            .get(&table_name)
                            .ok_or_else(|| anyhow!("{table_name} not a caching table"))?;

                        Message::from_frame(Frame::Redis(build_redis_ast_from_sql(
                            query,
                            table_cache_schema,
                        )?))
                    }
                    message => bail!("cannot fetch {message:?} from cache"),
                };
                message.invalidate_cache();
            } else {
                bail!("Failed to get message namespace");
            }
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
            *message = Message::from_frame(Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                operation: CassandraOperation::Result(CassandraResult::Void),
                stream_id: stream_ids.remove(0),
                tracing_id: None,
                warnings: vec![],
            }));
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
    ast: &CQL,
    table_cache_schema: &TableCacheSchema,
) -> Result<RedisFrame> {
    if let CQL::Parsed(ast) = ast {
        match &ast[0] {
            Statement::Query(q) => match &q.body {
                SetExpr::Select(s) if s.selection.is_some() => {
                    let expr = s.selection.as_ref().unwrap();
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
                        .map(|k| get_equal_value_from_expr(expr, k))
                        .fold(BytesMut::new(), |mut acc, v| {
                            if let Some(v) = v {
                                acc.extend(MessageValue::from(v).into_str_bytes());
                            }
                            acc
                        });

                    let commands_buffer = vec![
                        RedisFrame::BulkString("ZRANGEBYLEX".into()),
                        RedisFrame::BulkString(pk.freeze()),
                        RedisFrame::BulkString(min),
                        RedisFrame::BulkString(max),
                    ];
                    Ok(RedisFrame::Array(commands_buffer))
                }
                expr => Err(anyhow!("Can't build query from expr: {}", expr)),
            },
            Statement::Insert {
                source, columns, ..
            } => {
                let query_values = get_values_from_insert(columns, source);

                let pk = table_cache_schema
                    .partition_key
                    .iter()
                    .map(|k| query_values.get(k.as_str()).unwrap())
                    .fold(BytesMut::new(), |mut acc, v| {
                        acc.extend(MessageValue::from(*v).into_str_bytes());
                        acc
                    });

                insert_or_update(table_cache_schema, query_values, pk)
            }
            Statement::Update {
                assignments,
                selection,
                ..
            } => {
                let query_values = get_values_from_update(assignments);

                let pk = table_cache_schema
                    .partition_key
                    .iter()
                    .map(|k| get_equal_value_from_expr(selection.as_ref().unwrap(), k).unwrap())
                    .fold(BytesMut::new(), |mut acc, v| {
                        acc.extend(MessageValue::from(v).into_str_bytes());
                        acc
                    });

                insert_or_update(table_cache_schema, query_values, pk)
            }
            statement => Err(anyhow!("Cant build query from statement: {}", statement)),
        }
    } else {
        Err(anyhow!("cannot use unparsed CQL"))
    }
}

fn insert_or_update(
    table_cache_schema: &TableCacheSchema,
    query_values: HashMap<String, &Value>,
    pk: BytesMut,
) -> Result<RedisFrame> {
    let mut commands_buffer: Vec<RedisFrame> = vec![
        RedisFrame::BulkString("ZADD".into()),
        RedisFrame::BulkString(pk.freeze()),
    ];

    let clustering = table_cache_schema
        .range_key
        .iter()
        .map(|k| query_values.get(k.as_str()).unwrap())
        .fold(BytesMut::new(), |mut acc, v| {
            acc.extend(MessageValue::from(*v).into_str_bytes());
            acc
        });

    let values = query_values
        .iter()
        .filter_map(|(p, v)| {
            if table_cache_schema.partition_key.iter().all(|x| x != p)
                && table_cache_schema.range_key.iter().all(|x| x != p)
            {
                Some(MessageValue::from(*v))
            } else {
                None
            }
        })
        .collect_vec();

    for v in values {
        commands_buffer.push(RedisFrame::BulkString(Bytes::from_static(b"0")));
        let mut value = clustering.clone();
        if !value.is_empty() {
            value.put_u8(b':');
        }
        value.extend(v.clone().into_str_bytes());
        commands_buffer.push(RedisFrame::BulkString(value.freeze()));
    }

    Ok(RedisFrame::Array(commands_buffer))
}

fn get_values_from_insert<'a>(
    columns: &'a [Ident],
    source: &'a Query,
) -> HashMap<String, &'a Value> {
    let mut map = HashMap::new();
    let mut columns_iter = columns.iter();
    if let SetExpr::Values(v) = &source.body {
        for value in &v.0 {
            for ex in value {
                if let Expr::Value(v) = ex {
                    if let Some(c) = columns_iter.next() {
                        // TODO: We should be able to avoid allocation here
                        map.insert(c.value.to_string(), v);
                    }
                }
            }
        }
    }
    map
}

fn get_values_from_update(assignments: &[Assignment]) -> HashMap<String, &Value> {
    let mut map = HashMap::new();
    for assignment in assignments {
        if let Expr::Value(v) = &assignment.value {
            map.insert(assignment.id.iter().map(|x| &x.value).join("."), v);
        }
    }
    map
}

fn get_equal_value_from_expr<'a>(expr: &'a Expr, find_identifier: &str) -> Option<&'a Value> {
    if let Expr::BinaryOp { left, op, right } = expr {
        match op {
            BinaryOperator::And => get_equal_value_from_expr(left, find_identifier)
                .or_else(|| get_equal_value_from_expr(right, find_identifier)),
            BinaryOperator::Eq => {
                if let Expr::Identifier(i) = left.borrow() {
                    if i.value == find_identifier {
                        if let Expr::Value(v) = right.borrow() {
                            Some(v)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    } else {
        None
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
            if let Some(Frame::Cassandra(CassandraFrame {
                operation: CassandraOperation::Query { .. },
                ..
            })) = m.frame()
            {
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
    use crate::frame::cassandra::CQL;
    use crate::frame::RedisFrame;
    use crate::transforms::chain::TransformChain;
    use crate::transforms::debug::printer::DebugPrinter;
    use crate::transforms::null::Null;
    use crate::transforms::redis::cache::{
        build_redis_ast_from_sql, SimpleRedisCache, TableCacheSchema,
    };
    use crate::transforms::{Transform, Transforms};
    use bytes::Bytes;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;

    fn build_query(query_string: &str) -> CQL {
        CQL::Parsed(Parser::parse_sql(&GenericDialect {}, query_string).unwrap())
    }

    #[test]
    fn equal_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965");

        let query = build_redis_ast_from_sql(&ast, &table_cache_schema).unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZRANGEBYLEX")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"[123:965")),
            RedisFrame::BulkString(Bytes::from_static(b"]123:965")),
        ]);

        assert_eq!(expected, query);
    }

    #[test]
    fn insert_simple_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let ast = build_query("INSERT INTO foo (z, v) VALUES (1, 123)");

        let query = build_redis_ast_from_sql(&ast, &table_cache_schema).unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZADD")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"0")),
            RedisFrame::BulkString(Bytes::from_static(b"123")),
        ]);

        assert_eq!(expected, query);
    }

    #[test]
    fn insert_simple_clustering_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec!["c".to_string()],
        };

        let ast = build_query("INSERT INTO foo (z, c, v) VALUES (1, 'yo' , 123)");
        let query = build_redis_ast_from_sql(&ast, &table_cache_schema).unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZADD")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"0")),
            RedisFrame::BulkString(Bytes::from_static(b"yo:123")),
        ]);

        assert_eq!(expected, query);
    }

    #[test]
    fn update_simple_clustering_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec!["c".to_string()],
        };

        let ast = build_query("UPDATE foo SET c = 'yo', v = 123 WHERE z = 1");

        let query = build_redis_ast_from_sql(&ast, &table_cache_schema).unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZADD")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"0")),
            RedisFrame::BulkString(Bytes::from_static(b"yo:123")),
        ]);

        assert_eq!(expected, query);
    }

    #[test]
    fn check_deterministic_order_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965");

        let query_one = build_redis_ast_from_sql(&ast, &table_cache_schema).unwrap();

        let ast = build_query("SELECT * FROM foo WHERE y = 965 AND z = 1 AND x = 123");

        let query_two = build_redis_ast_from_sql(&ast, &table_cache_schema).unwrap();

        // Semantically databases treat the order of AND clauses differently, Cassandra however requires clustering key predicates be in order
        // So here we will just expect the order is correct in the query. TODO: we may need to revisit this as support for other databases is added
        assert_ne!(query_one, query_two);
    }

    #[test]
    fn range_exclusive_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x > 123 AND x < 999");

        let query = build_redis_ast_from_sql(&ast, &table_cache_schema).unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZRANGEBYLEX")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"[124")),
            RedisFrame::BulkString(Bytes::from_static(b"]998")),
        ]);

        assert_eq!(expected, query);
    }

    #[test]
    fn range_inclusive_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x >= 123 AND x <= 999");

        let query = build_redis_ast_from_sql(&ast, &table_cache_schema).unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZRANGEBYLEX")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"[123")),
            RedisFrame::BulkString(Bytes::from_static(b"]999")),
        ]);

        assert_eq!(expected, query);
    }

    #[test]
    fn single_pk_only_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1");

        let query = build_redis_ast_from_sql(&ast, &table_cache_schema).unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZRANGEBYLEX")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"-")),
            RedisFrame::BulkString(Bytes::from_static(b"+")),
        ]);

        assert_eq!(expected, query);
    }

    #[test]
    fn compound_pk_only_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string(), "y".to_string()],
            range_key: vec![],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND y = 2");

        let query = build_redis_ast_from_sql(&ast, &table_cache_schema).unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZRANGEBYLEX")),
            RedisFrame::BulkString(Bytes::from_static(b"12")),
            RedisFrame::BulkString(Bytes::from_static(b"-")),
            RedisFrame::BulkString(Bytes::from_static(b"+")),
        ]);

        assert_eq!(expected, query);
    }

    #[test]
    fn open_range_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x >= 123");

        let query = build_redis_ast_from_sql(&ast, &table_cache_schema).unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZRANGEBYLEX")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"[123")),
            RedisFrame::BulkString(Bytes::from_static(b"+")),
        ]);

        assert_eq!(expected, query);

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x <= 123");

        let query = build_redis_ast_from_sql(&ast, &table_cache_schema).unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZRANGEBYLEX")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"-")),
            RedisFrame::BulkString(Bytes::from_static(b"]123")),
        ]);

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
