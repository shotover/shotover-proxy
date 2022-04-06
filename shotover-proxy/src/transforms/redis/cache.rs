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
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{Operand, RelationOperator};
use itertools::Itertools;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};

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
    pub async fn get_transform(&self, topics: &TopicHolder) -> Result<Transforms> {
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

    async fn get_or_update_from_cache(
        &mut self,
        mut messages_cass_request: Messages,
    ) -> ChainResponse {
        // This function is a little hard to follow, so here's an overview.
        // We have 4 vecs of messages, each vec can be considered its own stage of processing.
        // 1. messages_cass_request:
        //     * the cassandra requests that the function receives.
        // 2. messages_redis_request:
        //     * each query in each cassandra request in messages_cass_request is transformed into a redis request
        //     * each request gets sent to the redis server
        // 3. messages_redis_response:
        //     * the redis responses we get back from the server
        // 4. messages_cass_response:
        //     * Well messages_cass_response is what we would have called this, in reality we reuse the messages_cass_request vec because its cheaper.
        //     * To create each response we go through each request in messages_cass_request:
        //         + if the request is a CassandraOperation::Batch then we consume a message from messages_redis_response for each query in the batch
        //             - if any of the messages are errors then generate a cassandra ERROR otherwise generate a VOID RESULT.
        //                  - we can get away with this because batches can only contain INSERT/UPDATE/DELETE and therefore always contain either an ERROR or a VOID RESULT
        //         + if the request is a CassandraOperation::Query then we consume a single message from messages_redis_response converting it to a cassandra response
        //     * These are the cassandra responses that we return from the function.

        let mut messages_redis_request = Vec::with_capacity(messages_cass_request.len());
        for cass_request in &mut messages_cass_request {
            match cass_request.frame() {
                Some(Frame::Cassandra(frame)) => {
                    for query in frame.operation.queries()? {
                        if let Some(table_name) = CQL::get_table_name(query) {
                            let table_cache_schema = self
                                .caching_schema
                                .get(table_name)
                                .ok_or_else(|| anyhow!("{table_name} not a caching table"))?;

                            messages_redis_request.push(Message::from_frame(Frame::Redis(
                                build_redis_ast_from_cql3(query, table_cache_schema)?,
                            )));
                        }
                    }
                }
                message => bail!("cannot fetch {message:?} from cache"),
            }
        }

        let messages_redis_response = self
            .cache_chain
            .process_request(
                Wrapper::new_with_chain_name(messages_redis_request, self.cache_chain.name.clone()),
                "clientdetailstodo".to_string(),
            )
            .await?;

        // Replace cass_request messages with cassandra responses in place.
        // We reuse the vec like this to save allocations.
        let mut messages_redis_response_iter = messages_redis_response.into_iter();
        for cass_request in &mut messages_cass_request {
            let mut redis_responses = vec![];
            if let Some(Frame::Cassandra(frame)) = cass_request.frame() {
                if let Ok(queries) = frame.operation.queries() {
                    for _query in queries {
                        redis_responses.push(messages_redis_response_iter.next());
                    }
                }
            }

            // TODO: Translate the redis_responses into a cassandra result
            *cass_request = Message::from_frame(Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                operation: CassandraOperation::Result(CassandraResult::Void),
                stream_id: cass_request.stream_id().unwrap(),
                tracing_id: None,
                warnings: vec![],
            }));
        }
        Ok(messages_cass_request)
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

fn operand_to_bytes(operand: &Operand) -> Vec<u8> {
    let message_value = MessageValue::from(operand);
    let bytes: cassandra_protocol::types::value::Bytes = message_value.into();
    bytes.into_inner()
}

fn build_zrangebylex_min_max_from_sql(
    operator: &RelationOperator,
    operand: &Operand,
    min: &mut Vec<u8>,
    max: &mut Vec<u8>,
) -> Result<()> {
    let mut bytes = operand_to_bytes(operand);
    match operator {
        RelationOperator::LessThan => {
            let last_byte = bytes.last_mut().unwrap();
            *last_byte -= 1;

            append_prefix_max(max);
            max.extend(bytes.iter());
        }
        RelationOperator::LessThanOrEqual => {
            append_prefix_max(max);
            max.extend(bytes.iter());
        }

        RelationOperator::Equal => {
            append_prefix_min(min);
            append_prefix_max(max);
            min.extend(bytes.iter());
            max.extend(bytes.iter());
        }
        RelationOperator::GreaterThanOrEqual => {
            append_prefix_min(min);
            min.extend(bytes.iter());
        }
        RelationOperator::GreaterThan => {
            let last_byte = bytes.last_mut().unwrap();
            *last_byte += 1;
            append_prefix_min(min);
            min.extend(bytes.iter());
        }
        // should "IN"" be converted to an "or" "eq" combination
        RelationOperator::NotEqual
        | RelationOperator::In
        | RelationOperator::Contains
        | RelationOperator::ContainsKey
        | RelationOperator::IsNot => {
            return Err(anyhow!("Couldn't build query"));
        }
    }
    Ok(())
}

fn build_redis_ast_from_cql3(
    statement: &CassandraStatement,
    table_cache_schema: &TableCacheSchema,
) -> Result<RedisFrame> {
    match statement {
        CassandraStatement::Select(select) => {
            if select.filtering || !select.columns.is_empty() || !select.where_clause.is_empty() {
                Err(anyhow!("Can't build query from expr: {}", select))
            } else {
                let mut min: Vec<u8> = Vec::new();
                let mut max: Vec<u8> = Vec::new();

                // extract the partition and range operands
                // fail if any are missing
                let mut partition_segments: HashMap<&str, &Operand> = HashMap::new();
                let mut range_segments: HashMap<&str, (&RelationOperator, &Operand)> =
                    HashMap::new();

                for relation_element in &select.where_clause {
                    if let Operand::Column(column_name) = &relation_element.obj {
                        // name has to be in partition or range key.
                        if table_cache_schema.partition_key.contains(&column_name) {
                            partition_segments.insert(&column_name, &relation_element.value);
                        } else if table_cache_schema.range_key.contains(&column_name) {
                            range_segments.insert(
                                &column_name,
                                (&relation_element.oper, &relation_element.value),
                            );
                        } else {
                            return Err(anyhow!(
                                "Couldn't build query- column {} is not in the key",
                                column_name
                            ));
                        }
                    }
                }
                let mut skipping = false;
                for column_name in &table_cache_schema.range_key {
                    if let Some((operator, operand)) = range_segments.get(column_name.as_str()) {
                        if skipping {
                            // we skipped an earlier column so this is an error.
                            return Err(anyhow!(
                                "Columns in the middle of the range key were skipped"
                            ));
                        }
                        if let Err(e) = build_zrangebylex_min_max_from_sql(
                            &operator, &operand, &mut min, &mut max,
                        ) {
                            return Err(e);
                        }
                    } else {
                        // once we skip a range key column we have to skip all the rest so set a flag.
                        skipping = true;
                    }
                }
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

                let mut partition_key = BytesMut::new();
                for column_name in &table_cache_schema.partition_key {
                    if let Some(operand) = partition_segments.get(column_name.as_str()) {
                        partition_key.extend(operand_to_bytes(operand).iter());
                    } else {
                        return Err(anyhow!("partition column {} missing", column_name));
                    }
                }

                let commands_buffer = vec![
                    RedisFrame::BulkString("ZRANGEBYLEX".into()),
                    RedisFrame::BulkString(partition_key.freeze()),
                    RedisFrame::BulkString(min),
                    RedisFrame::BulkString(max),
                ];
                Ok(RedisFrame::Array(commands_buffer))
            }
        }
        CassandraStatement::Insert(insert) => {
            let query_values = insert.get_value_map();
            add_query_values(table_cache_schema, query_values)
        }
        CassandraStatement::Update(update) => {
            let mut query_values: BTreeMap<&str, &Operand> = BTreeMap::new();
            for assignment_element in &update.assignments {
                if assignment_element.operator.is_some() {
                    return Err(anyhow!("Update has calculations in values"));
                }
                if assignment_element.name.idx.is_some() {
                    return Err(anyhow!("Update has indexed columns"));
                }
                query_values.insert(
                    assignment_element.name.column.as_str(),
                    &assignment_element.value,
                );
            }
            add_query_values(table_cache_schema, query_values)
        }
        statement => Err(anyhow!("Cant build query from statement: {}", statement)),
    }
}

fn add_query_values(
    table_cache_schema: &TableCacheSchema,
    query_values: BTreeMap<&str, &Operand>,
) -> Result<RedisFrame> {
    let mut partition_key = BytesMut::new();
    for column_name in &table_cache_schema.partition_key {
        if let Some(operand) = query_values.get(column_name.as_str()) {
            partition_key.extend(operand_to_bytes(operand).iter());
        } else {
            return Err(anyhow!("partition column {} missing", column_name));
        }
    }

    let mut clustering = BytesMut::new();
    for column_name in &table_cache_schema.range_key {
        if let Some(operand) = query_values.get(column_name.as_str()) {
            clustering.extend(operand_to_bytes(operand).iter());
        } else {
            return Err(anyhow!("range column {} missing", column_name));
        }
    }

    let mut commands_buffer: Vec<RedisFrame> = vec![
        RedisFrame::BulkString("ZADD".into()),
        RedisFrame::BulkString(partition_key.freeze()),
    ];

    let values = query_values
        .iter()
        .filter_map(|(column_name, value)| {
            if !table_cache_schema
                .partition_key
                .contains(&column_name.to_string())
                && !table_cache_schema
                    .range_key
                    .contains(&column_name.to_string())
            {
                Some(value)
            } else {
                None
            }
        })
        .collect_vec();

    for operand in values {
        commands_buffer.push(RedisFrame::BulkString(Bytes::from_static(b"0")));
        let mut value = clustering.clone();
        if !value.is_empty() {
            value.put_u8(b':');
        }
        value.extend(operand_to_bytes(operand).iter());
        commands_buffer.push(RedisFrame::BulkString(value.freeze()));
    }

    Ok(RedisFrame::Array(commands_buffer))
}

#[async_trait]
impl Transform for SimpleRedisCache {
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
}

#[cfg(test)]
mod test {
    use crate::frame::{RedisFrame, CQL};
    use crate::transforms::chain::TransformChain;
    use crate::transforms::debug::printer::DebugPrinter;
    use crate::transforms::null::Null;
    use crate::transforms::redis::cache::{
        build_redis_ast_from_cql3, SimpleRedisCache, TableCacheSchema,
    };
    use crate::transforms::{Transform, Transforms};
    use bytes::Bytes;
    use cql3_parser::cassandra_statement::CassandraStatement;
    use std::collections::HashMap;

    fn build_query(query_string: &str) -> CassandraStatement {
        let cql = CQL::parse_from_string(query_string);
        assert!(!cql.has_error);
        cql.statement
    }

    #[test]
    fn equal_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965");

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema).unwrap();

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

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema).unwrap();

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
        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema).unwrap();

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

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema).unwrap();

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

        let query_one = build_redis_ast_from_cql3(&ast, &table_cache_schema).unwrap();

        let ast = build_query("SELECT * FROM foo WHERE y = 965 AND z = 1 AND x = 123");

        let query_two = build_redis_ast_from_cql3(&ast, &table_cache_schema).unwrap();

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

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema).unwrap();

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

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema).unwrap();

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

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema).unwrap();

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

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema).unwrap();

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

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema).unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZRANGEBYLEX")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"[123")),
            RedisFrame::BulkString(Bytes::from_static(b"+")),
        ]);

        assert_eq!(expected, query);

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x <= 123");

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema).unwrap();

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
