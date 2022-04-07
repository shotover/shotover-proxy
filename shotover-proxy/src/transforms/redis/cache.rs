use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame, RedisFrame, CQL};
use crate::message::{Message, Messages, QueryType};
use crate::transforms::chain::TransformChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, Wrapper,
};
use anyhow::Result;
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use cassandra_protocol::frame::Version;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{Operand, RelationElement, RelationOperator};
use cql3_parser::select::SelectElement;
use itertools::Itertools;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use tracing_log::log::info;

enum CacheableState {
    Read,
    Update,
    Delete,
    /// string is the reason for the skip
    Skip(String),
    /// string is the reason for the error
    Err(String),
}

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
    ) -> Result<Messages, CacheableState> {
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
                    for statement in frame.operation.queries() {
                        let mut state = is_cacheable(statement);

                        match state {
                            // TODO implement proper handling of state
                            // currently if state is not Skip or Error it just processes
                            CacheableState::Read
                            | CacheableState::Update
                            | CacheableState::Delete => {
                                if let Some(table_name) = CQL::get_table_name(statement) {
                                    if let Some(table_cache_schema) =
                                        self.caching_schema.get(table_name)
                                    {
                                        let redis_state = build_redis_ast_from_cql3(
                                            statement,
                                            table_cache_schema,
                                        );
                                        if redis_state.is_ok() {
                                            messages_redis_request.push(Message::from_frame(
                                                Frame::Redis(redis_state.ok().unwrap()),
                                            ));
                                        } else {
                                            state = redis_state.err().unwrap();
                                        }
                                    } else {
                                        state = CacheableState::Skip(
                                            "Table not in caching list".into(),
                                        );
                                    }
                                } else {
                                    state = CacheableState::Skip("Table name not in query".into());
                                }
                            }
                            _ => {
                                // do nothing here but check again again outside of match as state may have changed
                            }
                        }

                        if let CacheableState::Err(_) = state {
                            return Err(state);
                        }

                        if let CacheableState::Skip(_) = state {
                            return Err(state);
                        }
                    }
                }
                _ => {
                    return Err(CacheableState::Err(format!(
                        "cannot fetch {cass_request:?} from cache"
                    )));
                }
            }
        }

        match self
            .cache_chain
            .process_request(
                Wrapper::new_with_chain_name(messages_redis_request, self.cache_chain.name.clone()),
                "clientdetailstodo".to_string(),
            )
            .await
        {
            Ok(messages_redis_response) => {
                // Replace cass_request messages with cassandra responses in place.
                // We reuse the vec like this to save allocations.
                let mut messages_redis_response_iter = messages_redis_response.into_iter();
                for cass_request in &mut messages_cass_request {
                    let mut redis_responses = vec![];
                    if let Some(Frame::Cassandra(frame)) = cass_request.frame() {
                        for _query in frame.operation.queries() {
                            redis_responses.push(messages_redis_response_iter.next());
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
            Err(e) => Err(CacheableState::Err(format!("Redis error: {}", e))),
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
    operator: &RelationOperator,
    operand: &Operand,
    min: &mut Vec<u8>,
    max: &mut Vec<u8>,
) -> Result<(), CacheableState> {
    let mut bytes = BytesMut::from(operand.to_string().as_bytes());
    match operator {
        RelationOperator::LessThan => {
            let last_byte = bytes.last_mut().unwrap();
            *last_byte -= 1;

            append_prefix_max(max);
            max.extend(bytes.iter());
            Ok(())
        }
        RelationOperator::LessThanOrEqual => {
            append_prefix_max(max);
            max.extend(bytes.iter());
            Ok(())
        }

        RelationOperator::Equal => {
            append_prefix_min(min);
            append_prefix_max(max);
            min.extend(bytes.iter());
            max.extend(bytes.iter());
            Ok(())
        }
        RelationOperator::GreaterThanOrEqual => {
            append_prefix_min(min);
            min.extend(bytes.iter());
            Ok(())
        }
        RelationOperator::GreaterThan => {
            let last_byte = bytes.last_mut().unwrap();
            *last_byte += 1;
            append_prefix_min(min);
            min.extend(bytes.iter());
            Ok(())
        }
        // should "IN"" be converted to an "or" "eq" combination
        RelationOperator::NotEqual
        | RelationOperator::In
        | RelationOperator::Contains
        | RelationOperator::ContainsKey => Err(CacheableState::Skip(format!(
            "{} comparisons are not supported",
            operator
        ))),
        RelationOperator::IsNot => Err(CacheableState::Skip(
            "IS NOT NULL comparisons are not supported".into(),
        )),
    }
}

fn is_cacheable(statement: &CassandraStatement) -> CacheableState {
    let has_params = CQL::has_params(statement);

    match statement {
        CassandraStatement::Select(select) => {
            if has_params {
                CacheableState::Delete
            } else if select.filtering {
                CacheableState::Skip("Can not cache with ALLOW FILTERING".into())
            } else if select.where_clause.is_empty() {
                CacheableState::Skip("Can not cache if where clause is empty".into())
            } else if !select.columns.is_empty() {
                if select.columns.len() == 1 && select.columns[0].eq(&SelectElement::Star) {
                    CacheableState::Read
                } else {
                    CacheableState::Skip(
                        "Can not cache if columns other than '*' are not selected".into(),
                    )
                }
            } else {
                CacheableState::Read
            }
        }
        CassandraStatement::Insert(insert) => {
            if has_params || insert.if_not_exists {
                CacheableState::Delete
            } else {
                CacheableState::Update
            }
        }
        CassandraStatement::Update(update) => {
            if has_params || update.if_exists {
                CacheableState::Delete
            } else {
                for assignment_element in &update.assignments {
                    if assignment_element.operator.is_some() {
                        info!(
                            "Clearing {} cache: {} has calculations in values",
                            update.table_name, assignment_element.name
                        );
                        return CacheableState::Delete;
                    }
                    if assignment_element.name.idx.is_some() {
                        info!(
                            "Clearing {} cache: {} is an indexed columns",
                            update.table_name, assignment_element.name
                        );
                        return CacheableState::Delete;
                    }
                }
                CacheableState::Update
            }
        }

        _ => CacheableState::Skip("Statement is not a cacheable type".into()),
    }
}

fn build_redis_ast_from_cql3(
    statement: &CassandraStatement,
    table_cache_schema: &TableCacheSchema,
) -> Result<RedisFrame, CacheableState> {
    match statement {
        CassandraStatement::Select(select) => {
            let mut min: Vec<u8> = Vec::new();
            let mut max: Vec<u8> = Vec::new();

            // extract the partition and range operands
            // fail if any are missing
            let mut partition_segments: HashMap<&str, &Operand> = HashMap::new();
            let mut range_segments: HashMap<&str, Vec<&RelationElement>> = HashMap::new();

            for relation_element in &select.where_clause {
                if let Operand::Column(column_name) = &relation_element.obj {
                    // name has to be in partition or range key.
                    if table_cache_schema.partition_key.contains(column_name) {
                        partition_segments.insert(column_name, &relation_element.value);
                    } else if table_cache_schema.range_key.contains(column_name) {
                        let value = range_segments.get_mut(column_name.as_str());
                        if let Some(vec) = value {
                            vec.push(relation_element)
                        } else {
                            range_segments.insert(column_name, vec![relation_element]);
                        };
                    } else {
                        return Err(CacheableState::Skip(format!(
                            "Couldn't build query - column {} is not in the key",
                            column_name
                        )));
                    }
                }
            }
            let mut skipping = false;
            for column_name in &table_cache_schema.range_key {
                if let Some(relation_elements) = range_segments.get(column_name.as_str()) {
                    if skipping {
                        // we skipped an earlier column so this is an error.
                        return Err(CacheableState::Err(
                            "Columns in the middle of the range key were skipped".into(),
                        ));
                    }
                    for range_element in relation_elements {
                        if let Err(e) = build_zrangebylex_min_max_from_sql(
                            &range_element.oper,
                            &range_element.value,
                            &mut min,
                            &mut max,
                        ) {
                            return Err(e);
                        }
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
                    partition_key.extend(operand.to_string().as_bytes());
                } else {
                    return Err(CacheableState::Err(format!(
                        "partition column {} missing",
                        column_name
                    )));
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
        CassandraStatement::Insert(insert) => {
            let query_values = insert.get_value_map();
            add_query_values(table_cache_schema, query_values)
        }
        CassandraStatement::Update(update) => {
            let mut query_values: BTreeMap<String, &Operand> = BTreeMap::new();

            update.assignments.iter().for_each( |assignment| {query_values.insert( assignment.name.to_string(), &assignment.value);} );
            for relation_element in &update.where_clause {
                if relation_element.oper == RelationOperator::Equal {
                    if let Operand::Column(name) = &relation_element.obj {
                        if table_cache_schema.partition_key.contains(name)
                            || table_cache_schema.range_key.contains(name)
                        {
                            query_values.insert(name.clone(), &relation_element.value);
                        }
                    }
                }
            }
            add_query_values(table_cache_schema, query_values)
        }
        _ => unreachable!(
            "{} should not be passed to build_redis_ast_from_cql3",
            statement
        ),
    }
}

fn add_query_values(
    table_cache_schema: &TableCacheSchema,
    query_values: BTreeMap<String, &Operand>,
) -> Result<RedisFrame, CacheableState> {
    let mut partition_key = BytesMut::new();
    for column_name in &table_cache_schema.partition_key {
        if let Some(operand) = query_values.get(column_name) {
            partition_key.extend(operand.to_string().as_bytes());
        } else {
            return Err(CacheableState::Err(format!(
                "partition column {} missing",
                column_name
            )));
        }
    }

    let mut clustering = BytesMut::new();
    for column_name in &table_cache_schema.range_key {
        if let Some(operand) = query_values.get(column_name.as_str()) {
            clustering.extend(operand.to_string().as_bytes());
        } else {
            return Err(CacheableState::Err(format!(
                "range column {} missing",
                column_name
            )));
        }
    }

    let mut commands_buffer: Vec<RedisFrame> = vec![
        RedisFrame::BulkString("ZADD".into()),
        RedisFrame::BulkString(partition_key.freeze()),
    ];

    // get values not in partition or cluster key
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
        value.extend(operand.to_string().as_bytes());
        commands_buffer.push(RedisFrame::BulkString(value.freeze()));
    }

    Ok(RedisFrame::Array(commands_buffer))
}

#[async_trait]
impl Transform for SimpleRedisCache {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        let mut read_cache = true;
        for m in &mut message_wrapper.messages {
            if let Some(Frame::Cassandra(CassandraFrame {
                operation: CassandraOperation::Query { .. },
                ..
            })) = m.frame()
            {
                /* let statement = query.get_statement();
                match  is_cacheable(statement ) {
                    CacheableState::Read |
                    CacheableState::Update |
                    CacheableState::Delete => {}
                    CacheableState::Skip(reason)  => {
                        tracing::info!( "Cache skipped for {} due to {}", statement, reason );
                        use_cache = false;
                    }
                    CacheableState::Err(reason) => {
                        tracing::error!("Cache failed for {} due to {}", statement, reason);
                        use_cache = false;
                    }
                }

                */
                match m.get_query_type() {
                    QueryType::Read => {}
                    QueryType::Write => read_cache = false,
                    QueryType::ReadWrite => read_cache = false,
                    QueryType::SchemaChange => read_cache = false,
                    QueryType::PubSubMessage => {}
                }
            }
        }

        // If there are no write queries (all queries are reads) we can use the cache
        if read_cache {
            match self
                .get_or_update_from_cache(message_wrapper.messages.clone())
                .await
            {
                Ok(cr) => return Ok(cr),
                Err(inner_state) => match &inner_state {
                    CacheableState::Read | CacheableState::Update | CacheableState::Delete => {
                        unreachable!("should not find read, update or delete as an error");
                    }
                    CacheableState::Skip(reason) => {
                        tracing::info!("Cache skipped: {} ", reason);
                        message_wrapper.call_next_transform().await
                    }
                    CacheableState::Err(reason) => {
                        tracing::error!("Cache failed: {} ", reason);
                        message_wrapper.call_next_transform().await
                    }
                },
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
        cql.get_statement().clone()
    }

    #[test]
    fn equal_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec!["x".to_string(), "y".to_string()],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965");

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

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

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

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
        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZADD")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"0")),
            RedisFrame::BulkString(Bytes::from_static(b"'yo':123")),
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

        let result = build_redis_ast_from_cql3(&ast, &table_cache_schema);
        let query = result.ok().unwrap();


        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZADD")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"0")),
            RedisFrame::BulkString(Bytes::from_static(b"'yo':123")),
        ]);

        assert_eq!(expected, query);
    }

    #[test]
    fn check_deterministic_order_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec!["x".to_string(), "y".to_string()],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965");

        let query_one = build_redis_ast_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        let ast = build_query("SELECT * FROM foo WHERE y = 965 AND z = 1 AND x = 123");

        let query_two = build_redis_ast_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        // Semantically databases treat the order of AND clauses differently, Cassandra however requires clustering key predicates be in order
        // So here we will just expect the order is correct in the query. TODO: we may need to revisit this as support for other databases is added
        assert_eq!(query_one, query_two);
    }

    #[test]
    fn range_exclusive_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec!["x".to_string()],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x > 123 AND x < 999");

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

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
            range_key: vec!["x".to_string()],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x >= 123 AND x <= 999");

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

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

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

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

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

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
            range_key: vec!["x".to_string()],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x >= 123");

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        let expected = RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"ZRANGEBYLEX")),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            RedisFrame::BulkString(Bytes::from_static(b"[123")),
            RedisFrame::BulkString(Bytes::from_static(b"+")),
        ]);

        assert_eq!(expected, query);

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x <= 123");

        let query = build_redis_ast_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

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
