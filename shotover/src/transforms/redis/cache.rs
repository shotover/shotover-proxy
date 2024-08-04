use crate::config::chain::TransformChainConfig;
use crate::frame::{CassandraFrame, CassandraOperation, Frame, MessageType, RedisFrame};
use crate::message::{Message, MessageIdMap, Messages, Metadata};
use crate::transforms::chain::{TransformChain, TransformChainBuilder};
use crate::transforms::{
    DownChainProtocol, Transform, TransformBuilder, TransformConfig, TransformContextBuilder,
    TransformContextConfig, UpChainProtocol, Wrapper,
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use cassandra_protocol::compression::Compression;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{FQName, Identifier, Operand, RelationElement, RelationOperator};
use cql3_parser::select::Select;
use itertools::Itertools;
use metrics::{counter, Counter};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use tracing::{error, warn};

/// Data is stored in Redis as a Hash (hset/hget) and constructed from the cassandra SELECT statement
/// * The name of the hash is constructed from: the FROM component and partition + range keys as per the TableCacheSchema configuration
/// * The name of the field in the hash is constructed from: the SELECT component and the WHERE component excluding the partition + range keys used in the hash name
/// * The contents of field in the hash is: the raw bytes of a cassandra response from a SELECT
///
/// The cache is addressed in this way to allow all caches matching a specific partition + range keys to be deleted at once when invalidated via an INSERT or UPDATE
///
/// e.g.
/// with the following configuration:
///     caching_schema:
///       keyspace1.table2:
///         partition_key: [e]
///         range_key: []
/// then this cassandra query:
///     `SELECT a, b, c as g FROM keyspace1.table2 WHERE e='foo' a[2]=3`
/// will result in this redis command:
///     `hset "keyspace1.table2:'foo'" "a b c WHERE a[2]=3" $SELECT_RESPONSE_BYTES`

// TODO: ensure quoted identifiers wont cause collisions in the above described format

#[derive(Debug)]
enum CacheableState {
    // The selected row should be added to the cache
    CacheRow,
    // The modified/deleted rows should be removed from the cache
    DeleteRow,
    // All rows in the table should be removed from the cache
    DropTable,
    // The cache should be unaffected
    Skip,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TableCacheSchemaConfig {
    partition_key: Vec<String>,
    range_key: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TableCacheSchema {
    partition_key: Vec<Identifier>,
    range_key: Vec<Identifier>,
}

impl From<&TableCacheSchemaConfig> for TableCacheSchema {
    fn from(cfg: &TableCacheSchemaConfig) -> Self {
        TableCacheSchema {
            partition_key: cfg
                .partition_key
                .iter()
                .map(|s| Identifier::parse(s))
                .collect(),
            range_key: cfg.range_key.iter().map(|s| Identifier::parse(s)).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    pub caching_schema: HashMap<String, TableCacheSchemaConfig>,
    pub chain: TransformChainConfig,
}

const NAME: &str = "RedisCache";
#[typetag::serde(name = "RedisCache")]
#[async_trait(?Send)]
impl TransformConfig for RedisConfig {
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        let missed_requests = counter!("shotover_cache_miss_count");

        let caching_schema: HashMap<FQName, TableCacheSchema> = self
            .caching_schema
            .iter()
            .map(|(k, v)| (FQName::parse(k), v.into()))
            .collect();

        let transform_context_config = TransformContextConfig {
            chain_name: "cache_chain".into(),
            protocol: MessageType::Redis,
        };

        Ok(Box::new(SimpleRedisCacheBuilder {
            cache_chain: self.chain.get_builder(transform_context_config).await?,
            caching_schema,
            missed_requests,
        }))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::MustBeOneOf(vec![MessageType::Cassandra])
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

pub struct SimpleRedisCacheBuilder {
    cache_chain: TransformChainBuilder,
    caching_schema: HashMap<FQName, TableCacheSchema>,
    missed_requests: Counter,
}

impl TransformBuilder for SimpleRedisCacheBuilder {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(SimpleRedisCache {
            cache_chain: self.cache_chain.build(transform_context.clone()),
            caching_schema: self.caching_schema.clone(),
            missed_requests: self.missed_requests.clone(),
            pending_cache_requests: Default::default(),
            cache_hit_cassandra_responses: vec![],
            cache_miss_cassandra_requests: vec![],
        })
    }

    fn get_name(&self) -> &'static str {
        NAME
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

pub struct SimpleRedisCache {
    cache_chain: TransformChain,
    caching_schema: HashMap<FQName, TableCacheSchema>,
    missed_requests: Counter,
    pending_cache_requests: MessageIdMap<Message>,

    /// cleared by the end of every `Transform::transform` call, stored here to avoid reallocation
    cache_hit_cassandra_responses: Vec<Message>,
    /// cleared by the end of every `Transform::transform` call, stored here to avoid reallocation
    cache_miss_cassandra_requests: Vec<Message>,
}

impl SimpleRedisCache {
    fn build_cache_query(&mut self, request: &mut Message) -> Option<Message> {
        if let Some(Frame::Cassandra(CassandraFrame {
            operation: CassandraOperation::Query { query, .. },
            ..
        })) = request.frame()
        {
            if let CacheableState::CacheRow = is_cacheable(query) {
                if let Some(table_name) = query.get_table_name() {
                    if let Some(table_cache_schema) = self.caching_schema.get(table_name) {
                        match build_redis_key_from_cql3(query, table_cache_schema) {
                            Ok(address) => {
                                return Some(Message::from_frame_diverged(
                                    Frame::Redis(RedisFrame::Array(vec![
                                        RedisFrame::BulkString("HGET".into()),
                                        RedisFrame::BulkString(address.key),
                                        RedisFrame::BulkString(address.field),
                                    ])),
                                    request,
                                ));
                            }
                            Err(_e) => {} // TODO match Err(()) here or just have build_redis_key_from_cql3 return Option
                        }
                    }
                }
            }
        }

        None
    }

    fn unwrap_cache_response(&mut self, redis_responses: Messages) {
        for mut redis_response in redis_responses {
            let original_request = self
                .pending_cache_requests
                .remove(
                    &redis_response
                        .request_id()
                        .expect("This must have a request, since we dont use redis pubsub"),
                )
                .expect("There must be a pending request, since we store a pending request for all redis requests");
            let cassandra_frame = match redis_response.frame() {
                Some(Frame::Redis(redis_frame)) => {
                    match redis_frame {
                        RedisFrame::Error(err) => {
                            error!("Redis cache server returned error: {err:?}");
                            None
                        }
                        RedisFrame::BulkString(redis_bytes) => {
                            match CassandraFrame::from_bytes(redis_bytes.clone(), Compression::None)
                            {
                                Ok(mut response_frame) => {
                                    match original_request.metadata() {
                                        Ok(Metadata::Cassandra(meta)) => {
                                            if response_frame.version == meta.version {
                                                response_frame.stream_id = meta.stream_id;
                                                Some(response_frame)
                                            } else {
                                                // TODO: we should have some logic to convert to the
                                                // expected version instead of just failing here
                                                error!("Failed to use cache as mismatch between request version and cached response version");
                                                None
                                            }
                                        }
                                        Ok(_) => {
                                            error!("Not a cassandra request");
                                            None
                                        }
                                        Err(err) => {
                                            error!("invalid request {err:?}");
                                            None
                                        }
                                    }
                                }
                                Err(err) => {
                                    error!("Failed to decode cached cassandra message {err:?}");
                                    None
                                }
                            }
                        }
                        RedisFrame::Null => {
                            self.missed_requests.increment(1);
                            None
                        }
                        _ => None,
                    }
                }
                _ => None,
            };
            match cassandra_frame {
                Some(cassandra_frame) => {
                    self.cache_hit_cassandra_responses
                        .push(Message::from_frame_diverged(
                            Frame::Cassandra(cassandra_frame),
                            &redis_response,
                        ));
                }
                None => self.cache_miss_cassandra_requests.push(original_request),
            }
        }
    }

    async fn read_from_cache(
        &mut self,
        cassandra_requests: &mut Messages,
        local_addr: SocketAddr,
    ) -> Result<()> {
        let mut redis_requests = Vec::with_capacity(cassandra_requests.len());

        for mut cassandra_request in cassandra_requests.drain(..) {
            match self.build_cache_query(&mut cassandra_request) {
                // The request is cacheable, store the cassandra request for later and send the redis request
                Some(redis_request) => {
                    self.pending_cache_requests
                        .insert(cassandra_request.id(), cassandra_request);
                    redis_requests.push(redis_request);
                }
                // The request is not cacheable, add it directly to the cache miss list
                None => self.cache_miss_cassandra_requests.push(cassandra_request),
            }
        }

        let redis_responses = self
            .cache_chain
            .process_request(&mut Wrapper::new_with_addr(redis_requests, local_addr))
            .await?;

        self.unwrap_cache_response(redis_responses);

        Ok(())
    }

    /// Clears the cache for the entire table
    /// TODO make this drop only the specified keys not the entire cache
    fn drop_table(&self, _statement: &CassandraStatement, response: &Message) -> Message {
        Message::from_frame_at_instant(
            Frame::Redis(RedisFrame::Array(vec![RedisFrame::BulkString(
                "FLUSHDB".into(),
            )])),
            response.received_from_source_or_sink_at,
        )
    }

    /// clear the cache for the single row specified by the redis_key
    fn delete_row(
        &mut self,
        statement: &CassandraStatement,
        response: &Message,
    ) -> Option<Message> {
        if let Some(table_name) = statement.get_table_name() {
            if let Some(table_cache_schema) = self.caching_schema.get(table_name) {
                if let Ok(address) =
                    // TODO: handle errors
                    build_redis_key_from_cql3(statement, table_cache_schema)
                {
                    return Some(Message::from_frame_at_instant(
                        Frame::Redis(RedisFrame::Array(vec![
                            RedisFrame::BulkString("DEL".into()),
                            RedisFrame::BulkString(address.key),
                        ])),
                        response.received_from_source_or_sink_at,
                    ));
                }
            }
        }
        None
    }

    fn cache_row(
        &mut self,
        statement: &CassandraStatement,
        response: &mut Message,
    ) -> Result<Option<Message>> {
        if let Some(table_name) = statement.get_table_name() {
            if let Some(table_cache_schema) = self.caching_schema.get(table_name) {
                if let Ok(address) =
                    // TODO: handle errors
                    build_redis_key_from_cql3(statement, table_cache_schema)
                {
                    if let Some(Frame::Cassandra(frame)) = response.frame() {
                        // TODO: two performance issues here:
                        // 1. we should be able to generate the encoded bytes without cloning the entire frame
                        // 2. we should be able to directly use the raw bytes when the message has not yet been mutated
                        let encoded = frame.clone().encode(Compression::None);

                        return Ok(Some(Message::from_frame_at_instant(
                            Frame::Redis(RedisFrame::Array(vec![
                                RedisFrame::BulkString("HSET".into()),
                                RedisFrame::BulkString(address.key),
                                RedisFrame::BulkString(address.field),
                                RedisFrame::BulkString(encoded.into()),
                            ])),
                            response.received_from_source_or_sink_at,
                        )));
                    }
                }
            }
        }
        Ok(None)
    }

    /// calls the next transform and process the result for caching.
    async fn execute_upstream_and_write_to_cache<'a>(
        &mut self,
        requests_wrapper: &'a mut Wrapper<'a>,
    ) -> Result<Messages> {
        let local_addr = requests_wrapper.local_addr;
        let mut request_messages: Vec<_> = requests_wrapper
            .requests
            .iter_mut()
            .map(|message| message.frame().cloned())
            .collect();
        let mut response_messages = requests_wrapper.call_next_transform().await?;

        let mut cache_messages = vec![];
        for (request, response) in request_messages
            .iter_mut()
            .zip(response_messages.iter_mut())
        {
            if let Some(Frame::Cassandra(CassandraFrame { operation, .. })) = request {
                for statement in operation.queries() {
                    match is_cacheable(statement) {
                        CacheableState::DeleteRow => {
                            if let Some(message) = self.delete_row(statement, response) {
                                cache_messages.push(message);
                            }
                        }
                        CacheableState::DropTable => {
                            cache_messages.push(self.drop_table(statement, response));
                        }
                        CacheableState::CacheRow => {
                            if let Some(message) = self.cache_row(statement, response)? {
                                cache_messages.push(message);
                            }
                        }
                        CacheableState::Skip => {}
                    }
                }
            }
        }
        if !cache_messages.is_empty() {
            let result = self
                .cache_chain
                .process_request(&mut Wrapper::new_with_addr(cache_messages, local_addr))
                .await;
            if let Err(err) = result {
                warn!("Cache error: {err}");
            }
        }
        Ok(response_messages)
    }
}

fn is_cacheable(statement: &CassandraStatement) -> CacheableState {
    match statement {
        CassandraStatement::Select(select) => {
            if select.filtering || select.where_clause.is_empty() {
                CacheableState::Skip
            } else {
                CacheableState::CacheRow
            }
        }
        CassandraStatement::Insert(_) => CacheableState::DeleteRow,
        CassandraStatement::DropTable(_) => CacheableState::DropTable,
        CassandraStatement::Update(_) => CacheableState::DeleteRow,
        _ => CacheableState::Skip,
    }
}

/// build the redis key for the query.
/// key is cassandra partition key (must be completely specified) prepended to
/// the cassandra range key (may be partially specified)
fn build_query_redis_key_from_value_map(
    table_cache_schema: &TableCacheSchema,
    query_values: &BTreeMap<Operand, Vec<RelationElement>>,
    table_name: &str,
) -> Result<Bytes> {
    let mut key = table_name.as_bytes().to_vec();
    for column_name in &table_cache_schema.partition_key {
        match query_values.get(&Operand::Column(column_name.clone())) {
            None => {
                bail!("Partition key not complete. missing segment {column_name}")
            }
            Some(relation_elements) => {
                if relation_elements.len() > 1 {
                    bail!("partition key segment {column_name} has more than one relationship")
                }
                key.push(b':');
                key.extend(relation_elements[0].value.to_string().as_bytes());
            }
        }
    }

    let mut skipping = false;
    for column_name in &table_cache_schema.range_key {
        match query_values.get(&Operand::Column(column_name.clone())) {
            None => {
                skipping = true;
            }
            Some(relation_elements) => {
                if skipping {
                    // we skipped an earlier column so this is an error.
                    bail!("Columns in the middle of the range key were skipped");
                }
                if relation_elements.len() > 1 {
                    bail!("partition key segment {column_name} has more than one relationship");
                }

                key.push(b':');
                key.extend(relation_elements[0].value.to_string().as_bytes());
            }
        }
    }
    Ok(Bytes::from(key))
}

/// build the redis key for the query.
/// key is cassandra partition key (must be completely specified) prepended to
/// the cassandra range key (may be partially specified)
fn build_query_redis_field_from_value_map(
    table_cache_schema: &TableCacheSchema,
    mut query_values: BTreeMap<Operand, Vec<RelationElement>>,
    select: &Select,
) -> Bytes {
    for column_name in &table_cache_schema.partition_key {
        query_values.remove(&Operand::Column(column_name.clone()));
    }
    for column_name in &table_cache_schema.range_key {
        query_values.remove(&Operand::Column(column_name.clone()));
    }

    let mut str = if select.columns.is_empty() {
        String::from("WHERE ")
    } else {
        let mut tmp = select.columns.iter().join(", ");
        tmp.push_str(" WHERE ");
        tmp
    };
    str.push_str(
        query_values
            .iter_mut()
            .sorted()
            .flat_map(|(_k, v)| v.iter())
            .join(" AND ")
            .as_str(),
    );

    Bytes::from(str)
}

fn populate_value_map_from_where_clause(
    value_map: &mut BTreeMap<Operand, Vec<RelationElement>>,
    where_clause: &[RelationElement],
) {
    // TODO: we could easily make BTreeMap hold references instead of owned values
    for relation_element in where_clause {
        if let Some(vec) = value_map.get_mut(&relation_element.obj) {
            vec.push(relation_element.clone())
        } else {
            value_map.insert(relation_element.obj.clone(), vec![relation_element.clone()]);
        };
    }
}

#[derive(PartialEq, Debug)]
struct HashAddress {
    key: Bytes,
    field: Bytes,
}

fn build_redis_key_from_cql3(
    statement: &CassandraStatement,
    table_cache_schema: &TableCacheSchema,
) -> Result<HashAddress> {
    // TODO: is this value_map abstraction needed?
    // Surely we can just get the values from the tree as we need them?

    // TODO: is ordering of values handled consistently, so that the generated key/field pairs always refer to the same thing?

    // TODO: is the logic for generating key/field pairs correct?
    // I guess I need to brush up on my understanding of cassandra to answer this question.

    // TODO: split this logic up so that we dont need to return empty Bytes for field in INSERT/UPDATE
    let mut value_map = BTreeMap::new();
    match statement {
        CassandraStatement::Select(select) => {
            populate_value_map_from_where_clause(&mut value_map, &select.where_clause);
            Ok(HashAddress {
                key: build_query_redis_key_from_value_map(
                    table_cache_schema,
                    &value_map,
                    &select.table_name.to_string(),
                )?,
                field: build_query_redis_field_from_value_map(
                    table_cache_schema,
                    value_map,
                    select,
                ),
            })
        }

        CassandraStatement::Insert(insert) => {
            for (column_name, operand) in insert.get_value_map().into_iter() {
                let relation_element = RelationElement {
                    obj: Operand::Column(column_name.clone()),
                    oper: RelationOperator::Equal,
                    value: operand.clone(),
                };
                let key = Operand::Column(column_name);
                let value = value_map.get_mut(&key);
                if let Some(vec) = value {
                    vec.push(relation_element)
                } else {
                    value_map.insert(key, vec![relation_element]);
                };
            }
            Ok(HashAddress {
                key: build_query_redis_key_from_value_map(
                    table_cache_schema,
                    &value_map,
                    &insert.table_name.to_string(),
                )?,
                field: Bytes::new(),
            })
        }
        CassandraStatement::Update(update) => {
            populate_value_map_from_where_clause(&mut value_map, &update.where_clause);
            Ok(HashAddress {
                key: build_query_redis_key_from_value_map(
                    table_cache_schema,
                    &value_map,
                    &update.table_name.to_string(),
                )?,
                field: Bytes::new(),
            })
        }
        _ => unreachable!("{statement} should not be passed to build_redis_key_from_cql3",),
    }
}

#[async_trait]
impl Transform for SimpleRedisCache {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(
        &'a mut self,
        requests_wrapper: &'a mut Wrapper<'a>,
    ) -> Result<Messages> {
        self.read_from_cache(&mut requests_wrapper.requests, requests_wrapper.local_addr)
            .await
            .unwrap_or_else(|err| error!("Failed to fetch from cache: {err:?}"));

        // send the cache misses to cassandra
        // since requests_wrapper.requests is now empty we can just swap the two vectors to avoid reallocations
        assert!(requests_wrapper.requests.is_empty());
        std::mem::swap(
            &mut requests_wrapper.requests,
            &mut self.cache_miss_cassandra_requests,
        );
        let mut responses = self
            .execute_upstream_and_write_to_cache(requests_wrapper)
            .await?;

        // add the cache hits to the final response
        responses.append(&mut self.cache_hit_cassandra_responses);

        Ok(responses)
    }
}

#[cfg(test)]
mod test {
    use crate::frame::cassandra::parse_statement_single;
    use crate::transforms::chain::TransformChainBuilder;
    use crate::transforms::debug::printer::DebugPrinter;
    use crate::transforms::null::NullSink;
    use crate::transforms::redis::cache::{
        build_redis_key_from_cql3, HashAddress, SimpleRedisCacheBuilder, TableCacheSchema,
    };
    use crate::transforms::TransformBuilder;
    use bytes::Bytes;
    use cql3_parser::common::Identifier;
    use metrics::counter;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;

    #[test]
    fn equal_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec![Identifier::parse("z")],
            range_key: vec![Identifier::parse("x"), Identifier::parse("y")],
        };

        let ast = parse_statement_single("SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965");

        assert_eq!(
            build_redis_key_from_cql3(&ast, &table_cache_schema).unwrap(),
            HashAddress {
                key: Bytes::from("foo:1:123:965"),
                field: Bytes::from("* WHERE "),
            }
        );
    }

    #[test]
    fn insert_simple_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec![Identifier::parse("z")],
            range_key: vec![],
        };

        let ast = parse_statement_single("INSERT INTO foo (z, v) VALUES (1, 123)");

        assert_eq!(
            build_redis_key_from_cql3(&ast, &table_cache_schema).unwrap(),
            HashAddress {
                key: Bytes::from("foo:1"),
                field: Bytes::from(""),
            }
        );
    }

    #[test]
    fn insert_simple_clustering_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec![Identifier::parse("z")],
            range_key: vec![Identifier::parse("c")],
        };

        let ast = parse_statement_single("INSERT INTO foo (z, c, v) VALUES (1, 'yo' , 123)");

        assert_eq!(
            build_redis_key_from_cql3(&ast, &table_cache_schema).unwrap(),
            HashAddress {
                key: Bytes::from("foo:1:'yo'"),
                field: Bytes::from(""),
            }
        );
    }

    #[test]
    fn update_simple_clustering_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec![Identifier::parse("z")],
            range_key: vec![],
        };

        let ast = parse_statement_single("UPDATE foo SET c = 'yo', v = 123 WHERE z = 1");

        assert_eq!(
            build_redis_key_from_cql3(&ast, &table_cache_schema).unwrap(),
            HashAddress {
                key: Bytes::from("foo:1"),
                field: Bytes::from(""),
            }
        );
    }

    #[test]
    fn check_deterministic_order_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec![Identifier::parse("z")],
            range_key: vec![Identifier::parse("x"), Identifier::parse("y")],
        };

        let ast = parse_statement_single("SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965");
        let query_one = build_redis_key_from_cql3(&ast, &table_cache_schema).unwrap();

        let ast = parse_statement_single("SELECT * FROM foo WHERE y = 965 AND z = 1 AND x = 123");
        let query_two = build_redis_key_from_cql3(&ast, &table_cache_schema).unwrap();

        // Semantically databases treat the order of AND clauses differently, Cassandra however requires clustering key predicates be in order
        // So here we will just expect the order is correct in the query. TODO: we may need to revisit this as support for other databases is added
        assert_eq!(query_one, query_two);
    }

    #[test]
    fn range_exclusive_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec![Identifier::parse("z")],
            range_key: vec![],
        };

        let ast = parse_statement_single("SELECT * FROM foo WHERE z = 1 AND x > 123 AND x < 999");

        assert_eq!(
            build_redis_key_from_cql3(&ast, &table_cache_schema).unwrap(),
            HashAddress {
                key: Bytes::from("foo:1"),
                field: Bytes::from("* WHERE x > 123 AND x < 999"),
            }
        );
    }

    #[test]
    fn range_inclusive_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec![Identifier::parse("z")],
            range_key: vec![],
        };

        let ast = parse_statement_single("SELECT * FROM foo WHERE z = 1 AND x >= 123 AND x <= 999");

        assert_eq!(
            build_redis_key_from_cql3(&ast, &table_cache_schema).unwrap(),
            HashAddress {
                key: Bytes::from("foo:1"),
                field: Bytes::from("* WHERE x >= 123 AND x <= 999"),
            }
        );
    }

    #[test]
    fn single_pk_only_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec![Identifier::parse("id")],
            range_key: vec![],
        };

        let ast = parse_statement_single(
            "SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE id=1",
        );

        assert_eq!(
            build_redis_key_from_cql3(&ast, &table_cache_schema).unwrap(),
            HashAddress {
                key: Bytes::from("test_cache_keyspace_simple.test_table:1"),
                field: Bytes::from("id, x, name WHERE ")
            }
        );
    }

    #[test]
    fn compound_pk_only_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec![Identifier::parse("z"), Identifier::parse("y")],
            range_key: vec![],
        };

        let ast = parse_statement_single("SELECT thing FROM foo WHERE z = 1 AND y = 2");

        assert_eq!(
            build_redis_key_from_cql3(&ast, &table_cache_schema).unwrap(),
            HashAddress {
                key: Bytes::from("foo:1:2"),
                field: Bytes::from("thing WHERE ")
            }
        );
    }

    #[test]
    fn open_range_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec![Identifier::parse("z")],
            range_key: vec![],
        };

        let ast = parse_statement_single("SELECT * FROM foo WHERE z = 1 AND x >= 123");

        assert_eq!(
            build_redis_key_from_cql3(&ast, &table_cache_schema).unwrap(),
            HashAddress {
                key: Bytes::from("foo:1"),
                field: Bytes::from("* WHERE x >= 123")
            }
        );

        let ast = parse_statement_single("SELECT * FROM foo WHERE z = 1 AND x <= 123");

        assert_eq!(
            build_redis_key_from_cql3(&ast, &table_cache_schema).unwrap(),
            HashAddress {
                key: Bytes::from("foo:1"),
                field: Bytes::from("* WHERE x <= 123")
            }
        );
    }

    #[test]
    fn test_validate_invalid_chain() {
        let transform = SimpleRedisCacheBuilder {
            cache_chain: TransformChainBuilder::new(vec![], "test-chain"),
            caching_schema: HashMap::new(),
            missed_requests: counter!("cache_miss"),
        };

        assert_eq!(
            transform.validate(),
            vec![
                "RedisCache:",
                "  test-chain chain:",
                "    Chain cannot be empty"
            ]
        );
    }

    #[tokio::test]
    async fn test_validate_valid_chain() {
        let cache_chain = TransformChainBuilder::new(
            vec![
                Box::new(DebugPrinter::new()),
                Box::new(DebugPrinter::new()),
                Box::<NullSink>::default(),
            ],
            "test-chain",
        );

        let transform = SimpleRedisCacheBuilder {
            cache_chain,
            caching_schema: HashMap::new(),
            missed_requests: counter!("cache_miss"),
        };

        assert_eq!(transform.validate(), Vec::<String>::new());
    }
}
