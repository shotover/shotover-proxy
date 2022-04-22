use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::frame::cassandra::CQLStatement;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame, RedisFrame, CQL};
use crate::message::{Message, Messages, QueryType};
use crate::transforms::chain::TransformChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, Wrapper,
};
use anyhow::Result;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use cassandra_protocol::frame::Serialize;
use cassandra_protocol::frame::Version;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{Operand, RelationElement, RelationOperator};
use cql3_parser::select::{Named, Select, SelectElement};
use itertools::Itertools;
use metrics::{register_counter, Counter};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::io::Cursor;
use std::ops::Deref;
use tracing_log::log::{debug, error, info, trace, warn};

/*
Uses redis as a cache.  Data is stored in Redis as a Hash.
The key for the Redis cache is the key for the Cassandra query.
Only exact Redis matches are supported, though the redis key can equate to a Cassandra scan.
If the key is requested again the result query result is returned from the cache.
If the key is deleted or updated the key is removed from the cache.
If the table is dropped the keys are removed from the cache.

The redis hash keys are:
data - serialized form of the row data from cassandra.
metadata - serialized form of the metadata from cassandra.

 */
enum CacheableState {
    /// string is the table name
    Read(String),
    /// string is the table name
    Update(String),
    /// string is the table name
    Delete(String),
    /// string is the table being dropped
    Drop(String),
    /// string is the reason for the skip
    Skip(String),
    /// string is the reason for the error
    Err(String),
}

impl Display for CacheableState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheableState::Read(name) => {
                write!(f, "Reading {}", name)
            }
            CacheableState::Update(name) => {
                write!(f, "Updating {}", name)
            }
            CacheableState::Delete(name) => {
                write!(f, "Deleting {}", name)
            }
            CacheableState::Drop(name) => {
                write!(f, "Dropping {}", name)
            }
            CacheableState::Skip(txt) => {
                write!(f, "Skipping due to: {}", txt)
            }
            CacheableState::Err(txt) => {
                write!(f, "Error due to: {}", txt)
            }
        }
    }
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
        let missed_requests = register_counter!("cache_miss");

        Ok(Transforms::RedisCache(SimpleRedisCache {
            cache_chain: build_chain_from_config("cache_chain".to_string(), &self.chain, topics)
                .await?,
            caching_schema: self.caching_schema.clone(),
            missed_requests,
        }))
    }
}

#[derive(Clone)]
pub struct SimpleRedisCache {
    cache_chain: TransformChain,
    caching_schema: HashMap<String, TableCacheSchema>,
    missed_requests: Counter,
}

impl SimpleRedisCache {
    fn get_name(&self) -> &'static str {
        "SimpleRedisCache"
    }

    /// Build the messages for the cache query from the cassandra request messages.
    /// returns the Redis Messages or a `CacheableState:Err` or `CacheableState::Skip` as the error
    fn build_cache_query(
        &mut self,
        cassandra_messages: &mut Messages,
    ) -> Result<Messages, CacheableState> {
        let mut messages_redis_request = Vec::with_capacity(cassandra_messages.len());
        for cass_request in cassandra_messages {
            match &mut cass_request.frame() {
                Some(Frame::Cassandra(frame)) => {
                    for cql_statement in frame.operation.get_cql_statements() {
                        let mut state = is_cacheable(cql_statement);
                        if let CacheableState::Read(table_name) = &mut state {
                            let statement = &cql_statement.statement;
                            debug!("build_cache_query processing cacheable state");
                            if let Some(table_cache_schema) =
                                self.caching_schema.get(table_name.as_str())
                            {
                                match build_redis_key_from_cql3(statement, table_cache_schema) {
                                    Ok((redis_key, hash_key)) => {
                                        trace!(
                                            "Redis key: {:?}",
                                            std::str::from_utf8(redis_key.deref())
                                        );
                                        trace!(
                                            "Hash key: {:?}",
                                            std::str::from_utf8(hash_key.deref())
                                        );
                                        let commands_buffer = vec![
                                            RedisFrame::BulkString("HGET".into()),
                                            RedisFrame::BulkString(redis_key),
                                            RedisFrame::BulkString(hash_key),
                                        ];

                                        messages_redis_request.push(Message::from_frame(
                                            Frame::Redis(RedisFrame::Array(commands_buffer)),
                                        ));
                                    }
                                    Err(err_state) => {
                                        warn!("build_cache_query err: {}", err_state);
                                        state = err_state;
                                    }
                                }
                            } else {
                                state = CacheableState::Skip(format!(
                                    "Table {} not in caching list",
                                    table_name
                                ));
                            }
                        } else {
                            state = CacheableState::Skip(format!(
                                "{} is not a readable query",
                                cql_statement
                            ));
                        }

                        match state {
                            CacheableState::Err(_) | CacheableState::Skip(_) => {
                                debug!("build_cache_query: {}", state);
                                return Err(state);
                            }
                            _ => {}
                        }
                    }
                }
                _ => {
                    return Err(CacheableState::Err(format!(
                        "cannot fetch {cass_request:?} from cache"
                    )))
                }
            }
        }
        Ok(messages_redis_request)
    }

    /// unwraps redis response messages into cassandra messages. It does this by replacing the Cassandra
    /// request messages with their corresponding Cassandra response messages and returns them.
    /// Result is either the modified message_cass_request (now response messages) or and CacheableState::Err.
    fn unwrap_cache_response(
        &self,
        messages_redis_response: Messages,
        mut cassandra_messages: Messages,
    ) -> Result<Messages, CacheableState> {
        // Replace cass_request messages with cassandra responses in place.
        // We reuse the vec like this to save allocations.
        let mut messages_redis_response_iter = messages_redis_response.into_iter();
        /* there is a redis response for each statement in a CassandraMessage so we have to map
        the redis responses back to the cassandra requests
         */

        for cass_request in cassandra_messages.iter_mut() {
            // the responses for this request
            let cassandra_result: Result<CassandraResult, CacheableState> =
                if let Some(Frame::Cassandra(frame)) = &mut cass_request.frame() {
                    let queries = frame.operation.queries();
                    if queries.len() != 1 {
                        Err(CacheableState::Err(
                            "Cacheable Cassandra query must be only one statement".into(),
                        ))
                    } else if let Some(mut redis_response) = messages_redis_response_iter.next() {
                        match redis_response.frame() {
                            Some(Frame::Redis(redis_frame)) => {
                                match redis_frame {
                                    RedisFrame::SimpleString(_) => Err(CacheableState::Err(
                                        "Redis returned a simple string".into(),
                                    )),
                                    RedisFrame::Error(e) => {
                                        return Err(CacheableState::Err(e.to_string()))
                                    }
                                    RedisFrame::Integer(_) => Err(CacheableState::Err(
                                        "Redis returned an int value".into(),
                                    )),
                                    RedisFrame::BulkString(redis_bytes) => {
                                        // Redis response contains serialized version of result struct from CassandraOperation::Result( result )
                                        let x = redis_bytes.iter().copied().collect_vec();
                                        let mut cursor = Cursor::new(x.as_slice());
                                        let answer =
                                            CassandraResult::from_cursor(&mut cursor, Version::V4);
                                        if let Ok(result) = answer {
                                            Ok(result)
                                        } else {
                                            Err(CacheableState::Err(
                                                answer.err().unwrap().to_string(),
                                            ))
                                        }
                                    }
                                    RedisFrame::Array(_) => Err(CacheableState::Err(
                                        "Redis returned an array value".into(),
                                    )),
                                    RedisFrame::Null => {
                                        self.missed_requests.increment(1);
                                        Err(CacheableState::Skip("No cache results".into()))
                                    }
                                }
                            }

                            _ => Err(CacheableState::Err(
                                "No Redis frame in Redis response".into(),
                            )),
                        }
                    } else {
                        Err(CacheableState::Err("Redis response was None".into()))
                    }
                } else {
                    Ok(CassandraResult::Void)
                };
            if let Err(state) = cassandra_result {
                return Err(state);
            }

            *cass_request = Message::from_frame(Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                operation: CassandraOperation::Result(cassandra_result.ok().unwrap()),
                stream_id: cass_request.stream_id().unwrap(),
                tracing_id: None,
                warnings: vec![],
            }));
        }
        Ok(cassandra_messages)
    }

    /// Reads the data from the cache for all the messages or None.
    /// on success the values in the messages_cass_request will be modified to be Cassandra response messages.
    /// return is the Cassandra response messages or an error containing CacheableState::Skip or CacheableState::Err.
    async fn read_from_cache(
        &mut self,
        mut cassandra_messages: Messages,
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

        debug!("read_from_cache called");

        // build the cache query
        let messages_redis_request = self.build_cache_query(&mut cassandra_messages)?;

        // execute the cache query
        debug!("read_from_cache calling cache_chain.process_request");
        let messages_redis_response = self
            .cache_chain
            .process_request(
                Wrapper::new_with_chain_name(messages_redis_request, self.cache_chain.name.clone()),
                "clientdetailstodo".to_string(),
            )
            .await
            .map_err(|e| CacheableState::Err(format!("Redis error: {}", e)))?;

        debug!("read_from_cache received OK from cache_chain.process_request");
        self.unwrap_cache_response(messages_redis_response, cassandra_messages)
    }

    /// Clears the cache for the entire table
    /// TODO make this drop only the specified keys not the entire cache
    fn clear_table_cache(&self) -> Option<Message> {
        Some(Message::from_frame(Frame::Redis(RedisFrame::BulkString(
            "FLUSHDB".into(),
        ))))
    }

    /// clear the cache for the single row specified by the redis_key
    fn clear_row_cache(
        &mut self,
        cql_statement: &CQLStatement,
        table_cache_schema: &TableCacheSchema,
    ) -> Option<Message> {
        // TODO is it possible to return the future and process in parallel?
        let statement = &cql_statement.statement;
        if let Ok((redis_key, _hash_key)) = build_redis_key_from_cql3(statement, table_cache_schema)
        {
            let commands_buffer: Vec<RedisFrame> = vec![
                RedisFrame::BulkString("DEL".into()),
                RedisFrame::BulkString(redis_key),
            ];
            Some(Message::from_frame(Frame::Redis(RedisFrame::Array(
                commands_buffer,
            ))))
        } else {
            None
        }
    }

    /// calls the next transform and process the result for caching.
    async fn execute_upstream_and_process_result<'a>(
        &mut self,
        message_wrapper: Wrapper<'a>,
    ) -> ChainResponse {
        let mut orig_messages = message_wrapper.messages.clone();
        let orig_cql: Option<&mut CQL> = orig_messages
            .iter_mut()
            .filter_map(|message| {
                if let Some(Frame::Cassandra(CassandraFrame {
                    operation: CassandraOperation::Query { query, .. },
                    ..
                })) = message.frame()
                {
                    Some(query)
                } else {
                    None
                }
            })
            .next();
        let result_messages = &mut message_wrapper.call_next_transform().await?;
        if orig_cql.is_some() {
            let mut cache_messages: Vec<Message> = vec![];
            for (response, cql_statement) in result_messages
                .iter_mut()
                .zip(orig_cql.unwrap().statements.iter())
            {
                if let Some(Frame::Cassandra(CassandraFrame {
                    operation: CassandraOperation::Result(result),
                    ..
                })) = response.frame()
                {
                    match is_cacheable(cql_statement) {
                        CacheableState::Update(table_name) | CacheableState::Delete(table_name) => {
                            if let Some(table_cache_schema) = self.caching_schema.get(&table_name) {
                                let table_schema = table_cache_schema.clone();
                                if let Some(fut_message) =
                                    self.clear_row_cache(cql_statement, &table_schema)
                                {
                                    cache_messages.push(fut_message);
                                }
                            } else {
                                debug!("table {} is not being cached", table_name);
                            }
                        }
                        CacheableState::Drop(table_name) => {
                            info!("table {} dropped", table_name);
                            self.clear_table_cache();
                        }
                        CacheableState::Read(table_name) => {
                            let statement = &cql_statement.statement;
                            if let Some(table_cache_schema) =
                                self.caching_schema.get(table_name.as_str())
                            {
                                if let Ok((redis_key, hash_key)) =
                                    build_redis_key_from_cql3(statement, table_cache_schema)
                                {
                                    let mut encoded: Vec<u8> = Vec::new();
                                    let mut cursor = Cursor::new(&mut encoded);
                                    result.serialize(&mut cursor);

                                    let commands_buffer: Vec<RedisFrame> = vec![
                                        RedisFrame::BulkString("HSET".into()),
                                        RedisFrame::BulkString(redis_key),
                                        RedisFrame::BulkString(hash_key),
                                        RedisFrame::BulkString(encoded.into()),
                                    ];

                                    cache_messages.push(Message::from_frame(Frame::Redis(
                                        RedisFrame::Array(commands_buffer),
                                    )));
                                }
                            }
                        }
                        CacheableState::Skip(_reason) | CacheableState::Err(_reason) => {
                            // do nothing
                        }
                    }
                }
            }
            if !cache_messages.is_empty() {
                let result = self
                    .cache_chain
                    .process_request(
                        Wrapper::new_with_chain_name(cache_messages, self.cache_chain.name.clone()),
                        "clientdetailstodo".to_string(),
                    )
                    .await;
                if result.is_err() {
                    warn!("Cache error: {}", result.err().unwrap());
                }
            }
        }
        Ok(result_messages.to_vec())
    }
}

/// Determines if a statement is cacheable.  Cacheable statements have several common
/// properties as well as operation specific properties.
/// Common properties include
///  * must specify table name
///  * must not contain a parsing error
///  *
fn is_cacheable(cql_statement: &CQLStatement) -> CacheableState {
    // check issues common to all cql_statements
    if cql_statement.has_error {
        return CacheableState::Skip("CQL statement has error".into());
    }
    if let Some(table_name) = CQLStatement::get_table_name(&cql_statement.statement) {
        let has_params = CQLStatement::has_params(&cql_statement.statement);

        match &cql_statement.statement {
            CassandraStatement::Select(select) => {
                if has_params {
                    CacheableState::Delete(table_name.into())
                } else if select.filtering {
                    CacheableState::Skip("Can not cache with ALLOW FILTERING".into())
                } else if select.where_clause.is_empty() {
                    CacheableState::Skip("Can not cache if where clause is empty".into())
                    /* } else if !select.columns.is_empty() {
                        if select.columns.len() == 1 && select.columns[0].eq(&SelectElement::Star) {
                            CacheableState::Read
                        } else {
                            CacheableState::Skip(
                                "Can not cache if columns other than '*' are selected".into(),
                            )
                        }

                    */
                } else {
                    CacheableState::Read(table_name.into())
                }
            }
            CassandraStatement::Insert(insert) => {
                if has_params || insert.if_not_exists {
                    CacheableState::Delete(table_name.into())
                } else {
                    CacheableState::Update(table_name.into())
                }
            }
            CassandraStatement::DropTable(_) => CacheableState::Drop(table_name.into()),
            CassandraStatement::Update(update) => {
                if has_params || update.if_exists {
                    CacheableState::Delete(table_name.into())
                } else {
                    for assignment_element in &update.assignments {
                        if assignment_element.operator.is_some() {
                            debug!(
                                "Clearing {} cache: {} has calculations in values",
                                update.table_name, assignment_element.name
                            );
                            return CacheableState::Delete(table_name.into());
                        }
                        if assignment_element.name.idx.is_some() {
                            debug!(
                                "Clearing {} cache: {} is an indexed columns",
                                update.table_name, assignment_element.name
                            );
                            return CacheableState::Delete(table_name.into());
                        }
                    }
                    CacheableState::Update(table_name.into())
                }
            }

            _ => CacheableState::Skip("Statement is not a cacheable type".into()),
        }
    } else {
        CacheableState::Skip("No table name specified".into())
    }
}

/// build the redis key for the query.
/// key is cassandra partition key (must be completely specified) prepended to
/// the cassandra range key (may be partially specified)
fn build_query_redis_key_from_value_map(
    table_cache_schema: &TableCacheSchema,
    query_values: &BTreeMap<String, Vec<RelationElement>>,
    table_name: &str,
) -> Result<Bytes, CacheableState> {
    let mut key: Vec<u8> = vec![];
    key.extend(table_name.as_bytes());
    for c_name in &table_cache_schema.partition_key {
        let column_name = c_name.to_lowercase();
        debug!("processing partition key segment: {}", column_name);
        match query_values.get(column_name.as_str()) {
            None => {
                return Err(CacheableState::Skip(format!(
                    "Partition key not complete. missing segment {}",
                    column_name
                )));
            }
            Some(relation_elements) => {
                if relation_elements.len() > 1 {
                    return Err(CacheableState::Skip(format!(
                        "partition key segment {} has more than one relationship",
                        column_name
                    )));
                }
                debug!(
                    "extending key with segment {} value {}",
                    column_name, relation_elements[0].value
                );
                key.push(b':');
                key.extend(relation_elements[0].value.to_string().as_bytes());
            }
        }
    }
    let mut skipping = false;

    for c_name in &table_cache_schema.range_key {
        let column_name = c_name.to_lowercase();
        match query_values.get(column_name.as_str()) {
            None => {
                skipping = true;
            }
            Some(relation_elements) => {
                if skipping {
                    // we skipped an earlier column so this is an error.
                    return Err(CacheableState::Err(
                        "Columns in the middle of the range key were skipped".into(),
                    ));
                }
                if relation_elements.len() > 1 {
                    return Err(CacheableState::Skip(format!(
                        "partition key segment {} has more than one relationship",
                        column_name
                    )));
                }
                debug!(
                    "extending key with segment {} value {}",
                    column_name, relation_elements[0].value
                );

                key.push(b':');
                key.extend(relation_elements[0].value.to_string().as_bytes());
            }
        }
    }
    Ok(BytesMut::from(key.as_slice()).freeze())
}

/// build the redis key for the query.
/// key is cassandra partition key (must be completely specified) prepended to
/// the cassandra range key (may be partially specified)
fn build_query_redis_hash_from_value_map(
    table_cache_schema: &TableCacheSchema,
    query_values: &BTreeMap<String, Vec<RelationElement>>,
    select: &Select,
) -> Result<Bytes, CacheableState> {
    let mut my_values = query_values.clone();
    for c_name in &table_cache_schema.partition_key {
        let column_name = c_name.to_lowercase();
        my_values.remove(&column_name);
    }
    for c_name in &table_cache_schema.range_key {
        let column_name = c_name.to_lowercase();
        my_values.remove(&column_name);
    }

    let mut str = if select.columns.is_empty() {
        String::from("WHERE ")
    } else {
        let mut tmp = select
            .columns
            .iter()
            .map(|select_element| match select_element {
                SelectElement::Star => SelectElement::Star,
                SelectElement::Column(named) => SelectElement::Column(Named {
                    name: named.name.to_lowercase(),
                    alias: named.alias.as_ref().map(|name| name.to_lowercase()),
                }),
                SelectElement::Function(named) => SelectElement::Function(Named {
                    name: named.name.to_lowercase(),
                    alias: named.alias.as_ref().map(|name| name.to_lowercase()),
                }),
            })
            .join(", ");
        tmp.push_str(" WHERE ");
        tmp
    };
    str.push_str(
        my_values
            .iter_mut()
            .sorted()
            .flat_map(|(_k, v)| v.iter())
            .join(" AND ")
            .as_str(),
    );

    Ok(BytesMut::from(str.as_str()).freeze())
}

fn populate_value_map_from_where_clause(
    value_map: &mut BTreeMap<String, Vec<RelationElement>>,
    where_clause: &[RelationElement],
) {
    for relation_element in where_clause {
        let column_name = relation_element.obj.to_string().to_lowercase();
        let value = value_map.get_mut(column_name.as_str());
        if let Some(vec) = value {
            vec.push(relation_element.clone())
        } else {
            value_map.insert(column_name, vec![relation_element.clone()]);
        };
    }
}

fn build_redis_key_from_cql3(
    statement: &CassandraStatement,
    table_cache_schema: &TableCacheSchema,
) -> Result<(Bytes, Bytes), CacheableState> {
    let mut value_map: BTreeMap<String, Vec<RelationElement>> = BTreeMap::new();
    match statement {
        CassandraStatement::Select(select) => {
            populate_value_map_from_where_clause(&mut value_map, &select.where_clause);
            Ok((
                build_query_redis_key_from_value_map(
                    table_cache_schema,
                    &value_map,
                    &select.table_name.to_string(),
                )?,
                build_query_redis_hash_from_value_map(table_cache_schema, &value_map, select)?,
            ))
        }

        CassandraStatement::Insert(insert) => {
            for (c_name, operand) in insert.get_value_map().into_iter() {
                let column_name = c_name.to_lowercase();
                let relation_element = RelationElement {
                    obj: Operand::Column(column_name.clone()),
                    oper: RelationOperator::Equal,
                    value: operand.clone(),
                };
                let value = value_map.get_mut(column_name.as_str());
                if let Some(vec) = value {
                    vec.push(relation_element)
                } else {
                    value_map.insert(column_name, vec![relation_element]);
                };
            }
            Ok((
                build_query_redis_key_from_value_map(
                    table_cache_schema,
                    &value_map,
                    &insert.table_name.to_string(),
                )?,
                Bytes::new(),
            ))
        }
        CassandraStatement::Update(update) => {
            populate_value_map_from_where_clause(&mut value_map, &update.where_clause);
            Ok((
                build_query_redis_key_from_value_map(
                    table_cache_schema,
                    &value_map,
                    &update.table_name.to_string(),
                )?,
                Bytes::new(),
            ))
        }
        _ => unreachable!(
            "{} should not be passed to build_redis_key_from_cql3",
            statement
        ),
    }
}

#[async_trait]
impl Transform for SimpleRedisCache {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        let mut read_cache = true;
        for m in &mut message_wrapper.messages {
            if let Some(Frame::Cassandra(CassandraFrame {
                operation: CassandraOperation::Query { query, .. },
                ..
            })) = m.frame()
            {
                for cql_statement in &query.statements {
                    debug!("cache transform processing {}", cql_statement);
                    match cql_statement.get_query_type() {
                        QueryType::Read => {}
                        QueryType::Write => read_cache = false,
                        QueryType::ReadWrite => read_cache = false,
                        QueryType::SchemaChange => read_cache = false,
                        QueryType::PubSubMessage => {}
                    }
                }
            } else {
                read_cache = false;
            }
        }
        debug!("cache transform read_cache:{} ", read_cache);

        // If there are no write queries (all queries are reads) we can read the cache
        if read_cache {
            match self.read_from_cache(message_wrapper.messages.clone()).await {
                Ok(cr) => return Ok(cr),
                Err(inner_state) => match &inner_state {
                    CacheableState::Skip(reason) => {
                        info!("Cache skipped: {} ", reason);
                        self.execute_upstream_and_process_result(message_wrapper)
                            .await
                    }
                    CacheableState::Err(reason) => {
                        error!("Cache failed: {} ", reason);
                        message_wrapper.call_next_transform().await
                    }
                    _ => {
                        unreachable!("should not find read, update or delete as an error.");
                    }
                },
            }
        } else {
            self.execute_upstream_and_process_result(message_wrapper)
                .await
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
    use crate::frame::CQL;
    use crate::transforms::chain::TransformChain;
    use crate::transforms::debug::printer::DebugPrinter;
    use crate::transforms::null::Null;
    use crate::transforms::redis::cache::{
        build_redis_key_from_cql3, SimpleRedisCache, TableCacheSchema,
    };
    use crate::transforms::{Transform, Transforms};
    use bytes::{Bytes, BytesMut};
    use cql3_parser::cassandra_statement::CassandraStatement;
    use metrics::register_counter;
    use std::collections::HashMap;

    fn build_query(query_string: &str) -> CassandraStatement {
        let cql = CQL::parse_from_string(query_string);
        assert!(!cql.has_error);
        cql.statements[0].statement.clone()
    }

    #[test]
    fn test_build_keys() {}

    #[test]
    fn equal_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec!["x".to_string(), "y".to_string()],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965");

        let (redis_key, hash_key) = build_redis_key_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        assert_eq!(Bytes::from("foo:1:123:965"), redis_key);
        assert_eq!(Bytes::from("* WHERE "), hash_key);
    }

    #[test]
    fn insert_simple_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let ast = build_query("INSERT INTO foo (z, v) VALUES (1, 123)");

        let (redis_key, hash_key) = build_redis_key_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        assert_eq!(BytesMut::from("foo:1"), redis_key);
        assert!(hash_key.is_empty());
    }

    #[test]
    fn insert_simple_clustering_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec!["c".to_string()],
        };

        let ast = build_query("INSERT INTO foo (z, c, v) VALUES (1, 'yo' , 123)");
        let (redis_key, hash_key) = build_redis_key_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        assert_eq!(BytesMut::from("foo:1:'yo'"), redis_key);
        assert!(hash_key.is_empty());
    }

    #[test]
    fn update_simple_clustering_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let ast = build_query("UPDATE foo SET c = 'yo', v = 123 WHERE z = 1");

        let result = build_redis_key_from_cql3(&ast, &table_cache_schema);
        let (redis_key, hash_key) = result.ok().unwrap();

        assert_eq!(BytesMut::from("foo:1"), redis_key);
        assert!(hash_key.is_empty());
    }

    #[test]
    fn check_deterministic_order_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec!["x".to_string(), "y".to_string()],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965");

        let query_one = build_redis_key_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        let ast = build_query("SELECT * FROM foo WHERE y = 965 AND z = 1 AND x = 123");

        let query_two = build_redis_key_from_cql3(&ast, &table_cache_schema)
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
            range_key: vec![],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x > 123 AND x < 999");

        let (redis_key, hash_key) = build_redis_key_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        assert_eq!(BytesMut::from("foo:1"), redis_key);
        assert_eq!(BytesMut::from("* WHERE x > 123 AND x < 999"), hash_key);
    }

    #[test]
    fn range_inclusive_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x >= 123 AND x <= 999");

        let (redis_key, hash_key) = build_redis_key_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        assert_eq!(BytesMut::from("foo:1"), redis_key);
        assert_eq!(BytesMut::from("* WHERE x >= 123 AND x <= 999"), hash_key);
    }

    #[test]
    fn single_pk_only_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["id".to_string()],
            range_key: vec![],
        };

        let ast =
            build_query("SELECT id, x, name FROM test_cache_keyspace_simple.test_table WHERE id=1");

        let (redis_key, hash_key) = build_redis_key_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        assert_eq!(
            BytesMut::from("test_cache_keyspace_simple.test_table:1"),
            redis_key
        );
        assert_eq!(BytesMut::from("id, x, name WHERE "), hash_key);
    }

    #[test]
    fn compound_pk_only_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string(), "y".to_string()],
            range_key: vec![],
        };

        let ast = build_query("SELECT thing FROM foo WHERE z = 1 AND y = 2");

        let (key, hash_key) = build_redis_key_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        assert_eq!(BytesMut::from("foo:1:2"), key);
        assert_eq!(BytesMut::from("thing WHERE "), hash_key);
    }

    #[test]
    fn open_range_test() {
        let table_cache_schema = TableCacheSchema {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x >= 123");

        let (redis_key, hash_key) = build_redis_key_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        assert_eq!(BytesMut::from("foo:1"), redis_key);
        assert_eq!(BytesMut::from("* WHERE x >= 123"), hash_key);

        let ast = build_query("SELECT * FROM foo WHERE z = 1 AND x <= 123");

        let (redis_key, hash_key) = build_redis_key_from_cql3(&ast, &table_cache_schema)
            .ok()
            .unwrap();

        assert_eq!(BytesMut::from("foo:1"), redis_key);
        assert_eq!(BytesMut::from("* WHERE x <= 123"), hash_key);
    }

    #[tokio::test]
    async fn test_validate_invalid_chain() {
        let missed_requests = register_counter!("cache_miss");
        let chain = TransformChain::new(vec![], "test-chain".to_string());
        let transform = SimpleRedisCache {
            cache_chain: chain,
            caching_schema: HashMap::new(),
            missed_requests,
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
        let missed_requests = register_counter!("cache_miss");
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
            missed_requests,
        };

        assert_eq!(transform.validate(), Vec::<String>::new());
    }
}
