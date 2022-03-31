use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame, RedisFrame};
use crate::message::{Message, MessageValue, Messages, QueryType};
use crate::transforms::chain::TransformChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, Wrapper,
};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use cassandra_protocol::frame::Version;
use serde::Deserialize;
use std::collections::HashMap;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{Operand, PrimaryKey, RelationElement, RelationOperator, WhereClause};

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


impl From<&PrimaryKey> for TableCacheSchema {
    fn from(value: &PrimaryKey) -> TableCacheSchema {
        TableCacheSchema { partition_key: value.partition.clone(), range_key: value.clustering.clone() }
    }
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
        mut messages_cass_request: Messages
    ) -> ChainResponse {
        // This function is a little hard to follow, so heres an overview.
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
            if let Some(table_name) = cass_request.namespace().map(|x| x.join(".")) {
                match cass_request.frame() {
                    Some(Frame::Cassandra(frame)) => {
                        for query in frame.operation.queries()? {
                            let table_cache_schema = self
                                .caching_schema
                                .get(&table_name)
                                .ok_or_else(|| anyhow!("{table_name} not a caching table"))?;

                            messages_redis_request.push(Message::from_frame(Frame::Redis(
                                build_redis_ast_from_cql3(query, table_cache_schema)?,
                            )));
                        }
                    }
                    message => bail!("cannot fetch {message:?} from cache"),
                }
            } else {
                bail!("Failed to get message namespace");
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

fn build_zrangebylex_min_max_from_cql3(
    operator : &RelationOperator,
    operand: &Operand,
    min: &mut Vec<u8>,
    max: &mut Vec<u8>,
) -> Result<()> {

    let mut bytes =
        match operand {
        Operand::Const(value) => {
            Vec::from(
                match value.to_uppercase().as_str() {
            "TRUE" => &TRUE,
                "FALSE" => &FALSE,
                _ => value.as_bytes(),
            })
        }
        Operand::Map(_) |
        Operand::Set(_) |
        Operand::List(_) |
        Operand::Tuple(_) |
        Operand::Column(_) |
        Operand::Func(_) => Vec::from(operand.to_string().as_bytes()),
        Operand::Null => vec!(),
    };

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

        RelationOperator::NotEqual |
        RelationOperator::In |
        RelationOperator::Contains |
        RelationOperator::ContainsKey |
        RelationOperator::IsNot => {
            return Err(anyhow!("Couldn't build query"));
        }
    }
    Ok(())
}

fn build_redis_frames_from_where_clause( where_clause : &[RelationElement], table_cache_schema: &TableCacheSchema)  -> Result<Vec<RedisFrame>> {
    let mut min: Vec<u8> = Vec::new();
    let mut max: Vec<u8> = Vec::new();
    let mut had_err = None;

    let where_columns  = WhereClause::get_column_relation_element_map( where_clause );

    // process the partition key
    where_columns.iter().filter(|(name,_relation_elements)| {
        ! table_cache_schema.partition_key.contains( name )
    }).for_each( |(_name,relation_elements)| {
            for relation_element in relation_elements {
                for operand in &relation_element.value {
                    let x = build_zrangebylex_min_max_from_cql3(&relation_element.oper, &operand, &mut min, &mut max, );
                    if x.is_err() {
                        had_err = x.err()
                    }
                }
            }
        });

    if let Some(e) = had_err {
        return Err(e);
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

    let pk = table_cache_schema
        .partition_key
        .iter()
        .filter_map(|k| {
            let x = where_columns.get(k);
            x?;
            let y = x.unwrap().iter().find(|x| x.oper == RelationOperator::Equal);
            y?;
            Some(&y.unwrap().value)
        })
        .fold(BytesMut::new(), |mut acc, v| {
            v.iter().for_each(|operand| acc.extend(MessageValue::from( operand ).into_str_bytes()));
            acc
        });
    Ok(vec![
        RedisFrame::BulkString("ZRANGEBYLEX".into()),
        RedisFrame::BulkString(pk.freeze()),
        RedisFrame::BulkString(min),
        RedisFrame::BulkString(max),
    ])
}
fn build_redis_ast_from_cql3 (
    statement: &CassandraStatement,
    table_cache_schema: &TableCacheSchema,
) -> Result<RedisFrame>
{
        match statement {
            CassandraStatement::Select(select) => {
                if select.where_clause.is_some() {
                    Ok(RedisFrame::Array( build_redis_frames_from_where_clause( select.where_clause.as_ref().unwrap(),table_cache_schema)?))
                } else {
                    Err(anyhow!("Cant build query from statement: {}", statement))
                }
            }
            CassandraStatement::Insert(insert) => {
                let value_map : HashMap<String,&Operand> = insert.get_value_map();
                let pk = table_cache_schema
                    .partition_key
                    .iter()
                    .map(|k| value_map.get(k.as_str()).unwrap())
                    .fold(BytesMut::new(), |mut acc, v| {
                        acc.extend(MessageValue::from(*v).into_str_bytes());
                        acc
                    });
                let redis_frames: Vec<RedisFrame> = vec![
                    RedisFrame::BulkString("ZADD".into()),
                    RedisFrame::BulkString(pk.freeze()),
                ];

                let mut map = HashMap::new();
                value_map.iter().for_each(|(key,value)| {map.insert( key.clone(), MessageValue::from( *value ));});
                Ok(RedisFrame::Array(add_values_to_redis_frames(table_cache_schema, map, redis_frames)))
            }
            CassandraStatement::Update(update) => {
                let redis_frames = build_redis_frames_from_where_clause( &update.where_clause, table_cache_schema)?;
                let mut map = HashMap::new();
                for x in &update.assignments {
                    // skip any columns with +/- modifiers.
                    if x.operator.is_none() {
                        map.insert(x.name.to_string(), MessageValue::from(&x.value));
                    }
                }

                Ok(RedisFrame::Array(add_values_to_redis_frames(table_cache_schema, map, redis_frames )))

            }
            statement => Err(anyhow!("Cant build query from statement: {}", statement)),
        }
}

fn add_values_to_redis_frames(
    table_cache_schema: &TableCacheSchema,
    query_values: HashMap<String, MessageValue>,
    mut redis_frames : Vec<RedisFrame>
) -> Vec<RedisFrame> {

    let clustering = table_cache_schema
        .range_key
        .iter()
        .map(|k| query_values.get(k.as_str()).unwrap())
        .fold(BytesMut::new(), |mut acc, message_value| {
            acc.extend(&message_value.clone().into_str_bytes());
            acc
        });

    query_values
        .iter()
        .filter_map(|(p, v)| {
            if !(table_cache_schema.partition_key.contains(p) ||
                table_cache_schema.range_key.contains( p))
            {
                None
            } else {
                Some(v)
            }
        })
        .for_each( |message_value| {
            redis_frames.push(RedisFrame::BulkString(Bytes::from_static(b"0")));
            let mut value = clustering.clone();
            if !value.is_empty() {
                value.put_u8(b':');
            }
            value.extend(message_value.clone().into_str_bytes());
            redis_frames.push(RedisFrame::BulkString(value.freeze()));
        });

    redis_frames
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
    use crate::frame::RedisFrame;
    use crate::transforms::chain::TransformChain;
    use crate::transforms::debug::printer::DebugPrinter;
    use crate::transforms::null::Null;
    use crate::transforms::redis::cache::{
        build_redis_ast_from_cql3, SimpleRedisCache, TableCacheSchema,
    };
    use crate::transforms::{Transform, Transforms};
    use bytes::Bytes;
    use std::collections::HashMap;
    use cql3_parser::cassandra_ast::CassandraAST;
    use cql3_parser::cassandra_statement::CassandraStatement;
    use tls_parser::nom::AsBytes;

    fn build_query(query_string: &str) -> CassandraStatement {
        let ast = CassandraAST::new( query_string );
        ast.statements[0].clone()
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

        if let RedisFrame::Array(v)= query {
            assert_eq!(4,v.len());
            let mut iter = v.iter();
            if let RedisFrame::BulkString( b ) = iter.next().unwrap() {
                assert_eq!( b"ZRANGEBYLEX", b.as_bytes());
            }
            if let RedisFrame::BulkString( b ) = iter.next().unwrap() {
                assert_eq!( b"1", b.as_bytes());
            }

            if let RedisFrame::BulkString( b ) = iter.next().unwrap() {
              if b.starts_with( b"[123:") {
                  assert_eq!(  b"[123:965", b.as_bytes());
              } else {
                  assert_eq!( b"[965:123", b.as_bytes());
              }
            } else {
                assert!(false);
            }

            if let RedisFrame::BulkString( b ) = iter.next().unwrap() {
                if b.starts_with( b"]123:") {
                    assert_eq!(  b"]123:965", b.as_bytes());
                } else {
                    assert_eq!( b"]965:123", b.as_bytes());
                }
            } else {
                assert!(false);
            }

        } else {
            assert!(false)
        }

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
