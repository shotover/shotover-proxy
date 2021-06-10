use std::collections::HashMap;

use crate::{MessageDetails, QueryMessage, QueryResponse, QueryType, Value};
use anyhow::Result;
use bytes::Bytes;
use itertools::Itertools;
use redis_protocol::prelude::Frame as RedisFrame;
use sqlparser::ast::Statement;

#[derive(PartialEq, Debug, Clone)]
pub enum ASTHolder {
    SQL(Statement),
    Commands(Value), // A flexible representation of a structured query that will naturally convert into the required type via into/from traits
}

impl ASTHolder {
    pub fn get_command(&self) -> String {
        match self {
            ASTHolder::SQL(statement) => {
                return match statement {
                    Statement::Query(_) => "SELECT",
                    Statement::Insert { .. } => "INSERT",
                    Statement::Update { .. } => "UPDATE",
                    Statement::Delete { .. } => "DELETE",
                    Statement::CreateView { .. } => "CREATE VIEW",
                    Statement::CreateTable { .. } => "CREATE TABLE",
                    Statement::AlterTable { .. } => "ALTER TABLE",
                    Statement::Drop { .. } => "DROP",
                    _ => "UKNOWN",
                }
                .to_string();
            }
            ASTHolder::Commands(commands) => {
                if let Value::List(coms) = commands {
                    if let Some(Value::Bytes(b)) = coms.get(0) {
                        return String::from_utf8(b.to_vec())
                            .unwrap_or_else(|_| "couldn't decode".to_string());
                    }
                }
            }
        }
        "UNKNOWN".to_string()
    }
}

#[inline]
pub(crate) fn redis_query_type(frame: &RedisFrame) -> QueryType {
    if let RedisFrame::Array(frames) = frame {
        if let Some(RedisFrame::BulkString(bytes)) = frames.get(0) {
            return match bytes.as_ref() {
                b"APPEND" | b"BITCOUNT" | b"STRLEN" | b"GET" | b"GETRANGE" | b"MGET"
                | b"LRANGE" | b"LINDEX" | b"LLEN" | b"SCARD" | b"SISMEMBER" | b"SMEMBERS"
                | b"SUNION" | b"SINTER" | b"ZCARD" | b"ZCOUNT" | b"ZRANGE" | b"ZRANK"
                | b"ZSCORE" | b"ZRANGEBYSCORE" | b"HGET" | b"HGETALL" | b"HEXISTS" | b"HKEYS"
                | b"HLEN" | b"HSTRLEN" | b"HVALS" | b"PFCOUNT" => QueryType::Read,
                _ => QueryType::Write,
            };
        }
    }
    QueryType::Write
}

fn get_keys(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    commands: &mut Vec<RedisFrame>,
) -> Result<()> {
    let mut keys_storage: Vec<Value> = vec![];
    while !commands.is_empty() {
        if let Some(RedisFrame::BulkString(v)) = commands.pop() {
            let key = String::from_utf8_lossy(v.as_ref()).to_string();
            fields.insert(key.clone(), Value::None);
            keys_storage.push(RedisFrame::BulkString(v).into());
        }
    }
    keys.insert("key".to_string(), Value::List(keys_storage));
    Ok(())
}

fn get_key_multi_values(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    commands: &mut Vec<RedisFrame>,
) -> Result<()> {
    let mut keys_storage: Vec<Value> = vec![];
    if let Some(RedisFrame::BulkString(v)) = commands.pop() {
        let key = String::from_utf8_lossy(v.as_ref()).to_string();
        keys_storage.push(RedisFrame::BulkString(v).into());
        let mut values: Vec<Value> = vec![];
        while !commands.is_empty() {
            if let Some(frame) = commands.pop() {
                values.push(frame.into());
            }
        }
        fields.insert(key, Value::List(values));
        keys.insert("key".to_string(), Value::List(keys_storage));
    }
    Ok(())
}

fn get_key_map(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    commands: &mut Vec<RedisFrame>,
) -> Result<()> {
    let mut keys_storage: Vec<Value> = vec![];
    if let Some(RedisFrame::BulkString(v)) = commands.pop() {
        let key = String::from_utf8_lossy(v.as_ref()).to_string();
        keys_storage.push(RedisFrame::BulkString(v).into());

        let mut values: HashMap<String, Value> = HashMap::new();
        while !commands.is_empty() {
            if let Some(RedisFrame::BulkString(field)) = commands.pop() {
                if let Some(frame) = commands.pop() {
                    values.insert(
                        String::from_utf8_lossy(field.as_ref()).to_string(),
                        frame.into(),
                    );
                }
            }
        }
        fields.insert(key, Value::Document(values));
        keys.insert("key".to_string(), Value::List(keys_storage));
    }
    Ok(())
}

fn get_key_values(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    commands: &mut Vec<RedisFrame>,
) -> Result<()> {
    let mut keys_storage: Vec<Value> = vec![];
    while !commands.is_empty() {
        if let Some(RedisFrame::BulkString(k)) = commands.pop() {
            let key = String::from_utf8_lossy(k.as_ref()).to_string();
            keys_storage.push(RedisFrame::BulkString(k).into());
            if let Some(frame) = commands.pop() {
                fields.insert(key, frame.into());
            }
        }
    }
    keys.insert("key".to_string(), Value::List(keys_storage));
    Ok(())
}

fn handle_redis_array(
    commands_vec: Vec<RedisFrame>,
    decode_as_request: bool,
) -> anyhow::Result<MessageDetails> {
    if decode_as_request {
        let mut keys_map: HashMap<String, Value> = HashMap::new();
        let mut values_map: HashMap<String, Value> = HashMap::new();
        let values = &mut values_map;
        let keys = &mut keys_map;
        let mut query_type: QueryType = QueryType::Write;
        let mut commands_reversed: Vec<RedisFrame> =
            commands_vec.iter().cloned().rev().collect_vec();
        let query_string = commands_vec
            .iter()
            .filter_map(|f| f.as_str())
            .map(|s| s.to_string())
            .collect_vec()
            .join(" ");

        let ast = ASTHolder::Commands(Value::List(
            commands_vec.iter().cloned().map(|f| f.into()).collect_vec(),
        ));

        let commands = &mut commands_reversed;

        // This should be a command from the server
        // Behaviour cribbed from:
        // https://redis.io/commands and
        // https://gist.github.com/LeCoupa/1596b8f359ad8812c7271b5322c30946
        if let Some(RedisFrame::BulkString(v)) = commands.pop() {
            let comm = String::from_utf8_lossy(v.as_ref())
                .to_string()
                .to_uppercase();
            match comm.as_str() {
                "APPEND" => {
                    get_key_values(values, keys, commands)?;
                } // append a value to a key
                "BITCOUNT" => {
                    query_type = QueryType::Read;
                    get_key_values(values, keys, commands)?;
                } // count set bits in a string
                "SET" => {
                    get_key_values(values, keys, commands)?;
                } // set value in key
                "SETNX" => {
                    get_key_values(values, keys, commands)?;
                } // set if not exist value in key
                "SETRANGE" => {
                    get_key_values(values, keys, commands)?;
                } // overwrite part of a string at key starting at the specified offset
                "STRLEN" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // get the length of the value stored in a key
                "MSET" => {
                    get_key_values(values, keys, commands)?;
                } // set multiple keys to multiple values
                "MSETNX" => {
                    get_key_values(values, keys, commands)?;
                } // set multiple keys to multiple values, only if none of the keys exist
                "GET" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // get value in key
                "GETRANGE" => {
                    query_type = QueryType::Read;
                    get_key_values(values, keys, commands)?;
                } // get a substring value of a key and return its old value
                "MGET" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // get the values of all the given keys
                "INCR" => {
                    get_keys(values, keys, commands)?;
                } // increment value in key
                "INCRBY" => {
                    get_key_values(values, keys, commands)?;
                } // increment the integer value of a key by the given amount
                "INCRBYFLOAT" => {
                    get_key_values(values, keys, commands)?;
                } // increment the float value of a key by the given amount
                "DECR" => {
                    get_keys(values, keys, commands)?;
                } // decrement the integer value of key by one
                "DECRBY" => {
                    get_key_values(values, keys, commands)?;
                } // decrement the integer value of a key by the given number
                "DEL" => {
                    get_keys(values, keys, commands)?;
                } // delete key
                "EXPIRE" => {
                    get_key_values(values, keys, commands)?;
                } // key will be deleted in 120 seconds
                "TTL" => {
                    get_keys(values, keys, commands)?;
                } // returns the number of seconds until a key is deleted
                "RPUSH" => {
                    get_key_multi_values(values, keys, commands)?;
                } // put the new value at the end of the list
                "RPUSHX" => {
                    get_key_values(values, keys, commands)?;
                } // append a value to a list, only if the exists
                "LPUSH" => {
                    get_key_multi_values(values, keys, commands)?;
                } // put the new value at the start of the list
                "LRANGE" => {
                    query_type = QueryType::Read;
                    get_key_multi_values(values, keys, commands)?;
                } // give a subset of the list
                "LINDEX" => {
                    query_type = QueryType::Read;
                    get_key_multi_values(values, keys, commands)?;
                } // get an element from a list by its index
                "LINSERT" => {
                    get_key_multi_values(values, keys, commands)?;
                } // insert an element before or after another element in a list
                "LLEN" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // return the current length of the list
                "LPOP" => {
                    get_keys(values, keys, commands)?;
                } // remove the first element from the list and returns it
                "LSET" => {
                    get_key_multi_values(values, keys, commands)?;
                } // set the value of an element in a list by its index
                "LTRIM" => {
                    get_key_multi_values(values, keys, commands)?;
                } // trim a list to the specified range
                "RPOP" => {
                    get_keys(values, keys, commands)?;
                } // remove the last element from the list and returns it
                "SADD" => {
                    get_key_multi_values(values, keys, commands)?;
                } // add the given value to the set
                "SCARD" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // get the number of members in a set
                "SREM" => {
                    get_key_multi_values(values, keys, commands)?;
                } // remove the given value from the set
                "SISMEMBER" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // test if the given value is in the set.
                "SMEMBERS" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // return a list of all the members of this set
                "SUNION" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // combine two or more sets and returns the list of all elements
                "SINTER" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // intersect multiple sets
                "SMOVE" => {
                    query_type = QueryType::Write;
                    get_key_values(values, keys, commands)?;
                } // move a member from one set to another
                "SPOP" => {
                    query_type = QueryType::Write;
                    get_key_values(values, keys, commands)?;
                } // remove and return one or multiple random members from a set
                "ZADD" => {
                    get_key_multi_values(values, keys, commands)?;
                } // add one or more members to a sorted set, or update its score if it already exists
                "ZCARD" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // get the number of members in a sorted set
                "ZCOUNT" => {
                    query_type = QueryType::Read;
                    get_key_multi_values(values, keys, commands)?;
                } // count the members in a sorted set with scores within the given values
                "ZINCRBY" => {
                    get_key_multi_values(values, keys, commands)?;
                } // increment the score of a member in a sorted set
                "ZRANGE" => {
                    query_type = QueryType::Read;
                    get_key_multi_values(values, keys, commands)?;
                } // returns a subset of the sorted set
                "ZRANK" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // determine the index of a member in a sorted set
                "ZREM" => {
                    get_key_multi_values(values, keys, commands)?;
                } // remove one or more members from a sorted set
                "ZREMRANGEBYRANK" => {
                    get_key_multi_values(values, keys, commands)?;
                } // remove all members in a sorted set within the given indexes
                "ZREMRANGEBYSCORE" => {
                    get_key_multi_values(values, keys, commands)?;
                } // remove all members in a sorted set, by index, with scores ordered from high to low
                "ZSCORE" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // get the score associated with the given mmeber in a sorted set
                "ZRANGEBYSCORE" => {
                    query_type = QueryType::Read;
                    get_key_multi_values(values, keys, commands)?;
                } // return a range of members in a sorted set, by score
                "HGET" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // get the value of a hash field
                "HGETALL" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // get all the fields and values in a hash
                "HSET" => {
                    get_key_map(values, keys, commands)?;
                } // set the string value of a hash field
                "HSETNX" => {
                    get_key_map(values, keys, commands)?;
                } // set the string value of a hash field, only if the field does not exists
                "HMSET" => {
                    get_key_map(values, keys, commands)?;
                } // set multiple fields at once
                "HINCRBY" => {
                    get_key_multi_values(values, keys, commands)?;
                } // increment value in hash by X
                "HDEL" => {
                    get_key_multi_values(values, keys, commands)?;
                } // delete one or more hash fields
                "HEXISTS" => {
                    query_type = QueryType::Read;
                    get_key_values(values, keys, commands)?;
                } // determine if a hash field exists
                "HKEYS" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // get all the fields in a hash
                "HLEN" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // get all the fields in a hash
                "HSTRLEN" => {
                    query_type = QueryType::Read;
                    get_key_values(values, keys, commands)?;
                } // get the length of the value of a hash field
                "HVALS" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // get all the values in a hash
                "PFADD" => {
                    get_key_multi_values(values, keys, commands)?;
                } // add the specified elements to the specified HyperLogLog
                "PFCOUNT" => {
                    query_type = QueryType::Read;
                    get_keys(values, keys, commands)?;
                } // return the approximated cardinality of the set(s) observed by the HyperLogLog at key's)
                "PFMERGE" => {
                    get_key_multi_values(values, keys, commands)?;
                } // merge N HyperLogLogs into a single one
                _ => {}
            }
            return Ok(MessageDetails::Query(QueryMessage {
                query_string,
                namespace: vec![],
                primary_key: keys_map,
                query_values: Some(values_map),
                projection: None,
                query_type,
                ast: Some(ast),
            }));
        }
    } else {
        return Ok(MessageDetails::Response(QueryResponse {
            matching_query: None,
            result: Some(Value::List(
                commands_vec.iter().map(|f| f.clone().into()).collect_vec(),
            )),
            error: None,
            response_meta: None,
        }));
    }
    Ok(MessageDetails::Bypass(Box::new(MessageDetails::Unknown)))
}

fn handle_redis_string(string: String, decode_as_request: bool) -> anyhow::Result<MessageDetails> {
    if !decode_as_request {
        Ok(MessageDetails::Response(QueryResponse {
            matching_query: None,
            result: Some(Value::Strings(string)),
            error: None,
            response_meta: None,
        }))
    } else {
        Ok(MessageDetails::Query(QueryMessage {
            query_string: string,
            namespace: vec![],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::ReadWrite,
            ast: None,
        }))
    }
}

fn handle_redis_bulkstring(
    bulkstring: Bytes,
    decode_as_request: bool,
) -> anyhow::Result<MessageDetails> {
    if !decode_as_request {
        Ok(MessageDetails::Response(QueryResponse {
            matching_query: None,
            result: Some(Value::Bytes(bulkstring)),
            error: None,
            response_meta: None,
        }))
    } else {
        Ok(MessageDetails::Query(QueryMessage {
            query_string: String::from_utf8_lossy(bulkstring.as_ref()).to_string(),
            namespace: vec![],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::ReadWrite,
            ast: None,
        }))
    }
}

fn handle_redis_integer(integer: i64, decode_as_request: bool) -> anyhow::Result<MessageDetails> {
    if !decode_as_request {
        Ok(MessageDetails::Response(QueryResponse {
            matching_query: None,
            result: Some(Value::Integer(integer)),
            error: None,
            response_meta: None,
        }))
    } else {
        Ok(MessageDetails::Query(QueryMessage {
            query_string: format!("{}", integer),
            namespace: vec![],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::ReadWrite,
            ast: None,
        }))
    }
}

fn handle_redis_error(error: String, decode_as_request: bool) -> anyhow::Result<MessageDetails> {
    if !decode_as_request {
        Ok(MessageDetails::Response(QueryResponse {
            matching_query: None,
            result: None,
            error: Some(Value::Strings(error)),
            response_meta: None,
        }))
    } else {
        Ok(MessageDetails::Query(QueryMessage {
            query_string: error,
            namespace: vec![],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::ReadWrite,
            ast: None,
        }))
    }
}

pub fn process_redis_frame(frame: &RedisFrame, response: bool) -> Result<MessageDetails> {
    let decode_as_request = !response;
    match frame.clone() {
        RedisFrame::SimpleString(s) => handle_redis_string(s, decode_as_request),
        RedisFrame::BulkString(bs) => handle_redis_bulkstring(bs, decode_as_request),
        RedisFrame::Array(frames) => handle_redis_array(frames, decode_as_request),
        RedisFrame::Moved { slot, host, port } => handle_redis_string(
            format!("MOVED {} {}:{}", slot, host, port),
            decode_as_request,
        ),
        RedisFrame::Ask { slot, host, port } => {
            handle_redis_string(format!("ASK {} {}:{}", slot, host, port), decode_as_request)
        }
        RedisFrame::Integer(i) => handle_redis_integer(i, decode_as_request),
        RedisFrame::Error(s) => handle_redis_error(s, decode_as_request),
        RedisFrame::Null => {
            return if decode_as_request {
                Ok(MessageDetails::Response(QueryResponse::empty()))
            } else {
                Ok(MessageDetails::Query(QueryMessage::empty()))
            }
        }
    }
}
