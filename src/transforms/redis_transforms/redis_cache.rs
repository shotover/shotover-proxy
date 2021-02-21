use core::fmt;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{ASTHolder, MessageDetails, Messages, QueryType, Value as ShotoverValue};
use crate::protocols::RawFrame;
use crate::transforms::chain::TransformChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, TransformsFromConfig, Wrapper,
};
use bytes::{BufMut, Bytes, BytesMut};
use cassandra_proto::frame::{Frame, Opcode};
use itertools::Itertools;
use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, Value};
use std::borrow::Borrow;

const TRUE: [u8; 1] = [0x1];
const FALSE: [u8; 1] = [0x0];

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisConfig {
    pub caching_schema: HashMap<String, PrimaryKey>,
    pub chain: Vec<TransformsConfig>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct PrimaryKey {
    partition_key: Vec<String>,
    range_key: Vec<String>,
}

impl PrimaryKey {
    #[cfg(test)]
    fn get_compound_key(&self) -> Vec<String> {
        let mut compound = Vec::new();
        compound.extend(self.partition_key.clone());
        compound.extend(self.range_key.clone());
        return compound;
    }
}

#[async_trait]
impl TransformsFromConfig for RedisConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::RedisCache(SimpleRedisCache {
            name: "SimpleRedisCache",
            cache_chain: build_chain_from_config("cache_chain".to_string(), &self.chain, &topics)
                .await?,
            caching_schema: self.caching_schema.clone(),
        }))
    }
}

#[derive(Clone)]
pub struct SimpleRedisCache {
    name: &'static str,
    cache_chain: TransformChain,
    caching_schema: HashMap<String, PrimaryKey>,
}

impl Debug for SimpleRedisCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Name: {}, conversions: {:?}",
            self.name, self.caching_schema
        )
    }
}

impl SimpleRedisCache {
    async fn get_or_update_from_cache(&mut self, mut messages: Messages) -> ChainResponse {
        for message in &mut messages.messages {
            match &mut message.details {
                MessageDetails::Query(ref mut qm) => {
                    let table_lookup = qm.namespace.join(".");
                    let table = self
                        .caching_schema
                        .get(&table_lookup)
                        .ok_or(anyhow!("not a caching table"))?;

                    let ast_ref = qm
                        .ast
                        .as_ref()
                        .ok_or(anyhow!("No AST to convert query to cache query"))?
                        .clone();

                    qm.ast.replace(build_redis_ast_from_sql(
                        ast_ref,
                        &qm.primary_key,
                        table,
                        &qm.query_values,
                    )?);
                }
                _ => return Err(anyhow!("cannot fetch from cache")),
            };
            message.modified = true;
        }

        self.cache_chain
            .process_request(Wrapper::new(messages), "cliebntdetailstodo".to_string())
            .await
    }
}

// TODO: We don't need to do it this way and allocate another struct
struct ValueHelper(Value);

impl ValueHelper {
    fn as_bytes(&self) -> &[u8] {
        return match &self.0 {
            Value::Number(v) => v.as_bytes(),
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
            Value::Date(v) => v.as_bytes(),
            Value::Time(v) => v.as_bytes(),
            Value::Timestamp(v) => v.as_bytes(),
            Value::Null => &[],
            _ => unreachable!(),
        };
    }
}

fn append_seperator(command_builder: &mut Vec<u8>) {
    let min_size = command_builder.len();
    let prev_char = command_builder.get_mut(min_size - 1).unwrap();

    // TODO this is super fragile and depends on hidden array values to signal whether we should build the query a certain way
    if min_size == 1 {
        if *prev_char == '-' as u8 {
            *prev_char = '[' as u8
        } else if *prev_char == '+' as u8 {
            *prev_char = ']' as u8
        }
    } else {
        command_builder.push(':' as u8);
    }
}

fn build_redis_commands(
    expr: &Expr,
    pks: &Vec<String>,
    min: &mut Vec<u8>,
    max: &mut Vec<u8>,
) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // first check if this is a related to PK
            if let Expr::Identifier(i) = left.borrow() {
                let id_string = i.to_string();
                if pks.iter().find(|&v| v == &id_string).is_some() {
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
                        let len = minrv.len();

                        let last_byte = minrv.get_mut(len - 1).unwrap();
                        *last_byte += 1;

                        append_seperator(min);
                        min.append(&mut minrv);
                    }
                }
                BinaryOperator::Lt => {
                    // we shift the value for Lt so that it works with other LtEq operators
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let mut maxrv = Vec::from(vh.as_bytes());
                        let len = maxrv.len();

                        let last_byte = maxrv.get_mut(len - 1).unwrap();
                        *last_byte -= 1;

                        append_seperator(max);
                        max.append(&mut maxrv);
                    }
                }
                BinaryOperator::GtEq => {
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let mut minrv = Vec::from(vh.as_bytes());

                        append_seperator(min);
                        min.append(&mut minrv);
                    }
                }
                BinaryOperator::LtEq => {
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let mut maxrv = Vec::from(vh.as_bytes());

                        append_seperator(max);
                        max.append(&mut maxrv);
                    }
                }
                BinaryOperator::Eq => {
                    if let Expr::Value(v) = right.borrow() {
                        let vh = ValueHelper(v.clone());

                        let mut minrv = Vec::from(vh.as_bytes());
                        let mut maxrv = minrv.clone();

                        append_seperator(min);
                        min.append(&mut minrv);

                        append_seperator(max);
                        max.append(&mut maxrv);
                    }
                }
                BinaryOperator::And => {
                    build_redis_commands(left, pks, min, max)?;
                    build_redis_commands(right, pks, min, max)?;
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
    primary_key_values: &HashMap<String, ShotoverValue>,
    pk_schema: &PrimaryKey,
    query_values: &Option<HashMap<String, ShotoverValue>>,
) -> Result<ASTHolder> {
    return match &mut ast {
        ASTHolder::SQL(sql) => match sql {
            Statement::Query(ref mut q) => match q.body {
                SetExpr::Select(ref mut s) if s.selection.is_some() => {
                    let expr = s.selection.as_mut().unwrap();
                    let mut commands_buffer: Vec<ShotoverValue> = Vec::new();
                    let mut min: Vec<u8> = Vec::new();
                    min.push('-' as u8);
                    let mut max: Vec<u8> = Vec::new();
                    max.push('+' as u8);

                    build_redis_commands(expr, &pk_schema.partition_key, &mut min, &mut max)?;

                    commands_buffer.push(ShotoverValue::Bytes("ZRANGEBYLEX".into()));
                    let pk = pk_schema
                        .partition_key
                        .iter()
                        .map(|k| primary_key_values.get(k).unwrap())
                        .fold(BytesMut::new(), |mut acc, v| {
                            acc.extend(v.clone().into_str_bytes());
                            acc
                        });
                    commands_buffer.push(ShotoverValue::Bytes(pk.freeze()));
                    commands_buffer.push(ShotoverValue::Bytes(Bytes::from(min)));
                    commands_buffer.push(ShotoverValue::Bytes(Bytes::from(max)));
                    Ok(ASTHolder::Commands(ShotoverValue::List(commands_buffer)))
                }
                _ => Err(anyhow!("Couldn't build query")),
            },
            Statement::Insert { .. } | Statement::Update { .. } => {
                let mut commands_buffer: Vec<ShotoverValue> = Vec::new();

                commands_buffer.push(ShotoverValue::Bytes("ZADD".into()));

                let pk = pk_schema
                    .partition_key
                    .iter()
                    .map(|k| primary_key_values.get(k).unwrap())
                    .fold(BytesMut::new(), |mut acc, v| {
                        acc.extend(v.clone().into_str_bytes());
                        acc
                    });
                commands_buffer.push(ShotoverValue::Bytes(pk.freeze()));

                let clustering = pk_schema
                    .range_key
                    .iter()
                    .map(|k| primary_key_values.get(k).unwrap())
                    .fold(BytesMut::new(), |mut acc, v| {
                        acc.extend(v.clone().into_str_bytes());
                        acc
                    });

                let values = query_values
                    .as_ref()
                    .ok_or(anyhow!("Couldn't build query"))?
                    .iter()
                    .filter_map(|(p, v)| {
                        return if !pk_schema.partition_key.contains(p)
                            && !pk_schema.range_key.contains(p)
                        {
                            Some(v)
                        } else {
                            None
                        };
                    })
                    .collect_vec();

                for v in values {
                    commands_buffer.push(ShotoverValue::Bytes(Bytes::from("0")));
                    let mut value = clustering.clone();
                    if value.len() != 0 {
                        value.put_u8(':' as u8);
                    }
                    value.extend(v.clone().into_str_bytes());
                    commands_buffer.push(ShotoverValue::Bytes(value.freeze()));
                }

                Ok(ASTHolder::Commands(ShotoverValue::List(commands_buffer)))
            }
            _ => Err(anyhow!("Couldn't build query")),
        },
        ASTHolder::Commands(_) => Ok(ast),
    };
}

#[async_trait]
impl Transform for SimpleRedisCache {
    async fn transform<'a>(&'a mut self, mut qd: Wrapper<'a>) -> ChainResponse {
        let mut updates = 0_i32;
        {
            for mut m in &mut qd.message.messages {
                if let RawFrame::CASSANDRA(Frame {
                    version: _,
                    flags: _,
                    opcode: Opcode::Query,
                    stream: _,
                    body: _,
                    tracing_id: _,
                    warnings: _,
                }) = &m.original
                {
                    m.generate_message_details(false);
                    if let MessageDetails::Query(qm) = &m.details {
                        if qm.query_type == QueryType::Write {
                            updates += 1;
                        }
                    }
                }
            }
        }

        return if updates == 0 {
            match self.get_or_update_from_cache(qd.message.clone()).await {
                Ok(cr) => Ok(cr),
                Err(_e) => qd.call_next_transform().await,
            }
        } else {
            let (_cache_res, upstream) = tokio::join!(
                self.get_or_update_from_cache(qd.message.clone()),
                qd.call_next_transform()
            );
            return upstream;
        };
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod test {
    use crate::message::{ASTHolder, MessageDetails, Value};
    use crate::message::{Messages, Value as ShotoverValue};
    use crate::protocols::cassandra_protocol2::CassandraCodec2;
    use crate::protocols::redis_codec::RedisCodec;
    use crate::transforms::redis_transforms::redis_cache::{build_redis_ast_from_sql, PrimaryKey};
    use anyhow::anyhow;
    use anyhow::Result;
    use bytes::BytesMut;
    use itertools::Itertools;
    use std::collections::HashMap;
    use tokio_util::codec::Decoder;

    fn build_query(
        query_string: &str,
        pk_col_map: &HashMap<String, Vec<String>>,
    ) -> Result<(ASTHolder, Option<HashMap<String, Value>>)> {
        let res = CassandraCodec2::parse_query_string(query_string.to_string(), pk_col_map);
        Ok((ASTHolder::SQL(res.ast.unwrap()), res.colmap))
    }

    fn build_redis_query_frame(query: &str) -> Result<ASTHolder> {
        let mut codec = RedisCodec::new(false, 0);

        let final_command_string = build_redis_string(query);

        let mut final_command_bytes: BytesMut = final_command_string.as_str().into();
        let mut frame: Messages = codec.decode(&mut final_command_bytes)?.unwrap();
        frame
            .messages
            .iter_mut()
            .for_each(|m| m.generate_message_details(false));
        return if let MessageDetails::Query(qm) = frame.messages.remove(0).details {
            qm.ast.ok_or(anyhow!("woops"))
        } else {
            Err(anyhow!("woops"))
        };
    }

    fn build_redis_string(query: &str) -> String {
        let query_string = query.to_string();
        let tokens = query_string.split_ascii_whitespace().collect_vec();
        let mut command_buffer: Vec<String> = Vec::new();
        command_buffer.push(format!("*{}\r\n", tokens.len()));

        for token in query_string.to_string().split_ascii_whitespace() {
            command_buffer.push(format!("${}\r\n", token.len()));
            command_buffer.push(format!("{}\r\n", token));
        }

        let final_command_string: String = command_buffer.join("");
        final_command_string
    }

    #[test]
    fn test_build_redis_query_string() -> Result<()> {
        assert_eq!(
            "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n".to_string(),
            build_redis_string("LLEN mylist")
        );
        Ok(())
    }

    #[test]
    fn equal_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));

        let pk_holder = PrimaryKey {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_col_map.insert("foo".to_string(), pk_holder.get_compound_key());

        let (ast, query_values) = build_query(
            "SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965",
            &pk_col_map,
        )?;

        let query = build_redis_ast_from_sql(ast, &pks, &pk_holder, &query_values)?;

        let expected = build_redis_query_frame("ZRANGEBYLEX 1 [123:965 ]123:965")?;

        assert_eq!(expected, query);

        Ok(())
    }

    #[test]
    fn insert_simple_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));

        let pk_holder = PrimaryKey {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_col_map.insert("foo".to_string(), pk_holder.get_compound_key());

        let (ast, query_values) =
            build_query("INSERT INTO foo (z, v) VALUES (1, 123)", &pk_col_map)?;

        let query = build_redis_ast_from_sql(ast, &pks, &pk_holder, &query_values)?;

        let expected = build_redis_query_frame("ZADD 1 0 123")?;

        assert_eq!(expected, query);

        Ok(())
    }

    #[test]
    fn insert_simple_clustering_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));
        pks.insert("c".to_string(), ShotoverValue::Strings("yo".to_string()));

        let pk_holder = PrimaryKey {
            partition_key: vec!["z".to_string()],
            range_key: vec!["c".to_string()],
        };

        let mut pk_col_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_col_map.insert("foo".to_string(), pk_holder.get_compound_key());

        let (ast, query_values) = build_query(
            "INSERT INTO foo (z, c, v) VALUES (1, 'yo' , 123)",
            &pk_col_map,
        )?;

        let query = build_redis_ast_from_sql(ast, &pks, &pk_holder, &query_values)?;

        let expected = build_redis_query_frame("ZADD 1 0 yo:123")?;

        assert_eq!(expected, query);

        Ok(())
    }

    #[test]
    fn update_simple_clustering_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));
        pks.insert("c".to_string(), ShotoverValue::Strings("yo".to_string()));

        let pk_holder = PrimaryKey {
            partition_key: vec!["z".to_string()],
            range_key: vec!["c".to_string()],
        };

        let mut pk_col_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_col_map.insert("foo".to_string(), pk_holder.get_compound_key());

        let (ast, query_values) =
            build_query("UPDATE foo SET c = 'yo', v = 123 WHERE z = 1", &pk_col_map)?;

        let query = build_redis_ast_from_sql(ast, &pks, &pk_holder, &query_values)?;

        let expected = build_redis_query_frame("ZADD 1 0 yo:123")?;

        assert_eq!(expected, query);

        Ok(())
    }

    #[test]
    fn check_deterministic_order_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));
        let pk_holder = PrimaryKey {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_col_map.insert("foo".to_string(), pk_holder.get_compound_key());

        let (ast, query_values) = build_query(
            "SELECT * FROM foo WHERE z = 1 AND x = 123 AND y = 965",
            &pk_col_map,
        )?;

        let query_one = build_redis_ast_from_sql(ast, &pks, &pk_holder, &query_values)?;

        let (ast, query_values) = build_query(
            "SELECT * FROM foo WHERE y = 965 AND z = 1 AND x = 123",
            &pk_col_map,
        )?;

        let query_two = build_redis_ast_from_sql(ast, &pks, &pk_holder, &query_values)?;

        println!("{:#?}", query_one);
        println!("{:#?}", query_two);

        // Semantically databases treat the order of AND clauses differently, Cassandra however requires clustering key predicates be in order
        // So here we will just expect the order is correct in the query. TODO: we may need to revisit this as support for other databases is added
        assert_ne!(query_one, query_two);

        Ok(())
    }

    #[test]
    fn range_exclusive_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));
        let pk_holder = PrimaryKey {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_col_map.insert("foo".to_string(), pk_holder.get_compound_key());

        let (ast, query_values) = build_query(
            "SELECT * FROM foo WHERE z = 1 AND x > 123 AND x < 999",
            &pk_col_map,
        )?;

        let query = build_redis_ast_from_sql(ast, &pks, &pk_holder, &query_values)?;

        let expected = build_redis_query_frame("ZRANGEBYLEX 1 [124 ]998")?;

        assert_eq!(expected, query);

        println!("{:#?}", query);

        Ok(())
    }

    #[test]
    fn range_inclusive_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));
        let pk_holder = PrimaryKey {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_col_map.insert("foo".to_string(), pk_holder.get_compound_key());

        let (ast, query_values) = build_query(
            "SELECT * FROM foo WHERE z = 1 AND x >= 123 AND x <= 999",
            &pk_col_map,
        )?;

        let query = build_redis_ast_from_sql(ast, &pks, &pk_holder, &query_values)?;

        let expected = build_redis_query_frame("ZRANGEBYLEX 1 [123 ]999")?;

        assert_eq!(expected, query);

        println!("{:#?}", query);

        Ok(())
    }

    #[test]
    fn single_pk_only_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));
        let pk_holder = PrimaryKey {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_col_map.insert("foo".to_string(), pk_holder.get_compound_key());

        let (ast, query_values) = build_query("SELECT * FROM foo WHERE z = 1", &pk_col_map)?;

        let query = build_redis_ast_from_sql(ast, &pks, &pk_holder, &query_values)?;

        let expected = build_redis_query_frame("ZRANGEBYLEX 1 - +")?;

        assert_eq!(expected, query);

        println!("{:#?}", query);

        Ok(())
    }

    #[test]
    fn compound_pk_only_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));
        pks.insert("y".to_string(), ShotoverValue::Integer(2));
        let pk_holder = PrimaryKey {
            partition_key: vec!["z".to_string(), "y".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_col_map.insert("foo".to_string(), pk_holder.get_compound_key());

        let (ast, query_values) =
            build_query("SELECT * FROM foo WHERE z = 1 AND y = 2", &pk_col_map)?;

        let query = build_redis_ast_from_sql(ast, &pks, &pk_holder, &query_values)?;

        let expected = build_redis_query_frame("ZRANGEBYLEX 12 - +")?;

        assert_eq!(expected, query);

        println!("{:#?}", query);

        Ok(())
    }

    #[test]
    fn open_range_test() -> Result<()> {
        let mut pks: HashMap<String, ShotoverValue> = HashMap::new();
        pks.insert("z".to_string(), ShotoverValue::Integer(1));
        let pk_holder = PrimaryKey {
            partition_key: vec!["z".to_string()],
            range_key: vec![],
        };

        let mut pk_col_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_col_map.insert("foo".to_string(), pk_holder.get_compound_key());

        let (ast, query_values) =
            build_query("SELECT * FROM foo WHERE z = 1 AND x >= 123", &pk_col_map)?;

        let query = build_redis_ast_from_sql(ast, &pks, &pk_holder, &query_values)?;

        let expected = build_redis_query_frame("ZRANGEBYLEX 1 [123 +")?;

        assert_eq!(expected, query);

        let mut pk_col_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_col_map.insert("foo".to_string(), pk_holder.get_compound_key());

        let (ast, query_values) =
            build_query("SELECT * FROM foo WHERE z = 1 AND x <= 123", &pk_col_map)?;

        let query = build_redis_ast_from_sql(ast, &pks, &pk_holder, &query_values)?;

        let expected = build_redis_query_frame("ZRANGEBYLEX 1 - ]123")?;

        assert_eq!(expected, query);

        println!("{:#?}", query);

        Ok(())
    }
}
