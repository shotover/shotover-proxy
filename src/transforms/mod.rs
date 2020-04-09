use std::collections::HashMap;
use crate::message::Message::{Query as MessageQuery};
use crate::message::{Message, QueryResponse};
use crate::message::QueryType;
use redis::{AsyncCommands, RedisFuture, pipe, RedisResult};
use sqlparser::dialect::GenericDialect;
use sqlparser::ast::Statement::*;
use sqlparser::ast::{SetExpr::Values, Expr, SetExpr, Expr::Value as EValue};
use sqlparser::ast::Value;
use std::iter::{Iterator};
use futures::future::{join, Join};

pub mod chain;

use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};
use std::borrow::{Borrow, BorrowMut};
use redis::aio::{MultiplexedConnection};
use futures::executor::block_on;
use sqlparser::ast::Expr::{BinaryOp, Identifier};
use std::future::Future;
use std::cell::{RefCell, RefMut};
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct Forward {
    name: &'static str,
}

impl Forward {
    pub fn new() -> Forward {
        Forward{
            name: "Forward",
        }
    }
}

#[derive(Debug, Clone)]
pub struct NoOp {
    name: &'static str,
}

impl NoOp {
    pub fn new() -> NoOp {
        NoOp{
            name: "NoOp",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Printer {
    name: &'static str,
}

impl Printer {
    pub fn new() -> Printer {
        Printer{
            name: "Printer",
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryTypeFilter {
    name: &'static str,
    filters: Vec<QueryType>,
}

impl QueryTypeFilter {
    pub fn new(nfilters: Vec<QueryType>) -> QueryTypeFilter {
        QueryTypeFilter{
            name: "QueryFilter",
            filters: nfilters,
        }
    }
}

impl<'a, 'c> Transform<'a, 'c> for Forward {
    fn transform(&self, mut qd: Wrapper<'c>, t: & TransformChain<'a,'c>) -> ChainResponse<'c> {
        return ChainResponse::Ok(qd.message.clone());
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

impl<'a, 'c> Transform<'a, 'c> for NoOp {
    fn transform(&self, mut qd: Wrapper<'c>, t: & TransformChain<'a,'c>) -> ChainResponse<'c> {
        return self.call_next_transform(qd, t)
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

impl<'a, 'c> Transform<'a, 'c> for Printer {
    fn transform(&self, mut qd: Wrapper<'c>, t: & TransformChain<'a,'c>) -> ChainResponse<'c> {
        println!("Message content: {:?}", qd.message);
        return self.call_next_transform(qd, t)
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

impl<'a, 'c> Transform<'a, 'c> for QueryTypeFilter {
    fn transform(&self, mut qd: Wrapper<'c>, t: & TransformChain<'a,'c>) -> ChainResponse<'c> {
        // TODO this is likely the wrong way to get around the borrow from the match statement
        let message  = qd.message.borrow();

        return match message {
            Message::Query(q) => {
                if self.filters.iter().any(|x| *x == q.query_type) {
                    return ChainResponse::Ok(Message::Response(QueryResponse::empty()))
                }
                self.call_next_transform(qd, t)
            }
            _ => self.call_next_transform(qd, t)
        }
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

pub struct SimpleRedisCache {
    name: &'static str,
    con: MultiplexedConnection,
    tables_to_pks: HashMap<String, Vec<String>>,
}

impl SimpleRedisCache {
    //"redis://127.0.0.1/"
    pub fn new(uri: & String, connection: MultiplexedConnection) -> SimpleRedisCache {
            return SimpleRedisCache {
                name: "SimpleRedisCache",
                con: connection,
                tables_to_pks: HashMap::new(),
            };
    }


    // TODO: learn rust macros as this will probably make parsing ASTs a million times easier
    fn getColumnValues(&self, expr: &SetExpr) -> Vec<String> {
        let mut cumulator: Vec<String> = Vec::new();
         match expr {
             Values(v) => {
                 for value in &v.0 {
                     for ex in value {
                         match ex {
                             EValue(v) => {
                                 cumulator.push(expr_to_string(v).clone());
                             },
                             _ => {}
                         }
                     }
                 }
             },
             _ => {}
         }
        return cumulator;
    }

}

fn expr_to_string<'a>(v: &'a Value) -> String {
    return match v {
        Value::Number(v) |
        Value::SingleQuotedString(v) |
        Value::NationalStringLiteral(v) |
        Value::HexStringLiteral(v) |
        Value::Date(v) |
        Value::Time(v) |
        Value::Timestamp(v) => {
            format!("{:?}", v)
        },
        Value::Boolean(v) => {
            format!("{:?}", v)
        }
        _ => {
            "NULL".to_string()
        }
    }
}

fn build_key(namespace: String, pks: &Vec<String>, col_map: &HashMap<String, &String>) -> String {
    let mut s: String = String::new();
    s.push_str(namespace.as_str());
    for pk in pks {
        if let Some(v) = col_map.get(pk) {
            s.push_str(v);
        }
    }
    return s;
}


fn binary_ops_to_hashmap<'a>(node: &'a Expr, map: &'a Rc<RefCell<HashMap<String, String>>>)  {
    if let BinaryOp{left, op, right} = node {
        match op {
            AND => {
                binary_ops_to_hashmap(left, map);
                binary_ops_to_hashmap(right, map);
            }
            EQUAL=> {
                if let Identifier(i) = left.borrow() {
                    if let EValue(v) = right.borrow() {
                        let mut mut_map: RefMut<_> = (**map).borrow_mut();
                        mut_map.insert(i.to_string(), expr_to_string(v));
                    }

                }
            }
        }
    }
}


struct CassandraEnrichment {
    name: &'static str,
}

// impl Transform for CassandraEnrichment {
//     fn transform(&self, qd: &mut Wrapper, t: &TransformChain) -> ChainResponse {
//         match qd.message.borrow() {
//             MessageQuery(qm) => {
//                 qm.
//             },
//             MessageResponse(qr) => {}
//         }
//
//
//         let dialect = GenericDialect {}; //TODO write CQL dialect
//
//     }
//
//     fn get_name(&self) -> &'static str {
//         return self.name;
//     }
// }



impl<'a, 'c> Transform<'a, 'c> for SimpleRedisCache {
    fn transform(&self, mut qd: Wrapper<'c>, t: & TransformChain<'a,'c>) -> ChainResponse<'c> {
        let message  = qd.message.borrow();
        let dialect = GenericDialect {}; //TODO write CQL dialect
        
        // Only handle client requests
        if let MessageQuery(qm) = message {
            for statement in qm.ast.iter() {
                match statement {
                    /*
                     Query String: SELECT id, lastname, teams FROM cycling.cyclist_career_teams WHERE id='5b6962dd-3f90-4c93-8f61-eabfa4a803e2'
                     AST: [Query(Query {
                        ctes: [],
                        body: Select(Select {
                            distinct: false,
                            projection: [UnnamedExpr(Identifier("id")), UnnamedExpr(Identifier("lastname")), UnnamedExpr(Identifier("teams"))],
                            from: [TableWithJoins {
                                relation: Table {
                                    name: ObjectName(["cycling", "cyclist_career_teams"]),
                                    alias: None, args: [],
                                    with_hints: [] },
                                joins: [] }],
                            selection:
                                    Some(BinaryOp {
                                        left: Identifier("id"),
                                        op: Eq,
                                        right: Value(SingleQuotedString("5b6962dd-3f90-4c93-8f61-eabfa4a803e2")) }),
                                group_by: [],
                                having: None }),
                        order_by: [],
                        limit: None,
                        offset: None,
                        fetch: None
                    })]

                    */
                    Query(q) => {
                        let mut client_copy = self.con.clone();

                        //TODO: something something what happens if hset fails.
                        // let f: RedisFuture<HashMap<String, String>> = client_copy.hgetall(&qm.get_primary_key());
                        let mut p = &mut pipe();
                        if let Some(pk) = qm.get_primary_key() {
                            if let Some(values) = &qm.projection {
                                for v in values {
                                    if let None = qm.primary_key.get(v) {
                                        p.hget(&pk, v).ignore();
                                    }
                                }
                            }
                        }


                        // let f: RedisFuture<()>  = client_copy.hset_multiple(build_key(namespace,pk_cols,&colmap), );
                        let result_vec: Vec<Vec<String>> = Vec::new();
                        let result: RedisResult<()> = block_on(p.query_async(&mut client_copy));

                        // if let Ok(result) =  {
                        //
                        //     return ChainResponse::Ok(Message::Response(QueryResponse{
                        //         original: RawFrame::NONE,
                        //         result: Some(IValue::Rows(Vec::new())), //todo: Translate function
                        //         error: None,
                        //     }))
                        // }

                        return self.call_next_transform(qd, t)
                    },

                    /*
                    Query String: INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES ('6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47', 'KRUIKSWIJK', 'Steven')
                    AST: [Insert {
                            table_name: ObjectName(["cycling", "cyclist_name"]),
                            columns: ["id", "lastname", "firstname"],
                            source: Query {
                                ctes: [],
                                body: Values(
                                    Values(
                                        [[Value(SingleQuotedString("6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47")), Value(SingleQuotedString("KRUIKSWIJK")), Value(SingleQuotedString("Steven"))]]
                                        )
                                      ),
                                      order_by: [],
                                      limit: None,
                                      offset: None,
                                      fetch: None }
                            }]

                    */
                    Insert {table_name, columns, source} => {
                        let namespace = table_name.0.join(".");
                        let pk_cols = self.tables_to_pks.get(&namespace).unwrap();
                        let values = self.getColumnValues(&source.body);
                        let mut colmap: HashMap<String, &String> = HashMap::new();
                        let mut insert_values: Vec<(String, String)> = Vec::new();
                        for (i, c) in columns.iter().enumerate() {
                            match values.get(i) {
                                Some(v) => {
                                    let key = c.to_string();
                                    colmap.insert(c.to_string(), v);
                                    if !pk_cols.contains(&key) {
                                        insert_values.push((c.clone(), v.clone()))
                                    }
                                },
                                None => {}, //TODO some error
                            }
                        }
                        //TODO: Dunno how great this is, but the underlying socket is shared between
                        // all clones, so now tcp session establishment overhead.
                        let mut client_copy = self.con.clone();

                        //TODO: something something what happens if hset fails.
                        let key = build_key(namespace,pk_cols,&colmap);
                        let f: RedisFuture<()>  = client_copy.hset_multiple(key.clone(), insert_values.as_slice());

                        // TODO: We update the cache asynchronously - currently errors on cache update are ignored
                        let res = self.call_next_transform(qd, t);

                        let cache_update = block_on(f);

                        return res;


                    },
                    Update {table_name, assignments, selection} => {},
                    Delete {table_name, selection} => {},
                    _ => {},


                }
            }
        }
//        match message.
        self.call_next_transform(qd, t)
    }


    fn get_name(&self) -> &'static str {
        self.name
    }
}

