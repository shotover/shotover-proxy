use std::collections::HashMap;
use crate::message::Message::Query as MessageQuery;
use crate::message::{Message, QueryResponse};
use crate::message::QueryType;
use redis::{Client, Connection};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Statement::*;
use sqlparser::ast::{Query, SetExpr::Values, Values as ValuesStruct, Expr, SetExpr, Expr::Value as EValue};
use sqlparser::ast::Value;
use std::iter::Iterator;

pub mod chain;

use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};
use std::borrow::Borrow;

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

impl Transform for Forward {
    fn transform(&self, qd: &mut Wrapper, t: &TransformChain) -> ChainResponse {
        return ChainResponse::Ok(qd.message.clone());
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

impl Transform for NoOp {
    fn transform(&self, qd: &mut Wrapper, t: &TransformChain) -> ChainResponse {
        return self.call_next_transform(qd, t)
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

impl Transform for Printer {
    fn transform(&self, qd: &mut Wrapper, t: &TransformChain) -> ChainResponse {
        println!("Message content: {:?}", qd.message);
        return self.call_next_transform(qd, t)
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

impl Transform for QueryTypeFilter {
    fn transform(&self, qd: &mut Wrapper, t: &TransformChain) -> ChainResponse {
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
    client: Client,
    con: Connection,
    tables_to_pks: HashMap<String, Vec<String>>,
}

pub struct RedisQueryCache {
    name: &'static str,
    client: Client,
    con: Connection
}

impl RedisQueryCache {
    pub fn new(uri: String) -> Option<SimpleRedisCache> {
        if let Ok(c) = redis::Client::open(uri) {
            if let Ok(con) = c.get_connection() {
                return Some(SimpleRedisCache {
                    name: "SimpleRedisCache",
                    client: c,
                    con: con,
                    tables_to_pks: HashMap::new(),
                })
            }
        }
        return None
    }
}


impl Transform for RedisQueryCache {
    fn transform(&self, qd: &mut Wrapper, t: &TransformChain) -> ChainResponse {
        // let message  = qd.message.borrow();
        // if let MessageQuery(qm) = message {
        //     if let
        // }


        unimplemented!()
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

impl SimpleRedisCache {
    //"redis://127.0.0.1/"
    pub fn new(uri: String) -> Option<SimpleRedisCache> {
        if let Ok(c) = redis::Client::open(uri) {
            if let Ok(con) = c.get_connection() {
                return Some(SimpleRedisCache {
                    name: "SimpleRedisCache",
                    client: c,
                    con: con,
                    tables_to_pks: HashMap::new(),
                })
            }
        }
        return None
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
                                 match v {
                                     Value::Number(v) |
                                     Value::SingleQuotedString(v) |
                                     Value::NationalStringLiteral(v) |
                                     Value::HexStringLiteral(v) |
                                     Value::Date(v) |
                                     Value::Time(v) |
                                     Value::Timestamp(v)  => {
                                         cumulator.push(format!("{:?}", v))
                                     },
                                     Value::Boolean(v) => {
                                         cumulator.push(format!("{:?}", v))
                                     }
                                     _ => cumulator.push("NULL".to_string())
                                 }
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

impl Transform for SimpleRedisCache {
    fn transform(&self, qd: &mut Wrapper, t: &TransformChain) -> ChainResponse {
        let message  = qd.message.borrow();
        let dialect = GenericDialect {}; //TODO write CQL dialect
        
        // Only handle client requests
        if let MessageQuery(qm) = message {
            let ast = Parser::parse_sql(&dialect, qm.query_string.clone()).unwrap(); //TODO eat the error properly
            //TODO Grammar parser
            for statement in ast.iter() {
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
                    Query(q) => {},

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



                        for (left, right) in pk_cols.iter().zip(columns.iter()) {

                        }

                        //TODO build set command for redis
                        /*
                        E.g use pk_cols as the key, append the non-pk columns to it as the unique id e.g. foo:bar:column_name = column_value where foo and bar are pk elements
                        */

                        //
                        // if let Values(v) = &source.body {
                        //     let flattened = v.0.into_iter().flatten().collect::<Vec<Expr>>();
                        // }

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

