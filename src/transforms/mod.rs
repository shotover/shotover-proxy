use std::collections::HashMap;
use crate::message::Message::Query as MessageQuery;
use crate::message::Message;
use crate::message::QueryType;
use redis::{Client, Connection};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Statement::*;
use sqlparser::ast::{Query, SetExpr::Values, Values as ValuesStruct, Expr, SetExpr, Expr::Value as EValue};
use sqlparser::ast::Value;
use std::iter::Iterator;


pub trait Transform: std::marker::Sync + std::marker::Send {
   fn transform(&self, message: Message) -> Option<Message>;
}

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

pub struct Printer {
    name: &'static str,
}

impl Printer {
    pub fn new() -> Printer {
        Printer{
            name: "NoOp",
        }
    }
}

pub struct QueryTypeFilter {
    name: &'static str,
    filters: Vec<QueryType>,
}


impl QueryTypeFilter {
    pub fn new(nfilters: Vec<QueryType>) -> QueryTypeFilter {
        QueryTypeFilter{
            name: "NoOp",
            filters: nfilters,
        }
    }
}

impl Transform for NoOp {
    fn transform(&self, message: Message) -> Option<Message> {
        return Some(message);
    }
}

impl Transform for Printer {
    fn transform(&self, message: Message) -> Option<Message> {
        println!("Message content: {:?}", message);
        return Some(message)
    }
}

impl Transform for QueryTypeFilter {
    fn transform(&self, message: Message) -> Option<Message> {
        // TODO this is likely the wrong way to get around the borrow from the match statement
        return match message {
            Message::Query(q) => {
                if self.filters.iter().any(|x| *x == q.query_type) {
                    return None
                }
                Some(Message::Query(q))
            }
            Message::Response(r) => {
                Some(Message::Response(r))
            }
        }
    }
}

pub struct SimpleRedisCache {
    name: &'static str,
    client: Client,
    con: Connection,
    tables_to_pks: HashMap<String, Vec<String>>,
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
                 for value in v.0 {
                     for ex in value {
                         match ex {
                             EValue(v) => {
                                 match v {
                                     Value::Number(v) |
                                     /// 'string value'
                                     Value::SingleQuotedString(v) |
                                     /// N'string value'
                                     Value::NationalStringLiteral(v) |
                                     /// X'hex value'
                                     Value::HexStringLiteral(v) |
                                     /// Boolean value true or false
                                     Value::Boolean(v) |
                                     /// `DATE '...'` literals
                                     Value::Date(v) |
                                     /// `TIME '...'` literals
                                     Value::Time(v) |
                                     /// `TIMESTAMP '...'` literals
                                     Value::Timestamp(v)  => {
                                         cumulator.push(format!("{:?}", v))
                                     },
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




    fn transform(&self, message: Message) -> Option<Message> {
        let dialect = GenericDialect {}; //TODO write CQL dialect
        
        // Only handle client requests
        if let MessageQuery(qm) = message {
            let ast = Parser::parse_sql(&dialect, qm.query_string).unwrap(); //TODO eat the error properly
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


                        if let Values(v) = source.body {
                            let flattened = v.0.into_iter().flatten().collect::<Vec<Expr>>();
                        }

                    },
                    Update {table_name, assignments, selection} => {},
                    Delete {table_name, selection} => {},
                    _ => {},


                }
            }
        }
//        match message.
        unimplemented!()
    }



}

