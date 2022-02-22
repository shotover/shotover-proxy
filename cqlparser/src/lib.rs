pub mod ast;
pub(crate) mod parser;

use std::str;

use nom::branch::alt;
use nom::combinator::map;
use nom::IResult;

use crate::ast::*;
use crate::parser::insert::insert;
use crate::parser::select::select;

pub fn parse(value: &str) -> Vec<Statement> {
    vec![sql_query(value.as_bytes()).unwrap().1]
}

pub fn sql_query(i: &[u8]) -> IResult<&[u8], Statement> {
    alt((
        map(select, Statement::Select),
        map(insert, Statement::Insert),
    ))(i)
}
