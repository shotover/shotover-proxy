pub mod ast;
pub(crate) mod parser;

use std::str;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::multispace0;
use nom::combinator::map;
use nom::multi::many0;
use nom::sequence::{delimited, terminated};
use nom::IResult;

use crate::ast::*;
use crate::parser::insert::insert;
use crate::parser::select::select;

pub fn parse(value: &str) -> Result<Vec<Statement>, String> {
    // TODO: parse multiple statements
    match statements(value.as_bytes()) {
        Ok(([], o)) => Ok(o),
        Ok((a @ [..], _)) => Err(format!("Unexpected end of query: {:?}", str::from_utf8(a))),
        Err(e) => Err(format!("nom error: {:?}", e)),
    }
}

fn statements(i: &[u8]) -> IResult<&[u8], Vec<Statement>> {
    many0(terminated(statement, semicolon))(i)
}

fn semicolon(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(multispace0, tag(";"), multispace0)(i)
}

fn statement(i: &[u8]) -> IResult<&[u8], Statement> {
    alt((
        map(select, Statement::Select),
        map(insert, Statement::Insert),
    ))(i)
}
