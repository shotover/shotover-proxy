use cqlparser::ast::*;
use cqlparser::parse;

fn assert_parses(input: &[&str], ast: Vec<Statement>) {
    for input in input {
        assert_eq!(parse(input).unwrap(), ast);
    }
}

fn assert_fails_to_parse(input: &[&str], error: &'static str) {
    for input in input {
        assert_eq!(parse(input), Err(error.to_string()));
    }
}

mod combined;
mod insert;
mod select;
