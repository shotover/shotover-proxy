use cqlparser::ast::*;

use crate::assert_parses;

#[test]
fn test_insert() {
    assert_parses(&["insert;"], vec![Statement::Insert(Insert {})]);
}
