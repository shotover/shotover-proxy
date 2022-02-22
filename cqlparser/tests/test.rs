use cqlparser::ast::*;
use cqlparser::parse;

fn assert_parses(input: &[&str], ast: Vec<Statement>) {
    for input in input {
        assert_eq!(parse(input), ast);
    }
}

#[test]
fn test_insert() {
    assert_parses(&["insert"], vec![Statement::Insert(Insert {})]);
}

#[test]
fn test_select_one_field() {
    assert_parses(
        &[
            "select field from table",
            "SELECT    field    FROM    table",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_one_field_as() {
    assert_parses(
        &[
            "select field as alias from table",
            "SELECT    field    AS    alias   FROM    table",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: Some("alias".into()),
            }],
            from: vec!["table".to_string()],
            where_: vec![],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_many_fields_as() {
    assert_parses(
        &[
            "select field1 as foo, field2, field3 as bar from table",
            "select field1    as    foo    ,   field2    ,   field3   as   bar from table",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![
                SelectElement {
                    expr: Expr::Name("field1".to_string()),
                    as_alias: Some("foo".into()),
                },
                SelectElement {
                    expr: Expr::Name("field2".to_string()),
                    as_alias: None,
                },
                SelectElement {
                    expr: Expr::Name("field3".to_string()),
                    as_alias: Some("bar".into()),
                },
            ],
            from: vec!["table".to_string()],
            where_: vec![],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_distinct() {
    assert_parses(
        &[
            "SELECT distinct field FROM table",
            "SELECT    DISTINCT    field FROM table",
        ],
        vec![Statement::Select(Select {
            distinct: true,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_json() {
    assert_parses(
        &[
            "SELECT json field FROM table",
            "SELECT    JSON    field FROM table",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: true,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_order_by_asc() {
    assert_parses(
        &[
            "SELECT field FROM table order by pk_field",
            "SELECT field FROM table ORDER BY     pk_field",
            "SELECT field FROM table order   BY    pk_field asc",
            "SELECT field FROM table ORDER     BY     pk_field    ASC",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![],
            order_by: Some(OrderBy {
                name: "pk_field".to_string(),
                ordering: Ordering::Asc,
            }),
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_where_field_greater_than_int() {
    assert_parses(
        &[
            "select field from table where foo > 1",
            "select field from table where     foo     >     1",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![RelationElement::Comparison(RelationComparison {
                lhs: Expr::Name("foo".to_string()),
                operator: ComparisonOperator::GreaterThan,
                rhs: Expr::Constant(Constant::Decimal(1)),
            })],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_where_field_and() {
    assert_parses(
        &[
            "select field from table where foo < 1 and bar <= 1111",
            "select field from table where     foo    <   1    AND    bar   <=    1111",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![
                RelationElement::Comparison(RelationComparison {
                    lhs: Expr::Name("foo".to_string()),
                    operator: ComparisonOperator::LessThan,
                    rhs: Expr::Constant(Constant::Decimal(1)),
                }),
                RelationElement::Comparison(RelationComparison {
                    lhs: Expr::Name("bar".to_string()),
                    operator: ComparisonOperator::LessThanOrEqualTo,
                    rhs: Expr::Constant(Constant::Decimal(1111)),
                }),
            ],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_where_field_greater_than_or_equal_negative_int() {
    assert_parses(
        &[
            "select field from table where foo >= -13",
            "select field from table where     foo     >=     -13",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![RelationElement::Comparison(RelationComparison {
                lhs: Expr::Name("foo".to_string()),
                operator: ComparisonOperator::GreaterThanOrEqualTo,
                rhs: Expr::Constant(Constant::Decimal(-13)),
            })],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_where_field_equals_bool() {
    assert_parses(
        &[
            "select field from table where foo = true",
            "select field from table where     foo     =     true",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![RelationElement::Comparison(RelationComparison {
                lhs: Expr::Name("foo".to_string()),
                operator: ComparisonOperator::Equals,
                rhs: Expr::Constant(Constant::Bool(true)),
            })],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_where_field_equals_string() {
    assert_parses(
        &[
            "select field from table where foo = 'bar'",
            "select field from table where     foo     =     'bar'",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![RelationElement::Comparison(RelationComparison {
                lhs: Expr::Name("foo".to_string()),
                operator: ComparisonOperator::Equals,
                rhs: Expr::Constant(Constant::String("bar".into())),
            })],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_where_field_equals_string_escape_quote() {
    assert_parses(
        &[
            "select field from table where foo = 'lucas'' cool string '''''",
            "select field from table where     foo     =     'lucas'' cool string '''''",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![RelationElement::Comparison(RelationComparison {
                lhs: Expr::Name("foo".to_string()),
                operator: ComparisonOperator::Equals,
                rhs: Expr::Constant(Constant::String("lucas' cool string ''".into())),
            })],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_order_by_desc() {
    assert_parses(
        &[
            "SELECT field FROM table order by foo desc",
            "SELECT field FROM table ORDER     BY     foo    DESC",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![],
            order_by: Some(OrderBy {
                name: "foo".to_string(),
                ordering: Ordering::Desc,
            }),
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_limit_42() {
    assert_parses(
        &[
            "SELECT field FROM table limit 42",
            "SELECT field FROM table    LIMIT    42",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![],
            order_by: None,
            limit: Some(42),
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_limit_0() {
    assert_parses(
        &[
            "SELECT field FROM table limit 0",
            "SELECT field FROM table    LIMIT    0",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![],
            order_by: None,
            limit: Some(0),
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_allow_filtering() {
    assert_parses(
        &[
            "SELECT field FROM table allow filtering",
            "SELECT field FROM table    ALLOW FILTERING",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Name("field".to_string()),
                as_alias: None,
            }],
            from: vec!["table".to_string()],
            where_: vec![],
            order_by: None,
            limit: None,
            allow_filtering: true,
        })],
    );
}

#[test]
fn test_select_two_fields() {
    assert_parses(
        &[
            "SELECT field1,field2 FROM table",
            "SELECT field1, field2 FROM table",
            "SELECT   field1  ,   field2    FROM    table",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![
                SelectElement {
                    expr: Expr::Name("field1".to_string()),
                    as_alias: None,
                },
                SelectElement {
                    expr: Expr::Name("field2".to_string()),
                    as_alias: None,
                },
            ],
            from: vec!["table".to_string()],
            where_: vec![],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_all() {
    assert_parses(
        &[
            "SELECT * FROM foo",
            "SELECT        *        FROM        foo",
        ],
        vec![Statement::Select(Select {
            distinct: false,
            json: false,
            select: vec![SelectElement {
                expr: Expr::Wildcard,
                as_alias: None,
            }],
            from: vec!["foo".to_string()],
            where_: vec![],
            order_by: None,
            limit: None,
            allow_filtering: false,
        })],
    );
}

#[test]
fn test_select_christmas_tree() {
    assert_parses(
        &["SELECT distinct json field1, field2 as foo FROM table WHERE foo = 1 order by order_column DESC limit 9999 allow filtering"],
        vec![Statement::Select(Select {
            distinct: true,
            json: true,
            select: vec![
                SelectElement {
                    expr: Expr::Name("field1".to_string()),
                    as_alias: None,
                },
                SelectElement {
                    expr: Expr::Name("field2".to_string()),
                    as_alias: Some("foo".into()),
                },
            ],
            from: vec!["table".to_string()],
            where_: vec![RelationElement::Comparison(RelationComparison {
                lhs: Expr::Name("foo".to_string()),
                operator: ComparisonOperator::Equals,
                rhs: Expr::Constant(Constant::Decimal(1)),
            })],
            order_by: Some(OrderBy {
                name: "order_column".to_string(),
                ordering: Ordering::Desc,
            }),
            limit: Some(9999),
            allow_filtering: true,
        })],
    );
}
