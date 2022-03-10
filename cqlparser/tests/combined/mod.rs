use crate::assert_fails_to_parse;

#[test]
fn test_invalid_after_complete_query() {
    assert_fails_to_parse(
        &[
            "select field from table; BLAH",
            "SELECT    field    FROM    table; BLAH",
        ],
        "Unexpected end of query: Ok(\"BLAH\")",
    );
}
