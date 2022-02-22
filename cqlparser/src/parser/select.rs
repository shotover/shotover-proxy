use std::str;
use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case, take_while1};
use nom::character::complete::{digit1, multispace0, multispace1};
use nom::character::is_alphanumeric;
use nom::combinator::{map, opt};
use nom::multi::{fold_many0, many0};
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use nom::IResult;

use crate::ast::*;

pub fn select(i: &[u8]) -> IResult<&[u8], Select> {
    let (
        remaining_input,
        (_, _, distinct, json, select, from, where_, order_by, limit, allow_filtering),
    ) = tuple((
        tag_no_case("select"),
        multispace1,
        distinct,
        json,
        fields,
        from,
        where_,
        opt(order_by),
        limit,
        allow_filtering,
    ))(i)?;
    Ok((
        remaining_input,
        Select {
            distinct,
            json,
            select,
            from,
            where_,
            order_by,
            limit,
            allow_filtering,
        },
    ))
}

pub fn json(i: &[u8]) -> IResult<&[u8], bool> {
    map(opt(terminated(tag_no_case("json"), multispace1)), |v| {
        v.is_some()
    })(i)
}

pub fn distinct(i: &[u8]) -> IResult<&[u8], bool> {
    map(opt(terminated(tag_no_case("distinct"), multispace1)), |v| {
        v.is_some()
    })(i)
}

pub fn where_(i: &[u8]) -> IResult<&[u8], Vec<RelationElement>> {
    map(
        opt(preceded(
            tuple((multispace1, tag_no_case("where"), multispace1)),
            where_elements,
        )),
        |x| x.unwrap_or_default(),
    )(i)
}

pub fn where_elements(i: &[u8]) -> IResult<&[u8], Vec<RelationElement>> {
    many0(terminated(
        where_element,
        opt(tuple((multispace1, tag_no_case("AND"), multispace1))), // TODO: this seems wrong
    ))(i)
}

pub fn where_element(i: &[u8]) -> IResult<&[u8], RelationElement> {
    let (remaining_input, (lhs, _, operator, _, rhs)) =
        tuple((expr, multispace1, operator, multispace1, expr))(i)?;

    Ok((
        remaining_input,
        RelationElement::Comparison(RelationComparison { lhs, operator, rhs }),
    ))
}

pub fn order_by(i: &[u8]) -> IResult<&[u8], OrderBy> {
    let (remaining_input, (_, _, _, _, _, name, ordering)) = tuple((
        multispace1,
        tag_no_case("order"),
        multispace1,
        tag_no_case("by"),
        multispace1,
        identifier,
        opt(preceded(multispace1, ordering)),
    ))(i)?;

    let name = String::from_utf8(name.to_vec()).unwrap();
    let ordering = ordering.unwrap_or(Ordering::Asc);
    Ok((remaining_input, OrderBy { name, ordering }))
}

pub fn operator(i: &[u8]) -> IResult<&[u8], ComparisonOperator> {
    alt((
        map(tag("="), |_| ComparisonOperator::Equals),
        map(tag(">="), |_| ComparisonOperator::GreaterThanOrEqualTo),
        map(tag(">"), |_| ComparisonOperator::GreaterThan),
        map(tag("<="), |_| ComparisonOperator::LessThanOrEqualTo),
        map(tag("<"), |_| ComparisonOperator::LessThan),
    ))(i)
}

pub fn ordering(i: &[u8]) -> IResult<&[u8], Ordering> {
    alt((
        map(tag_no_case("asc"), |_| Ordering::Asc),
        map(tag_no_case("desc"), |_| Ordering::Desc),
    ))(i)
}

pub fn limit(i: &[u8]) -> IResult<&[u8], Option<u64>> {
    opt(preceded(
        tuple((multispace1, tag_no_case("limit"), multispace1)),
        unsigned_number,
    ))(i)
}

pub fn unsigned_number(i: &[u8]) -> IResult<&[u8], u64> {
    map(digit1, |d| {
        FromStr::from_str(str::from_utf8(d).unwrap()).unwrap()
    })(i)
}

pub fn allow_filtering(i: &[u8]) -> IResult<&[u8], bool> {
    opt(preceded(multispace1, tag_no_case("allow filtering")))(i).map(|(r, v)| (r, v.is_some()))
}

pub fn fields(i: &[u8]) -> IResult<&[u8], Vec<SelectElement>> {
    many0(terminated(field, opt(ws_sep_comma)))(i) // TODO: this seems wrong
}

pub fn field(i: &[u8]) -> IResult<&[u8], SelectElement> {
    let (remaining, (expr, as_alias)) = pair(
        expr,
        opt(preceded(
            tuple((multispace1, tag_no_case("AS"), multispace1)),
            identifier,
        )),
    )(i)?;

    let as_alias = as_alias.map(|x| String::from_utf8(x.to_vec()).unwrap());
    Ok((remaining, SelectElement { expr, as_alias }))
}

pub fn from(i: &[u8]) -> IResult<&[u8], Vec<String>> {
    preceded(
        tuple((multispace1, tag_no_case("from"), multispace1)),
        map(identifier, |name| {
            vec![String::from_utf8(name.to_vec()).unwrap()]
        }),
    )(i)
}

pub fn expr(i: &[u8]) -> IResult<&[u8], Expr> {
    alt((
        map(tag("*"), |_| Expr::Wildcard),
        map(constant, Expr::Constant),
        map(identifier, |name| {
            Expr::Name(String::from_utf8(name.to_vec()).unwrap())
        }),
    ))(i)
}

pub fn constant(i: &[u8]) -> IResult<&[u8], Constant> {
    alt((
        map(integer_constant, Constant::Decimal),
        map(string_constant, Constant::String),
        map(bool_constant, Constant::Bool),
    ))(i)
}

pub fn integer_constant(i: &[u8]) -> IResult<&[u8], i64> {
    map(pair(opt(tag("-")), digit1), |(negative, bytes)| {
        let mut intval = i64::from_str(str::from_utf8(bytes).unwrap()).unwrap();
        if (negative).is_some() {
            intval *= -1;
        }
        intval
    })(i)
}

pub fn string_constant(i: &[u8]) -> IResult<&[u8], String> {
    map(raw_string_quoted, |bytes| String::from_utf8(bytes).unwrap())(i)
}

fn raw_string_quoted(i: &[u8]) -> IResult<&[u8], Vec<u8>> {
    delimited(
        tag("'"),
        fold_many0(
            alt((
                is_not("'"), //
                map(tag("''"), |_| &b"'"[..]),
            )),
            Vec::new,
            |mut acc: Vec<u8>, bytes: &[u8]| {
                acc.extend(bytes);
                acc
            },
        ),
        tag("'"),
    )(i)
}

pub fn bool_constant(i: &[u8]) -> IResult<&[u8], bool> {
    alt((
        map(tag_no_case("true"), |_| true),
        map(tag_no_case("false"), |_| false),
    ))(i)
}

pub(crate) fn ws_sep_comma(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(multispace0, tag(","), multispace0)(i)
}

pub fn identifier(i: &[u8]) -> IResult<&[u8], &[u8]> {
    take_while1(is_identifier)(i)
}

pub fn is_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == b'_'
}
