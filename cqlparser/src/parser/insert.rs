use nom::bytes::complete::tag_no_case;
use nom::sequence::tuple;
use nom::IResult;

use crate::ast::*;

pub fn insert(i: &[u8]) -> IResult<&[u8], Insert> {
    let (remaining_input, _) = tuple((tag_no_case("insert"),))(i)?;
    Ok((remaining_input, Insert {}))
}
