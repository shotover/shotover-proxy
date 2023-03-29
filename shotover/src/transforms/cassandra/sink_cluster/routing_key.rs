use cassandra_protocol::frame::{Serialize, Version};
use cassandra_protocol::query::QueryValues;
use cassandra_protocol::types::value::Value;
use cassandra_protocol::types::CIntShort;
use itertools::Itertools;
use std::io::{Cursor, Write};

// functions taken from https://github.com/krojew/cdrs-tokio/blob/9246dcf4227c1d4b1ff1eafaf0abfae2d831eec4/cdrs-tokio/src/cluster/session.rs#L126

pub fn calculate_routing_key(
    pk_indexes: &[i16],
    query_values: &QueryValues,
    version: Version,
) -> Option<Vec<u8>> {
    let values = match query_values {
        QueryValues::SimpleValues(values) => values,
        _ => panic!("handle named"),
    };

    serialize_routing_key_with_indexes(values, pk_indexes, version)
}

fn serialize_routing_key_with_indexes(
    values: &[Value],
    pk_indexes: &[i16],
    version: Version,
) -> Option<Vec<u8>> {
    match pk_indexes.len() {
        0 => None,
        1 => values
            .get(pk_indexes[0] as usize)
            .and_then(|value| match value {
                Value::Some(value) => Some(value.serialize_to_vec(version)),
                _ => None,
            }),
        _ => {
            let mut buf = vec![];
            if pk_indexes
                .iter()
                .map(|index| values.get(*index as usize))
                .fold_options(Cursor::new(&mut buf), |mut cursor, value| {
                    if let Value::Some(value) = value {
                        serialize_routing_value(&mut cursor, value, version)
                    }
                    cursor
                })
                .is_some()
            {
                Some(buf)
            } else {
                None
            }
        }
    }
}

// https://github.com/apache/cassandra/blob/3a950b45c321e051a9744721408760c568c05617/src/java/org/apache/cassandra/db/marshal/CompositeType.java#L39
fn serialize_routing_value(cursor: &mut Cursor<&mut Vec<u8>>, value: &Vec<u8>, version: Version) {
    let size: CIntShort = value.len().try_into().unwrap();
    size.serialize(cursor, version);
    value.serialize(cursor, version);
    let _ = cursor.write(&[0]);
}
