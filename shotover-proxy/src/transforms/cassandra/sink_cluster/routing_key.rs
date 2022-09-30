use cassandra_protocol::frame::{Serialize, Version};
use cassandra_protocol::query::QueryValues;
use cassandra_protocol::types::value::Value;
use cassandra_protocol::types::CIntShort;
use cassandra_protocol::types::SHORT_LEN;
use itertools::Itertools;
use std::io::{Cursor, Write};

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
            .map(|value| value.serialize_to_vec(version)),
        _ => {
            let mut buf = vec![];
            if pk_indexes
                .iter()
                .map(|index| values.get(*index as usize))
                .fold_options(Cursor::new(&mut buf), |mut cursor, value| {
                    serialize_routing_value(&mut cursor, value, version);
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
fn serialize_routing_value(cursor: &mut Cursor<&mut Vec<u8>>, value: &Value, version: Version) {
    let temp_size: CIntShort = 0;
    temp_size.serialize(cursor, version);

    let before_value_pos = cursor.position();
    value.serialize(cursor, version);

    let after_value_pos = cursor.position();
    cursor.set_position(before_value_pos - SHORT_LEN as u64);

    let value_size: CIntShort = (after_value_pos - before_value_pos) as CIntShort;
    value_size.serialize(cursor, version);

    cursor.set_position(after_value_pos);
    let _ = cursor.write(&[0]);
}
