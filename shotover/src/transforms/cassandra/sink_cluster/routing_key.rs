use super::murmur::Murmur3PartitionerHasher;
use cassandra_protocol::query::QueryValues;
use cassandra_protocol::token::Murmur3Token;
use cassandra_protocol::types::value::Value;

// functions taken from https://github.com/krojew/cdrs-tokio/blob/9246dcf4227c1d4b1ff1eafaf0abfae2d831eec4/cdrs-tokio/src/cluster/session.rs#L126

pub fn calculate_routing_key(
    pk_indexes: &[i16],
    query_values: &QueryValues,
) -> Option<Murmur3Token> {
    let values = match query_values {
        QueryValues::SimpleValues(values) => values,
        _ => panic!("handle named"),
    };

    serialize_routing_key_with_indexes(values, pk_indexes)
}

fn serialize_routing_key_with_indexes(
    values: &[Value],
    pk_indexes: &[i16],
) -> Option<Murmur3Token> {
    let mut partitioner_hasher = Murmur3PartitionerHasher::new();

    match pk_indexes.len() {
        0 => None,
        1 => values
            .get(pk_indexes[0] as usize)
            .and_then(|value| match value {
                Value::Some(value) => {
                    partitioner_hasher.write(value);
                    Some(Murmur3Token {
                        value: partitioner_hasher.finish().value,
                    })
                }
                _ => None,
            }),
        _ => {
            for index in pk_indexes {
                match values.get(*index as usize) {
                    Some(Value::Some(value)) => {
                        // logic for hashing in this case is not documented but implemented at:
                        // https://github.com/apache/cassandra/blob/3a950b45c321e051a9744721408760c568c05617/src/java/org/apache/cassandra/db/marshal/CompositeType.java#L39
                        let len = value.len();
                        let attempt: Result<u16, _> = len.try_into();
                        match attempt {
                            Ok(len) => partitioner_hasher.write(&len.to_be_bytes()),
                            Err(_) => tracing::error!(
                                "could not route cassandra request as value was too long: {len}",
                            ),
                        }
                        partitioner_hasher.write(value);
                        partitioner_hasher.write(&[0u8]);
                    }
                    Some(Value::Null | Value::NotSet) => {
                        // write nothing
                    }
                    _ => {
                        // do not perform routing
                        return None;
                    }
                }
            }
            Some(Murmur3Token {
                value: partitioner_hasher.finish().value,
            })
        }
    }
}
