use crate::transforms::cassandra::sink_cluster::CassandraNode;
use cassandra_protocol::token::Murmur3Token;
use std::collections::BTreeMap;
use uuid::Uuid;

use super::node_pool::{KeyspaceMetadata, ReplicationStrategy};

#[derive(Debug, Clone)]
pub struct TokenMap {
    token_ring: BTreeMap<Murmur3Token, Uuid>,
}

impl TokenMap {
    pub fn new(nodes: &[CassandraNode]) -> Self {
        TokenMap {
            token_ring: nodes
                .iter()
                .flat_map(|node| node.tokens.iter().map(|token| (*token, node.host_id)))
                .collect(),
        }
    }

    /// Walk the token ring to figure out which nodes are acting as replicas for the given query.
    /// The way we do this depends on the replication strategy used:
    /// * ReplicationStrategy::SimpleStrategy - Each cassandra node has many tokens assigned to it to form the token ring.
    ///     Take the hashed key (token) and walk along the token ring starting at the token in the ring after the token from our key.
    ///     Return the nodes belonging to the first replication_factor number of tokens encountered.
    /// * ReplicationStrategy::NetworkTopologyStrategy - The same as the simple strategy but also:
    ///     When we walk the token ring we need to skip tokens belonging to nodes in a rack we have already encountered in this walk.
    ///
    /// For more info: https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/architecture/archDataDistributeReplication.html
    pub fn iter_replica_nodes<'a>(
        &'a self,
        nodes: &'a [CassandraNode],
        token_from_key: Murmur3Token,
        keyspace: &'a KeyspaceMetadata,
    ) -> impl Iterator<Item = Uuid> + '_ {
        let mut racks_used = vec![];
        self.token_ring
            .range(token_from_key..)
            .chain(self.token_ring.iter())
            .filter(move |(_, host_id)| {
                if let ReplicationStrategy::NetworkTopologyStrategy = keyspace.replication_strategy
                {
                    let rack = &nodes.iter().find(|x| x.host_id == **host_id).unwrap().rack;
                    if racks_used.contains(&rack) {
                        false
                    } else {
                        racks_used.push(rack);
                        true
                    }
                } else {
                    true
                }
            })
            .take(keyspace.replication_factor)
            .map(|(_, node)| *node)
    }
}

#[cfg(test)]
mod test_token_map {
    use super::*;
    use hex_literal::hex;
    use itertools::Itertools;
    use uuid::Uuid;

    static NODE_1: Uuid = Uuid::from_bytes(hex!("2DD022D62937475489D602D2933A8F71"));
    static NODE_2: Uuid = Uuid::from_bytes(hex!("2DD022D62937475489D602D2933A8F72"));
    static NODE_3: Uuid = Uuid::from_bytes(hex!("2DD022D62937475489D602D2933A8F73"));

    fn prepare_nodes() -> Vec<CassandraNode> {
        vec![
            CassandraNode::new(
                "127.0.0.1:9042".parse().unwrap(),
                "rack1".into(),
                vec![
                    Murmur3Token::new(-2),
                    Murmur3Token::new(-1),
                    Murmur3Token::new(0),
                ],
                NODE_1,
            ),
            CassandraNode::new(
                "127.0.0.1:9043".parse().unwrap(),
                "rack1".into(),
                vec![Murmur3Token::new(20)],
                NODE_2,
            ),
            CassandraNode::new(
                "127.0.0.1:9044".parse().unwrap(),
                "rack1".into(),
                vec![
                    Murmur3Token::new(2),
                    Murmur3Token::new(1),
                    Murmur3Token::new(10),
                ],
                NODE_3,
            ),
        ]
    }

    #[test]
    fn should_return_replicas_in_order() {
        verify_tokens(
            &[NODE_1, NODE_3, NODE_3, NODE_3, NODE_2],
            Murmur3Token::new(0),
        );
    }

    #[test]
    fn should_return_replicas_in_order_for_non_primary_token() {
        verify_tokens(&[NODE_3, NODE_2], Murmur3Token::new(3));
    }

    #[test]
    fn should_return_replicas_in_a_ring() {
        verify_tokens(
            &[NODE_2, NODE_1, NODE_1, NODE_1, NODE_3],
            Murmur3Token::new(20),
        );
    }

    fn verify_tokens(node_host_ids: &[Uuid], token: Murmur3Token) {
        let token_map = TokenMap::new(prepare_nodes().as_slice());
        let nodes = token_map
            .iter_replica_nodes(
                &[
                    CassandraNode::new(
                        "127.0.0.1:9042".parse().unwrap(),
                        "rack1".to_owned(),
                        vec![],
                        NODE_1,
                    ),
                    CassandraNode::new(
                        "127.0.0.2:9042".parse().unwrap(),
                        "rack1".to_owned(),
                        vec![],
                        NODE_2,
                    ),
                    CassandraNode::new(
                        "127.0.0.3:9042".parse().unwrap(),
                        "rack1".to_owned(),
                        vec![],
                        NODE_3,
                    ),
                ],
                token,
                &KeyspaceMetadata {
                    replication_factor: node_host_ids.len(),
                    replication_strategy: ReplicationStrategy::SimpleStrategy,
                },
            )
            .collect_vec();

        assert_eq!(nodes, node_host_ids);
    }
}
