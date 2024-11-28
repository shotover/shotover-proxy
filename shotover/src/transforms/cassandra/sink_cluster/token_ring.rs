use crate::transforms::cassandra::sink_cluster::CassandraNode;
use cassandra_protocol::token::Murmur3Token;
use uuid::Uuid;

use super::node_pool::{KeyspaceMetadata, ReplicationStrategy};

#[derive(Debug, Clone)]
pub struct TokenRing {
    ring_in: Vec<Murmur3Token>,
    ring_out: Vec<Uuid>,
}

impl TokenRing {
    pub fn new(nodes: &[CassandraNode]) -> Self {
        let mut ring: Vec<_> = nodes
            .iter()
            .flat_map(|node| node.tokens.iter().map(|token| (*token, node.host_id)))
            .collect();
        ring.sort_by(|a, b| a.0.cmp(&b.0));

        // Split ring into ring_in and ring_out as its faster to search and retrive seperately
        let ring_in: Vec<_> = ring.iter().map(|node| node.0).collect();
        let ring_out: Vec<_> = ring.iter().map(|node| node.1).collect();

        TokenRing { ring_in, ring_out }
    }

    /// Provides an iterator over the ring members starting at the given token.
    /// The iterator traverses the whole ring in the direction of increasing tokens.
    /// After reaching the maximum token it wraps around and continues from the lowest one.
    /// The iterator visits each member once, it doesn't have infinite length.
    pub fn ring_range(&self, token: Murmur3Token) -> impl Iterator<Item = Uuid> + '_ {
        let binary_search_index: usize = match self.ring_in.binary_search_by(|e| e.cmp(&token)) {
            Ok(exact_match_index) => exact_match_index,
            Err(first_greater_index) => first_greater_index,
        };

        self.ring_out
            .iter()
            .skip(binary_search_index)
            .chain(self.ring_out.iter())
            .copied()
            .take(self.ring_out.len())
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
    ) -> impl Iterator<Item = Uuid> + 'a {
        let mut racks_used = vec![];
        self.ring_range(token_from_key)
            .filter(move |host_id| {
                if let ReplicationStrategy::NetworkTopologyStrategy = keyspace.replication_strategy
                {
                    let rack = &nodes.iter().find(|x| x.host_id == *host_id).unwrap().rack;
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
    }
}

#[cfg(test)]
mod test_token_map {
    use super::*;
    use hex_literal::hex;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
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
        let token_map = TokenRing::new(prepare_nodes().as_slice());
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
