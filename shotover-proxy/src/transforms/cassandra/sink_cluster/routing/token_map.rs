use crate::transforms::cassandra::sink_cluster::CassandraNode;
use cassandra_protocol::token::Murmur3Token;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct TokenMap {
    token_ring: BTreeMap<Murmur3Token, Uuid>,
}

impl TokenMap {
    pub fn new(nodes: &[CassandraNode]) -> Self {
        TokenMap {
            token_ring: nodes
                .iter()
                .flat_map(|node| node.tokens.iter().map(move |token| (*token, node.host_id)))
                .collect(),
        }
    }

    /// Returns local nodes starting at given token and going in the direction of replicas.
    pub fn nodes_for_token_capped(
        &self,
        token: Murmur3Token,
        replica_count: usize,
    ) -> impl Iterator<Item = Uuid> + '_ {
        self.token_ring
            .range(token..)
            .chain(self.token_ring.iter())
            .take(replica_count)
            .map(|(_, node)| *node)
    }

    // /// Returns local nodes starting at given token and going in the direction of replicas.
    // pub fn nodes_for_token(&self, token: Murmur3Token) -> impl Iterator<Item = Uuid> + '_ {
    //     self.token_ring
    //         .range(token..)
    //         .chain(self.token_ring.iter())
    //         .take(self.token_ring.len())
    //         .map(|(_, node)| *node)
    // }
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
                "dc1".into(),
                vec![
                    Murmur3Token::new(-2),
                    Murmur3Token::new(-1),
                    Murmur3Token::new(0),
                ],
                NODE_1,
            ),
            CassandraNode::new(
                "127.0.0.1:9043".parse().unwrap(),
                "dc1".into(),
                vec![Murmur3Token::new(20)],
                NODE_2,
            ),
            CassandraNode::new(
                "127.0.0.1:9044".parse().unwrap(),
                "dc1".into(),
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
            .nodes_for_token_capped(token, node_host_ids.len())
            .collect_vec();

        assert_eq!(nodes.len(), node_host_ids.len());
        for (index, node) in nodes.iter().enumerate() {
            assert_eq!(*node, node_host_ids[index]);
        }
    }
}
