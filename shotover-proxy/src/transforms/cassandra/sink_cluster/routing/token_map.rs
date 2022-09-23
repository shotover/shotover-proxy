use crate::transforms::cassandra::sink_cluster::CassandraNode;
use cassandra_protocol::token::Murmur3Token;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct TokenMap {
    token_ring: BTreeMap<Murmur3Token, Arc<CassandraNode>>,
}

impl TokenMap {
    pub fn new(nodes: &[Arc<CassandraNode>]) -> Self {
        TokenMap {
            token_ring: nodes
                .iter()
                .flat_map(|node| node.tokens.iter().map(move |token| (*token, node.clone())))
                .collect(),
        }
    }

    /// Returns local nodes starting at given token and going in the direction of replicas.
    pub fn nodes_for_token_capped(
        &self,
        token: Murmur3Token,
        replica_count: usize,
    ) -> impl Iterator<Item = Arc<CassandraNode>> + '_ {
        self.token_ring
            .range(token..)
            .chain(self.token_ring.iter())
            .take(replica_count)
            .map(|(_, node)| node.clone())
    }

    /// Returns local nodes starting at given token and going in the direction of replicas.
    pub fn nodes_for_token(
        &self,
        token: Murmur3Token,
    ) -> impl Iterator<Item = Arc<CassandraNode>> + '_ {
        self.token_ring
            .range(token..)
            .chain(self.token_ring.iter())
            .take(self.token_ring.len())
            .map(|(_, node)| node.clone())
    }
}

#[cfg(test)]
mod test_token_map {
    use super::*;
    use itertools::Itertools;
    use uuid::uuid;

    fn prepare_nodes() -> Vec<Arc<CassandraNode>> {
        vec![
            CassandraNode::new(
                "127.0.0.1:9042".parse().unwrap(),
                "dc1".into(),
                vec![
                    Murmur3Token::new(-2),
                    Murmur3Token::new(-1),
                    Murmur3Token::new(0),
                ],
                uuid!("2dd022d6-2937-4754-89d6-02d2933a8f7f"),
            ),
            CassandraNode::new(
                "127.0.0.1:9043".parse().unwrap(),
                "dc1".into(),
                vec![Murmur3Token::new(20)],
                uuid!("2dd022d6-2937-4754-89d6-02d2933a8f7f"),
            ),
            CassandraNode::new(
                "127.0.0.1:9044".parse().unwrap(),
                "dc1".into(),
                vec![
                    Murmur3Token::new(2),
                    Murmur3Token::new(1),
                    Murmur3Token::new(10),
                ],
                uuid!("2dd022d6-2937-4754-89d6-02d2933a8f7f"),
            ),
        ]
        .into_iter()
        .map(Arc::new)
        .collect()
    }

    #[test]
    fn should_return_replicas_in_order() {
        verify_tokens(
            &[
                "127.0.0.1:9042",
                "127.0.0.1:9044",
                "127.0.0.1:9044",
                "127.0.0.1:9044",
                "127.0.0.1:9043",
            ],
            Murmur3Token::new(0),
        );
    }

    #[test]
    fn should_return_replicas_in_order_for_non_primary_token() {
        verify_tokens(&["127.0.0.1:9044", "127.0.0.1:9043"], Murmur3Token::new(3));
    }

    #[test]
    fn should_return_replicas_in_a_ring() {
        verify_tokens(
            &[
                "127.0.0.1:9043",
                "127.0.0.1:9042",
                "127.0.0.1:9042",
                "127.0.0.1:9042",
                "127.0.0.1:9044",
            ],
            Murmur3Token::new(20),
        );
    }

    fn verify_tokens(node_addresses: &[&str], token: Murmur3Token) {
        let token_map = TokenMap::new(&prepare_nodes());
        let nodes = token_map
            .nodes_for_token_capped(token, node_addresses.len())
            .collect_vec();

        assert_eq!(nodes.len(), node_addresses.len());
        for (index, node) in nodes.iter().enumerate() {
            assert_eq!(node.address, node_addresses[index].parse().unwrap());
        }
    }
}
