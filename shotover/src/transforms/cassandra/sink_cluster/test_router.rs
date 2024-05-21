#[cfg(test)]
mod test_token_aware_router {
    use super::super::node_pool::KeyspaceMetadata;
    use super::super::routing_key::calculate_routing_key;
    use crate::transforms::cassandra::sink_cluster::node::CassandraNode;
    use crate::transforms::cassandra::sink_cluster::node_pool::{
        NodePoolBuilder, PreparedMetadata, ReplicationStrategy,
    };
    use crate::transforms::cassandra::sink_cluster::{KeyspaceChanRx, KeyspaceChanTx};
    use cassandra_protocol::consistency::Consistency::One;
    use cassandra_protocol::frame::message_execute::BodyReqExecuteOwned;
    use cassandra_protocol::query::QueryParams;
    use cassandra_protocol::query::QueryValues::SimpleValues;
    use cassandra_protocol::token::Murmur3Token;
    use cassandra_protocol::types::value::Value;
    use cassandra_protocol::types::CBytesShort;
    use pretty_assertions::assert_eq;
    use rand::prelude::*;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use tokio::sync::watch;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_router() {
        let mut rng = SmallRng::from_rng(rand::thread_rng()).unwrap();

        let nodes = prepare_nodes();
        let mut router = NodePoolBuilder::new("chain".to_owned()).build();
        let (_nodes_tx, mut nodes_rx) = watch::channel(nodes);
        router.update_nodes(&mut nodes_rx);

        let id = CBytesShort::new(vec![
            11, 241, 38, 11, 140, 72, 217, 34, 214, 128, 175, 241, 151, 73, 197, 227,
        ]);

        let keyspace_metadata = KeyspaceMetadata {
            replication_factor: 3,
            replication_strategy: ReplicationStrategy::SimpleStrategy,
        };

        let (keyspaces_tx, mut keyspaces_rx): (KeyspaceChanTx, KeyspaceChanRx) =
            watch::channel(HashMap::new());

        keyspaces_tx
            .send(
                [("demo_ks".to_string(), keyspace_metadata)]
                    .into_iter()
                    .collect(),
            )
            .unwrap();

        router.update_keyspaces(&mut keyspaces_rx);

        router
            .add_prepared_result(id.clone(), prepared_metadata())
            .await;

        for (pk, test_token, rack_replicas, dc_replicas) in test_data() {
            let query_parameters = QueryParams {
                consistency: One,
                with_names: false,
                values: Some(SimpleValues(vec![Value::Some(pk.as_bytes().to_vec())])),
                page_size: None,
                paging_state: None,
                serial_consistency: None,
                timestamp: None,
                keyspace: None,
                now_in_seconds: None,
            };

            let token = calculate_routing_key(
                &prepared_metadata().pk_indexes,
                query_parameters.values.as_ref().unwrap(),
            )
            .unwrap();

            assert_eq!(token, test_token);

            let node = router
                .get_replica_node_in_dc(
                    &execute_body(id.clone(), query_parameters),
                    "rack1",
                    &mut rng,
                )
                .await
                .unwrap()
                .remove(0);

            if !rack_replicas.is_empty() {
                assert!(rack_replicas.contains(&node.address));
            } else {
                assert!(dc_replicas.contains(&node.address));
            }
        }
    }

    fn test_data() -> Vec<(&'static str, Murmur3Token, Vec<SocketAddr>, Vec<SocketAddr>)> {
        vec![
            (
                "J.R.R.Tolkien",
                Murmur3Token::new(33977059392662464),
                vec![
                    "172.16.1.4:9042".parse().unwrap(),
                    "172.16.1.3:9042".parse().unwrap(),
                ],
                vec!["172.16.1.10:9042".parse().unwrap()],
            ),
            (
                "LeoTolstoy",
                Murmur3Token::new(1121202131577552268),
                vec![],
                vec![
                    "172.16.1.7:9042".parse().unwrap(),
                    "172.16.1.8:9042".parse().unwrap(),
                    "172.16.1.9:9042".parse().unwrap(),
                ],
            ),
            (
                "MarkTwain",
                Murmur3Token::new(5440223371079860612),
                vec!["172.16.1.4:9042".parse().unwrap()],
                vec![
                    "172.16.1.10:9042".parse().unwrap(),
                    "172.16.1.7:9042".parse().unwrap(),
                ],
            ),
            (
                "J.K.Rowling",
                Murmur3Token::new(8929012386469871705),
                vec!["172.16.1.2:9042".parse().unwrap()],
                vec![
                    "172.16.1.10:9042".parse().unwrap(),
                    "172.16.1.7:9042".parse().unwrap(),
                ],
            ),
        ]
    }

    fn execute_body(id: CBytesShort, query_parameters: QueryParams) -> BodyReqExecuteOwned {
        BodyReqExecuteOwned {
            id,
            result_metadata_id: None,
            query_parameters,
        }
    }

    fn prepared_metadata() -> PreparedMetadata {
        PreparedMetadata {
            pk_indexes: vec![0],
            keyspace: Some("demo_ks".into()),
        }
    }

    fn prepare_nodes() -> Vec<CassandraNode> {
        let tokens = prepare_tokens();

        vec![
            CassandraNode::new(
                "172.16.1.10:9042".parse().unwrap(),
                "rack3".into(),
                tokens.get("172.16.1.10").unwrap().clone(),
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.3:9042".parse().unwrap(),
                "rack1".into(),
                tokens.get("172.16.1.3").unwrap().clone(),
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.8:9042".parse().unwrap(),
                "rack3".into(),
                tokens.get("172.16.1.8").unwrap().clone(),
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.6:9042".parse().unwrap(),
                "rack2".into(),
                tokens.get("172.16.1.6").unwrap().clone(),
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.5:9042".parse().unwrap(),
                "rack2".into(),
                tokens.get("172.16.1.5").unwrap().clone(),
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.2:9042".parse().unwrap(),
                "rack1".into(),
                tokens.get("172.16.1.2").unwrap().clone(),
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.9:9042".parse().unwrap(),
                "rack3".into(),
                tokens.get("172.16.1.9").unwrap().clone(),
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.4:9042".parse().unwrap(),
                "rack1".into(),
                tokens.get("172.16.1.4").unwrap().clone(),
                Uuid::new_v4(),
            ),
            CassandraNode::new(
                "172.16.1.7:9042".parse().unwrap(),
                "rack2".into(),
                tokens.get("172.16.1.7").unwrap().clone(),
                Uuid::new_v4(),
            ),
        ]
    }

    fn prepare_tokens() -> HashMap<String, Vec<Murmur3Token>> {
        let data = include_str!("./test_cluster_data.json");

        let parsed: HashMap<String, Vec<i64>> = serde_json::from_str(data).unwrap();

        let mut result = HashMap::new();
        for (address, tokens) in parsed {
            result.insert(
                address,
                tokens
                    .iter()
                    .map(|token| Murmur3Token::new(*token))
                    .collect(),
            );
        }

        result
    }
}
