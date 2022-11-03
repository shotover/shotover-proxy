#[cfg(test)]
mod test_token_aware_router {
    use super::super::node_pool::NodePool;
    use super::super::routing_key::calculate_routing_key;
    use crate::transforms::cassandra::sink_cluster::node::CassandraNode;
    use cassandra_protocol::consistency::Consistency::One;
    use cassandra_protocol::frame::message_execute::BodyReqExecuteOwned;
    use cassandra_protocol::frame::message_result::PreparedMetadata;
    use cassandra_protocol::frame::message_result::{
        ColSpec, ColType::Varchar, ColTypeOption, TableSpec,
    };
    use cassandra_protocol::frame::Version;
    use cassandra_protocol::query::QueryParams;
    use cassandra_protocol::query::QueryValues::SimpleValues;
    use cassandra_protocol::token::Murmur3Token;
    use cassandra_protocol::types::value::Value;
    use cassandra_protocol::types::CBytesShort;
    use rand::prelude::*;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_router() {
        let mut rng = SmallRng::from_rng(rand::thread_rng()).unwrap();

        let mut router = NodePool::new(prepare_nodes());

        let id = CBytesShort::new(vec![
            11, 241, 38, 11, 140, 72, 217, 34, 214, 128, 175, 241, 151, 73, 197, 227,
        ]);

        router
            .add_prepared_result(id.clone(), prepared_metadata().clone())
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

            let token = Murmur3Token::generate(
                &calculate_routing_key(
                    &prepared_metadata().pk_indexes,
                    query_parameters.values.as_ref().unwrap(),
                    Version::V4,
                )
                .unwrap(),
            );

            assert_eq!(token, test_token);

            let node = router
                .get_replica_node_in_dc(
                    &execute_body(id.clone(), query_parameters),
                    "rack1",
                    &Version::V4,
                    &mut rng,
                    3,
                )
                .await
                .unwrap()
                .unwrap();

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
            global_table_spec: Some(TableSpec {
                ks_name: "demo_ks".into(),
                table_name: "books_by_author".into(),
            }),
            col_specs: vec![ColSpec {
                table_spec: None,
                name: "author".into(),
                col_type: ColTypeOption {
                    id: Varchar,
                    value: None,
                },
            }],
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
