use std::time::Duration;
use test_helpers::connection::kafka::rdkafka::admin::{
    AdminOptions, AlterConfig, NewPartitions, NewTopic, OwnedResourceSpecifier, ResourceSpecifier,
    TopicReplication,
};
use test_helpers::connection::kafka::rdkafka::types::RDKafkaErrorCode;
use test_helpers::connection::kafka::rdkafka::util::Timeout;
use test_helpers::connection::kafka::{ExpectedResponse, KafkaConnectionBuilder, Record};

async fn admin(connection_builder: &KafkaConnectionBuilder) {
    let admin = connection_builder.connect_admin().await;
    admin
        .create_topics(
            &[
                NewTopic {
                    name: "partitions1",
                    num_partitions: 1,
                    replication: TopicReplication::Fixed(1),
                    config: vec![],
                },
                NewTopic {
                    name: "paritions3",
                    num_partitions: 3,
                    replication: TopicReplication::Fixed(1),
                    config: vec![],
                },
                NewTopic {
                    name: "acks0",
                    num_partitions: 1,
                    replication: TopicReplication::Fixed(1),
                    config: vec![],
                },
                NewTopic {
                    name: "to_delete",
                    num_partitions: 1,
                    replication: TopicReplication::Fixed(1),
                    config: vec![],
                },
            ],
            &AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
        )
        .await
        .unwrap();

    let results = admin
        .create_partitions(
            &[NewPartitions {
                // TODO: modify topic "foo" instead so that we can test our handling of that with interesting partiton + replication count
                topic_name: "to_delete",
                new_partition_count: 2,
                assignment: None,
            }],
            &AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
        )
        .await
        .unwrap();
    for result in results {
        let result = result.unwrap();
        assert_eq!(result, "to_delete")
    }

    let results = admin
        .describe_configs(
            // TODO: test ResourceSpecifier::Broker and ResourceSpecifier::Group as well.
            //       Will need to find a way to get a valid broker id and to create a group.
            &[ResourceSpecifier::Topic("to_delete")],
            &AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
        )
        .await
        .unwrap();
    for result in results {
        let result = result.unwrap();
        assert_eq!(
            result.specifier,
            OwnedResourceSpecifier::Topic("to_delete".to_owned())
        );
    }

    let results = admin
        .alter_configs(
            &[AlterConfig {
                specifier: ResourceSpecifier::Topic("to_delete"),
                entries: [("foo", "bar")].into(),
            }],
            &AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
        )
        .await
        .unwrap();
    for result in results {
        assert_eq!(
            result.unwrap(),
            OwnedResourceSpecifier::Topic("to_delete".to_owned())
        );
    }

    let results = admin
        .delete_topics(
            &["to_delete"],
            &AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
        )
        .await
        .unwrap();
    for result in results {
        assert_eq!(result.unwrap(), "to_delete");
    }
}

async fn admin_cleanup(connection_builder: &KafkaConnectionBuilder) {
    let admin = connection_builder.connect_admin().await;
    let results = admin
        // The cpp driver will lock up when running certain commands after a delete_groups if the delete_groups is targeted at a group that doesnt exist.
        // So just make sure to run it against a group that does exist.
        .delete_groups(
            &["some_group"],
            &AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(30)))),
        )
        .await
        .unwrap();
    for result in results {
        match result {
            Ok(result) => assert_eq!(result, "some_group"),
            Err(err) => assert_eq!(
                err,
                // Allow this error which can occur due to race condition in the test, but do not allow any other error types
                ("some_group".to_owned(), RDKafkaErrorCode::NonEmptyGroup)
            ),
        }
    }
}

async fn produce_consume(connection_builder: &KafkaConnectionBuilder, topic_name: &str, i: i64) {
    let producer = connection_builder.connect_producer(1).await;

    producer
        .assert_produce(
            Record {
                payload: "Message1",
                topic_name,
                key: Some("Key"),
            },
            Some(i * 2),
        )
        .await;
    producer
        .assert_produce(
            Record {
                payload: "Message2",
                topic_name,
                key: None,
            },
            Some(i * 2 + 1),
        )
        .await;

    let consumer = connection_builder.connect_consumer(topic_name).await;

    consumer
        .assert_consume(ExpectedResponse {
            message: "Message1",
            key: Some("Key"),
            topic_name,
            offset: 0,
        })
        .await;
    consumer
        .assert_consume(ExpectedResponse {
            message: "Message2",
            key: None,
            topic_name,
            offset: 1,
        })
        .await;
}

async fn produce_consume_acks0(connection_builder: &KafkaConnectionBuilder) {
    let topic_name = "acks0";
    let producer = connection_builder.connect_producer(0).await;

    for _ in 0..10 {
        producer
            .assert_produce(
                Record {
                    payload: "MessageAcks0",
                    topic_name,
                    key: Some("KeyAcks0"),
                },
                None,
            )
            .await;
    }

    let consumer = connection_builder.connect_consumer(topic_name).await;

    for j in 0..10 {
        consumer
            .assert_consume(ExpectedResponse {
                message: "MessageAcks0",
                key: Some("KeyAcks0"),
                topic_name,
                offset: j,
            })
            .await;
    }
}

pub async fn basic(connection_builder: KafkaConnectionBuilder) {
    admin(&connection_builder).await;
    for i in 0..2 {
        produce_consume(&connection_builder, "partitions1", i).await;
        produce_consume(&connection_builder, "partitions3", i).await;
        produce_consume_acks0(&connection_builder).await;
    }
    admin_cleanup(&connection_builder).await;
}
