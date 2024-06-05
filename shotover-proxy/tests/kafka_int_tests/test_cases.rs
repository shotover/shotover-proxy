use test_helpers::connection::kafka::{
    Acl, AclOperation, AclPermissionType, AlterConfig, ConfigEntry, ExpectedResponse,
    KafkaConnectionBuilder, NewPartition, NewTopic, Record, ResourcePatternType, ResourceSpecifier,
    ResourceType,
};

async fn admin_setup(connection_builder: &KafkaConnectionBuilder) {
    let admin = connection_builder.connect_admin().await;
    admin
        .create_topics(&[
            NewTopic {
                name: "partitions1",
                num_partitions: 1,
                replication_factor: 1,
            },
            NewTopic {
                name: "partitions3",
                num_partitions: 3,
                replication_factor: 1,
            },
            NewTopic {
                name: "acks0",
                num_partitions: 1,
                replication_factor: 1,
            },
            NewTopic {
                name: "to_delete",
                num_partitions: 1,
                replication_factor: 1,
            },
        ])
        .await;

    admin
        .create_partitions(&[NewPartition {
            topic_name: "to_delete",
            new_partition_count: 2,
        }])
        .await;

    admin
        // TODO: test ResourceSpecifier::Broker and ResourceSpecifier::Group as well.
        //       Will need to find a way to get a valid broker id and to create a group.
        .describe_configs(&[ResourceSpecifier::Topic("to_delete")])
        .await;

    admin
        .alter_configs(&[AlterConfig {
            specifier: ResourceSpecifier::Topic("to_delete"),
            entries: &[ConfigEntry {
                key: "delete.retention.ms".to_owned(),
                value: "86400001".to_owned(),
            }],
        }])
        .await;

    admin.delete_topics(&["to_delete"]).await
}

async fn produce_consume_partitions1(
    connection_builder: &KafkaConnectionBuilder,
    topic_name: &str,
) {
    {
        let producer = connection_builder.connect_producer(1).await;
        // create an initial record to force kafka to create the topic if it doesnt yet exist
        producer
            .assert_produce(
                Record {
                    payload: "initial",
                    topic_name,
                    key: Some("Key"),
                },
                Some(0),
            )
            .await;

        let mut consumer = connection_builder.connect_consumer(topic_name).await;
        consumer
            .assert_consume(ExpectedResponse {
                message: "initial".to_owned(),
                key: Some("Key".to_owned()),
                topic_name: topic_name.to_owned(),
                offset: Some(0),
            })
            .await;

        // create and consume records
        for i in 0..5 {
            producer
                .assert_produce(
                    Record {
                        payload: "Message1",
                        topic_name,
                        key: Some("Key"),
                    },
                    Some(i * 2 + 1),
                )
                .await;
            producer
                .assert_produce(
                    Record {
                        payload: "Message2",
                        topic_name,
                        key: None,
                    },
                    Some(i * 2 + 2),
                )
                .await;

            consumer
                .assert_consume(ExpectedResponse {
                    message: "Message1".to_owned(),
                    key: Some("Key".to_owned()),
                    topic_name: topic_name.to_owned(),
                    offset: Some(i * 2 + 1),
                })
                .await;
            consumer
                .assert_consume(ExpectedResponse {
                    message: "Message2".to_owned(),
                    key: None,
                    topic_name: topic_name.to_owned(),
                    offset: Some(i * 2 + 2),
                })
                .await;
        }
    }

    // if we create a new consumer it will start from the begginning since `auto.offset.reset = earliest`
    // so we test that we can access all records ever created on this topic
    let mut consumer = connection_builder.connect_consumer(topic_name).await;
    consumer
        .assert_consume(ExpectedResponse {
            message: "initial".to_owned(),
            key: Some("Key".to_owned()),
            topic_name: topic_name.to_owned(),
            offset: Some(0),
        })
        .await;
    for i in 0..5 {
        consumer
            .assert_consume(ExpectedResponse {
                message: "Message1".to_owned(),
                key: Some("Key".to_owned()),
                topic_name: topic_name.to_owned(),
                offset: Some(i * 2 + 1),
            })
            .await;
        consumer
            .assert_consume(ExpectedResponse {
                message: "Message2".to_owned(),
                key: None,
                topic_name: topic_name.to_owned(),
                offset: Some(i * 2 + 2),
            })
            .await;
    }
}

async fn produce_consume_partitions3(connection_builder: &KafkaConnectionBuilder) {
    let topic_name = "partitions3";
    let producer = connection_builder.connect_producer(1).await;
    let mut consumer = connection_builder.connect_consumer(topic_name).await;

    for _ in 0..5 {
        producer
            .assert_produce(
                Record {
                    payload: "Message1",
                    topic_name,
                    key: Some("Key"),
                },
                // We cant predict the offsets since that will depend on which partition the keyless record ends up in
                None,
            )
            .await;
        producer
            .assert_produce(
                Record {
                    payload: "Message2",
                    topic_name,
                    key: None,
                },
                None,
            )
            .await;

        consumer
            .assert_consume_in_any_order(vec![
                ExpectedResponse {
                    message: "Message1".to_owned(),
                    key: Some("Key".to_owned()),
                    topic_name: topic_name.to_owned(),
                    offset: None,
                },
                ExpectedResponse {
                    message: "Message2".to_owned(),
                    key: None,
                    topic_name: topic_name.to_owned(),
                    offset: None,
                },
            ])
            .await;
    }
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

    let mut consumer = connection_builder.connect_consumer(topic_name).await;

    for j in 0..10 {
        consumer
            .assert_consume(ExpectedResponse {
                message: "MessageAcks0".to_owned(),
                key: Some("KeyAcks0".to_owned()),
                topic_name: topic_name.to_owned(),
                offset: Some(j),
            })
            .await;
    }
}

pub async fn standard_test_suite(connection_builder: KafkaConnectionBuilder) {
    admin_setup(&connection_builder).await;
    produce_consume_partitions1(&connection_builder, "partitions1").await;
    produce_consume_partitions1(&connection_builder, "unknown_topic").await;
    produce_consume_partitions3(&connection_builder).await;
    produce_consume_acks0(&connection_builder).await;
    connection_builder.admin_cleanup().await;
}

pub async fn setup_basic_user_acls(connection: &KafkaConnectionBuilder, username: &str) {
    let admin = connection.connect_admin().await;
    admin
        .create_acls(vec![Acl {
            resource_type: ResourceType::Topic,
            resource_name: "*".to_owned(),
            resource_pattern_type: ResourcePatternType::Literal,
            principal: format!("User:{username}"),
            host: "*".to_owned(),
            operation: AclOperation::Describe,
            permission_type: AclPermissionType::Allow,
        }])
        .await;
}

/// Invariants:
/// * The passed connection is a user setup with the ACL's of `setup_basic_user_acls`
/// Assertions:
/// * Asserts that the user cannot perform the admin operation of creating new topics (not allowed by ACL)
///     + Asserts that the topic was not created as a result of the failed topic creation.
/// * Asserts that the user can perform the describe operation on topics (explicitly allowed by ACL)
pub async fn assert_topic_creation_is_denied_due_to_acl(connection: &KafkaConnectionBuilder) {
    let admin = connection.connect_admin().await;
    // attempt to create topic and get auth failure due to missing ACL
    assert_eq!(
        admin
            .create_topics_fallible(&[NewTopic {
                name: "acl_check_topic",
                num_partitions: 1,
                replication_factor: 1,
            }])
            .await
            .unwrap_err()
            .to_string(),
        "org.apache.kafka.common.errors.TopicAuthorizationException: Authorization failed.\n"
    );

    // attempt to describe topic:
    // * The request succeeds because user has AclOperation::Describe.
    // * But no topic is found since the topic creation was denied.
    assert_eq!(
        admin.describe_topic("acl_check_topic").await.unwrap_err().to_string(),
        "org.apache.kafka.common.errors.UnknownTopicOrPartitionException: This server does not host this topic-partition.\n"
    )
}
