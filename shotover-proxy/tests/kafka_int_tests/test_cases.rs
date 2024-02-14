use test_helpers::connection::kafka::{
    ExpectedResponse, KafkaConnectionBuilder, NewPartition, NewTopic, Record,
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
                name: "paritions3",
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
            // TODO: modify topic "foo" instead so that we can test our handling of that with interesting partition + replication count
            topic_name: "to_delete",
            new_partition_count: 2,
        }])
        .await;
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
    admin_setup(&connection_builder).await;
    connection_builder.admin_setup().await;
    for i in 0..2 {
        produce_consume(&connection_builder, "partitions1", i).await;
        produce_consume(&connection_builder, "partitions3", i).await;
        produce_consume_acks0(&connection_builder).await;
    }
    connection_builder.admin_cleanup().await;
}

pub async fn basic_sasl(address: &str) {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", address)
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "user")
        .set("sasl.password", "password")
        .set("security.protocol", "SASL_PLAINTEXT")
        // internal driver debug logs are emitted to tokio tracing, assuming the appropriate filter is used by the tracing subscriber
        .set("debug", "all");
    admin(client.clone()).await;
    produce_consume(client.clone()).await;
    produce_consume_acks0(client.clone()).await;
    admin_cleanup(client.clone()).await;
}
