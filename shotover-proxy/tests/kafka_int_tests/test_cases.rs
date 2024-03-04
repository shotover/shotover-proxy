use test_helpers::connection::kafka::{ExpectedResponse, KafkaConnectionBuilder, Record};

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
    connection_builder.admin_setup().await;
    for i in 0..2 {
        produce_consume(&connection_builder, "partitions1", i).await;
        produce_consume(&connection_builder, "partitions3", i).await;
        produce_consume_acks0(&connection_builder).await;
    }
    connection_builder.admin_cleanup().await;
}
