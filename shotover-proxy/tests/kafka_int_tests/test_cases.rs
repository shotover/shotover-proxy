use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use std::time::Duration;

async fn produce_consume(brokers: &str, topic_name: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap();

    let delivery_status = producer
        .send_result(FutureRecord::to(topic_name).payload("Message").key("Key"))
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery_status, (0, 0));

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "some_group")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    consumer.subscribe(&[topic_name]).unwrap();

    let message = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
        .await
        .expect("Timeout while receiving from consumer")
        .unwrap();
    let contents = message.payload_view::<str>().unwrap().unwrap();
    assert_eq!("Message", contents);
    assert_eq!(b"Key", message.key().unwrap());
    assert_eq!("foo", message.topic());
    assert_eq!(0, message.offset());
    assert_eq!(0, message.partition());
}

async fn produce_consume_acks0(brokers: &str) {
    let topic_name = "acks0";
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .set("acks", "0")
        .create()
        .unwrap();

    for _ in 0..10 {
        tokio::time::timeout(
            Duration::from_secs(30),
            producer
                .send_result(
                    FutureRecord::to(topic_name)
                        .payload("MessageAcks0")
                        .key("KeyAcks0"),
                )
                .unwrap(),
        )
        .await
        .expect("Timeout while receiving from producer")
        .unwrap()
        .unwrap();
    }

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "some_group")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    consumer.subscribe(&[topic_name]).unwrap();

    for i in 0..10 {
        let message = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
            .await
            .expect("Timeout while receiving from consumer")
            .unwrap();
        let contents = message.payload_view::<str>().unwrap().unwrap();
        assert_eq!("MessageAcks0", contents);
        assert_eq!(b"KeyAcks0", message.key().unwrap());
        assert_eq!("acks0", message.topic());
        assert_eq!(i, message.offset());
        assert_eq!(0, message.partition());
    }
}

pub async fn basic(address: &str) {
    produce_consume(address, "foo").await;
    produce_consume_acks0(address).await;
}
