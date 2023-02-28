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

    let message = tokio::time::timeout(Duration::from_secs(10), consumer.recv())
        .await
        .expect("Timeout while receiving from producer")
        .unwrap();
    let contents = message.payload_view::<str>().unwrap().unwrap();
    assert_eq!("Message", contents);
    assert_eq!(b"Key", message.key().unwrap());
    assert_eq!("foo", message.topic());
    assert_eq!(0, message.offset());
    assert_eq!(0, message.partition());
}

pub async fn basic() {
    produce_consume("localhost:9192", "foo").await;
}
