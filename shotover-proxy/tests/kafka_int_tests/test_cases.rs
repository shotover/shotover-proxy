use std::time::Duration;
use test_helpers::rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, NewPartitions, NewTopic, OwnedResourceSpecifier,
    ResourceSpecifier, TopicReplication,
};
use test_helpers::rdkafka::client::DefaultClientContext;
use test_helpers::rdkafka::config::ClientConfig;
use test_helpers::rdkafka::consumer::{Consumer, StreamConsumer};
use test_helpers::rdkafka::producer::{FutureProducer, FutureRecord};
use test_helpers::rdkafka::types::RDKafkaErrorCode;
use test_helpers::rdkafka::util::Timeout;
use test_helpers::rdkafka::Message;

async fn admin(config: ClientConfig) {
    let admin: AdminClient<DefaultClientContext> = config.create().unwrap();
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

async fn admin_cleanup(config: ClientConfig) {
    let admin: AdminClient<DefaultClientContext> = config.create().unwrap();
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

async fn assert_produce(
    producer: &FutureProducer,
    record: Record<'_>,
    expected_offset: Option<i64>,
) {
    let send = match record.key {
        Some(key) => producer
            .send_result(
                FutureRecord::to(record.topic_name)
                    .payload(record.payload)
                    .key(key),
            )
            .unwrap(),
        None => producer
            .send_result(FutureRecord::<(), _>::to(record.topic_name).payload(record.payload))
            .unwrap(),
    };
    let delivery_status = tokio::time::timeout(Duration::from_secs(30), send)
        .await
        .expect("Timeout while receiving from producer")
        .unwrap()
        .unwrap();

    if let Some(offset) = expected_offset {
        assert_eq!(delivery_status.1, offset, "Unexpected offset");
    }
}

struct Record<'a> {
    payload: &'a str,
    topic_name: &'a str,
    key: Option<&'a str>,
}

async fn assert_consume(consumer: &StreamConsumer, response: ExpectedResponse<'_>) {
    let message = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
        .await
        .expect("Timeout while receiving from consumer")
        .unwrap();
    let contents = message.payload_view::<str>().unwrap().unwrap();
    assert_eq!(response.message, contents);
    assert_eq!(
        response.key,
        message.key().map(|x| std::str::from_utf8(x).unwrap())
    );
    assert_eq!(response.topic_name, message.topic());
    assert_eq!(response.offset, message.offset());
}

struct ExpectedResponse<'a> {
    message: &'a str,
    key: Option<&'a str>,
    topic_name: &'a str,
    offset: i64,
}

async fn produce_consume(client: ClientConfig, topic_name: &str, i: i64) {
    let producer: FutureProducer = client
        .clone()
        .set("message.timeout.ms", "5000")
        .create()
        .unwrap();

    assert_produce(
        &producer,
        Record {
            payload: "Message1",
            topic_name,
            key: Some("Key"),
        },
        Some(i * 2),
    )
    .await;
    assert_produce(
        &producer,
        Record {
            payload: "Message2",
            topic_name,
            key: None,
        },
        Some(i * 2 + 1),
    )
    .await;

    let consumer: StreamConsumer = client
        .clone()
        .set("group.id", "some_group")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    consumer.subscribe(&[topic_name]).unwrap();

    assert_consume(
        &consumer,
        ExpectedResponse {
            message: "Message1",
            key: Some("Key"),
            topic_name,
            offset: 0,
        },
    )
    .await;
    assert_consume(
        &consumer,
        ExpectedResponse {
            message: "Message2",
            key: None,
            topic_name,
            offset: 1,
        },
    )
    .await;
}

async fn produce_consume_acks0(client: ClientConfig) {
    let topic_name = "acks0";
    let producer: FutureProducer = client
        .clone()
        .set("message.timeout.ms", "5000")
        .set("acks", "0")
        .create()
        .unwrap();

    for _ in 0..10 {
        assert_produce(
            &producer,
            Record {
                payload: "MessageAcks0",
                topic_name,
                key: Some("KeyAcks0"),
            },
            None,
        )
        .await;
    }

    let consumer: StreamConsumer = client
        .clone()
        .set("group.id", "some_group")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    consumer.subscribe(&[topic_name]).unwrap();

    for j in 0..10 {
        assert_consume(
            &consumer,
            ExpectedResponse {
                message: "MessageAcks0",
                key: Some("KeyAcks0"),
                topic_name,
                offset: j,
            },
        )
        .await;
    }
}

pub async fn basic(address: &str) {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", address)
        // internal driver debug logs are emitted to tokio tracing, assuming the appropriate filter is used by the tracing subscriber
        .set("debug", "all");
    admin(client.clone()).await;
    for i in 0..2 {
        produce_consume(client.clone(), "partitions1", i).await;
        produce_consume(client.clone(), "partitions3", i).await;
        produce_consume_acks0(client.clone()).await;
    }
    admin_cleanup(client.clone()).await;
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
