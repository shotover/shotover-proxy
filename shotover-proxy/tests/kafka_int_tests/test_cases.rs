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
                    name: "foo",
                    num_partitions: 1,
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

async fn produce_consume(client: ClientConfig) {
    let topic_name = "foo";
    let producer: FutureProducer = client
        .clone()
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

    let consumer: StreamConsumer = client
        .clone()
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

async fn produce_consume_acks0(client: ClientConfig) {
    let topic_name = "acks0";
    let producer: FutureProducer = client
        .clone()
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

    let consumer: StreamConsumer = client
        .clone()
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
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", address)
        // internal driver debug logs are emitted to tokio tracing, assuming the appropriate filter is used by the tracing subscriber
        .set("debug", "all");
    admin(client.clone()).await;
    produce_consume(client.clone()).await;
    produce_consume_acks0(client.clone()).await;
    admin_cleanup(client.clone()).await;
}
