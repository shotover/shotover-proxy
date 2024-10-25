from kafka import KafkaConsumer
from kafka import KafkaAdminClient
from kafka import KafkaProducer
from kafka.admin import NewTopic
import sys

def main():
    config = eval(sys.argv[1])
    print("Running kafka-python script with config:")
    print(config)

    admin = KafkaAdminClient(**config)
    admin.create_topics([
        NewTopic(
            name='python_test_topic',
            num_partitions=1,
            replication_factor=1
        )
    ])

    producer = KafkaProducer(**config)
    producer.send('python_test_topic', b'some_message_bytes').get(timeout=30)
    producer.send('python_test_topic', b'another_message').get(timeout=30)

    consumer = KafkaConsumer('python_test_topic', auto_offset_reset='earliest', **config)

    msg = next(consumer)
    assert(msg.topic == "python_test_topic")
    assert(msg.value == b"some_message_bytes")
    assert(msg.offset == 0)

    msg = next(consumer)
    assert(msg.topic == "python_test_topic")
    assert(msg.value == b"another_message")
    assert(msg.offset == 1)

    print("kafka-python script passed all test cases")


if __name__ == "__main__":
    main()
