from kafka import KafkaConsumer
from kafka import KafkaAdminClient
from kafka import KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
from time import sleep
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

    # send first message with retry since the topic may not be created yet.
    retry_if_not_ready(lambda : producer.send('python_test_topic', b'some_message_bytes').get(timeout=30))

    # send second message without retry, it has no reason to fail.
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

def retry_if_not_ready(attempt):
    tries = 0
    while True:
        try:
            attempt()
            return
        except UnknownTopicOrPartitionError:
            tries += 1
            sleep(0.1)
            # fail after 10s worth of attempts
            if tries > 100:
                raise Exception("Timedout, hit UnknownTopicOrPartitionError 100 times in a row")


if __name__ == "__main__":
    main()
