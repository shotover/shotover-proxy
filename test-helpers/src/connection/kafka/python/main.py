from kafka import KafkaConsumer
from kafka import KafkaProducer
import sys

def main():
    config = eval(sys.argv[1])
    print("Running kafka-python script with config:")
    print(config)

    producer = KafkaProducer(**config)
    producer.send('test_topic', b'some_message_bytes').get(timeout=10)
    producer.send('test_topic', b'another_message').get(timeout=10)

    consumer = KafkaConsumer('test_topic', auto_offset_reset='earliest', **config)

    msg = next(consumer)
    assert(msg.topic == "test_topic")
    assert(msg.value == b"some_message_bytes")
    assert(msg.offset == 0)

    msg = next(consumer)
    assert(msg.topic == "test_topic")
    assert(msg.value == b"another_message")
    assert(msg.offset == 1)

    print("kafka-python script passed all test cases")


if __name__ == "__main__":
    main()
