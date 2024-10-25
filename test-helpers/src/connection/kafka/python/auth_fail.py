from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys

def main():
    config = eval(sys.argv[1])
    print("Running kafka-python script with config:")
    print(config)

    try:
        KafkaProducer(**config)
        raise Exception("KafkaProducer was succesfully created but expected to fail due to using incorrect username/password")
    except KafkaError:
        print("kafka-python auth_fail script passed all test cases")

if __name__ == "__main__":
    main()
