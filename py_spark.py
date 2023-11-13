from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from lib.kafka_json import KafkaReceiver, KafkaSender

def transform_data(data: dict) -> dict:
    return data

def stream_data(data: dict) -> None:
    producer = KafkaSender('localhost:9092')
    producer.send_data('covid19_stream_data', transform_data(data))
    print('Transformed data sent to Kafka')

if __name__ == '__main__':
    consumer = KafkaReceiver('localhost:9092', 'covid19_raw_data')
    consumer.receive_data(stream_data)

