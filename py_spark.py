# from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from lib.kafka_json import KafkaReceiver, KafkaSender

schema = StructType([
    StructField('Country', StringType(), True),
    StructField('CountryCode', StringType(), False),
    StructField('NewConfirmed', IntegerType(), True),
    StructField('TotalConfirmed', IntegerType(), True),
    StructField('Date', StringType(), False)
])

# spark = SparkSession.builder.appName("Covid19").getOrCreate()

def transform_data(data: dict) -> dict:
    global spark
    transformed_data = []
    for country_data in data['Countries']:
        transformed_data.append({
            'Country': country_data['Country'],
            'CountryCode': country_data['CountryCode'],
            'NewConfirmed': country_data['NewConfirmed'],
            'TotalConfirmed': country_data['TotalConfirmed'],
            'Date': country_data['Date']
        })
    # df = spark.createDataFrame(transformed_data, schema)
    # df.show()
    return {"Countries" : transformed_data}

def stream_data(data: dict) -> None:
    producer = KafkaSender('localhost:9092')
    producer.send_data('covid19_stream_data', transform_data(data))
    print('Transformed data sent to Kafka')

if __name__ == '__main__':
    consumer = KafkaReceiver('localhost:9092', 'covid19_raw_data')
    consumer.receive_data(stream_data)

