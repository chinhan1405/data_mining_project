from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField('Country', StringType(), True),
    StructField('CountryCode', StringType(), False),
    StructField('NewConfirmed', IntegerType(), True),
    StructField('TotalConfirmed', IntegerType(), True),
    StructField('Date', StringType(), False)
])

spark = SparkSession.builder.appName("Covid19")\
    .getOrCreate()

def transform_data() -> dict:
    # global spark
    # transformed_data = []
    # for country_data in data['Countries']:
    #     transformed_data.append({
    #         'Country': country_data['Country'],
    #         'CountryCode': country_data['CountryCode'],
    #         'NewConfirmed': country_data['NewConfirmed'],
    #         'TotalConfirmed': country_data['TotalConfirmed'],
    #         'Date': country_data['Date']
    #     })
    # df = spark.createDataFrame(transformed_data, schema)
    df = spark.readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "covid19_raw_data") \
        .option("startingOffsets", "latest") \
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    checkpointDir = "C:/checkpoint/"
    flower_agg_write_stream = df \
            .writeStream \
            .format("kafka") \
            .outputMode("append")\
            .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
            .option("topic", "covid19_data") \
            .trigger(processingTime="10 seconds")\
            .option("checkpointLocation", checkpointDir)\
            .start()
    # return {"Countries" : transformed_data}
    flower_agg_write_stream.awaitTermination()

if __name__ == '__main__':
    transform_data()

