from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, to_json, struct


schema = StructType([
    StructField('Country', StringType(), True),
    StructField('CountryCode', StringType(), False),
    StructField('NewConfirmed', IntegerType(), True),
    StructField('TotalConfirmed', IntegerType(), True),
    StructField('Date', StringType(), False)
])

def parse_data_from_kafka_message(sdf, schema):
    sdf = sdf.selectExpr("CAST(value AS STRING)")
    sdf = sdf.select(from_json("value", schema).alias("value"))
    return sdf.select("value.*")


spark = SparkSession.builder.appName("Covid19")\
    .getOrCreate()

    

if __name__ == '__main__':
    df = spark.readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "covid19_raw_data") \
        .option("startingOffsets", "latest") \
        .load()

    new_df = parse_data_from_kafka_message(df, schema)

    
    json_df = new_df.select(to_json(struct("*")).alias("value"))

    checkpointDir = "C:/checkpoint/"
    json_df \
        .writeStream \
        .format("kafka") \
        .outputMode("append")\
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("topic", "covid19_data") \
        .trigger(processingTime="5 seconds")\
        .option("checkpointLocation", checkpointDir)\
        .start() \
        .awaitTermination()

    # json_df.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .trigger(processingTime="5 seconds")\
    #     .start() \
    #     .awaitTermination()
    


