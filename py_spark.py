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

def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    col = split(sdf['value'], ',') #split attributes to nested array in one Column
    #now expand col to multiple top-level columns
    for idx, field in enumerate(schema): 
        sdf  = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])

def transform_data() -> dict:
    pass
    

if __name__ == '__main__':
    df = spark.readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "covid19_raw_data") \
        .option("startingOffsets", "latest") \
        .load()
    df.selectExpr("CAST(value AS STRING)")
    # df.printSchema()
    # dfCSV = parse_data_from_kafka_message(dfCSV, userSchema)
    checkpointDir = "C:/checkpoint/"
    flower_agg_write_stream = df \
            .writeStream \
            .format("kafka") \
            .outputMode("append")\
            .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
            .option("topic", "covid19_data") \
            .trigger(processingTime="5 seconds")\
            .option("checkpointLocation", checkpointDir)\
            .start()
    flower_agg_write_stream.awaitTermination()

