from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Covid19")\
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0")\
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "covid19_raw_data") \
    .option("startingOffsets", "latest") \
    .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# df.show()
checkpointDir = "C:/checkpoint/"
flower_agg_write_stream = df \
        .writeStream \
        .format("kafka") \
        .outputMode("append")\
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("topic", "hehe4") \
        .trigger(processingTime="10 seconds")\
        .option("checkpointLocation", checkpointDir)\
        .start()

flower_agg_write_stream.awaitTermination()
