from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Covid19").getOrCreate()

df = spark.read.format("kafka")\
.option("kafka.bootstrap.servers", "localhost:9092")\
.option("subscribe", "covid19_raw_data")\
.load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df.show()

spark.stop()
