from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import from_json, to_json, struct, explode
from lib.sqlite import Sqlite

schema_raw = StructType([
    StructField('Global', StructType([
        StructField('NewConfirmed', IntegerType(), True),
        StructField('TotalConfirmed', IntegerType(), True),
        StructField('NewDeaths', IntegerType(), True),
        StructField('TotalDeaths', IntegerType(), True),
        StructField('NewRecovered', IntegerType(), True),
        StructField('TotalRecovered', IntegerType(), True)
    ]), True),
    StructField('Countries', ArrayType
    (
        StructType
        ([
        StructField('Country', StringType(), True),
        StructField('CountryCode', StringType(), False),
        StructField('NewConfirmed', IntegerType(), True),
        StructField('TotalConfirmed', IntegerType(), True),
        StructField('NewDeaths', IntegerType(), True),
        StructField('TotalDeaths', IntegerType(), True),
        StructField('NewRecovered', IntegerType(), True),
        StructField('TotalRecovered', IntegerType(), True),
        StructField('Date', StringType(), False)
        ])
        , True
    )
    , True)    
])

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

def write_to_sqlite(df, epoch_id):
        pandas_df = df.toPandas()
        conn = Sqlite('covid19.db')
        for index, row in pandas_df.iterrows():
            conn.insert('Countries', [
                row['Country'],
                row['CountryCode'],
                row['NewConfirmed'],
                row['TotalConfirmed'],
                row['Date']
            ])
        conn.close()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Covid19") \
        .getOrCreate()

    df = spark.readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "covid19_raw_data") \
        .option("startingOffsets", "latest") \
        .load()

    new_df = parse_data_from_kafka_message(df, schema_raw)

    new_df = new_df.select(explode(new_df["Countries"]).alias("Countries"))

    new_df = new_df.select("Countries.Country", "Countries.CountryCode", "Countries.NewConfirmed", "Countries.TotalConfirmed", "Countries.Date")
    


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

    # new_df.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .trigger(processingTime="5 seconds")\
    #     .start() \
    #     .awaitTermination()
    


