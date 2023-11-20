# Requirements:
    - Install required modules in requirements.txt
    - Initialize database: 
        py lib/sqlite.py

# How to use this data-pipeline:

Step 1: Run zookeeper and kafka-server

Step 2: Create kafka-consumer that receives data and inserts them into the database.
    py kafka_to_sqlite.py

Step 3: Transforming data in data-pipeline.
    .\bin\spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 D:\Course\HK-231\Khai_pha_du_lieu\Assignment\covid19_datapipeline\data_pipeline\py_spark.py

Step 3: Fetch data from the api and send to kafka every 5 seconds (run this in a different terminal)
    py rawdata_to_kafka.py

Step 4: Check the covid19.db (You may use SQLite Viewer extension in vscode)

