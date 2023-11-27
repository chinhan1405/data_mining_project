# Requirements:
    - Install required modules in requirements.txt
    - Initialize database: 
        py lib/sqlite.py
    - Initialize Nifi

# How to use this data-pipeline:

Step 1: Run Nifi

Step 2: Run zookeeper and kafka-server

Step 3: Create kafka-consumer that receives data and inserts them into the database.
    py storing_data.py

Step 4: Transforming data in data-pipeline.
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 <<path to processing.py>>
    Check the covid19.db (You may use SQLite Viewer extension in vscode)

Step 5: Visualize