# Requirements:
    - Install required modules in requirements.txt
    - Initialize database: 
        py lib/sqlite.py
    - spark-3.3.0-bin-hadoop2
    - nifi-1.23.2-bin
    - kafka_2.12-3.6.0
    

# How to use this data-pipeline:

Step 1: Run Nifi
    powershell:
        cd <<path to nifi folder>>
        .\bin\run-nifi.bat
    Open Nifi on https://127.0.0.1:8443/nifi/login
    Open logs/nifi-app.log to get Username and Password.
    Login to Nifi and create a data flow
    Upload template from covid19_nifi.xml
    

Step 2: Run Zookeeper
    powershell:
        zkServer


Step 3: Run Kafka server
    powershell
        cd <<path to kafka folder>>
        .\bin\windows\kafka-server-start .\config\server.properties


Step 3: Create kafka-consumer that receives data and inserts them into the database.
    py storing_data.py


Step 4: Transforming data in data-pipeline.
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 <<path to processing.py>>
    Check the covid19.db (You may use SQLite Viewer extension in vscode)


Step 5: Start the data flow on Nifi.


Step 6: Start the API server for querying data from the database
    py -m uvicorn server:app


Step 7: Visualize