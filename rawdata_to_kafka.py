import requests
import json
from lib.kafka_json import KafkaSender

def get_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None


if __name__ == '__main__':
    API_URL = "https://s3.amazonaws.com/hackerday.bigdata/113/dataset_july3rd.json"
    producer = KafkaSender('localhost:9092')

    data = get_data(API_URL)
    producer.send_data('covid19_raw_data', data)