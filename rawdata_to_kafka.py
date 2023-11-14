import requests
import json
from lib.kafka_json import KafkaSender

def get_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def send_data():
    API_URL = "http://localhost:8000/"
    producer = KafkaSender('localhost:9092')

    data = get_data(API_URL)
    producer.send_data('covid19_raw_data', data)


if __name__ == '__main__':

    # import threading

    # def set_interval(func, sec):
    #     def func_wrapper():
    #         set_interval(func, sec)
    #         func()
    #     t = threading.Timer(sec, func_wrapper)
    #     t.start()
    #     return t

    # set_interval(send_data, 5)

    send_data()