from kafka import KafkaProducer, KafkaConsumer
import json

class KafkaSender(KafkaProducer):
    def __init__(self, server: str):
        super().__init__(bootstrap_servers=server)

    def send_data(self, topic_name: str, data: dict):
        try:
            self.send(topic_name, json.dumps(data).encode('utf-8'))
            self.flush()
            print('Message published successfully.')
        except Exception as ex:
            print('Exception in publishing message.')
            print(str(ex))

    def send_data_string(self, topic_name: str, data: str):
        try:
            self.send(topic_name, data.encode('utf-8'))
            self.flush()
            print('Message published successfully.')
        except Exception as ex:
            print('Exception in publishing message.')
            print(str(ex))


class KafkaReceiver(KafkaConsumer):
    def __init__(self, server: str, topic: str):
        super().__init__(topic, bootstrap_servers=server)

    def receive_data(self, callback: callable):
        for message in self:
            callback(json.loads(message.value.decode('utf-8')))