from lib.kafka_json import KafkaReceiver
from lib.sqlite import Sqlite

if __name__ == '__main__':
    conn = Sqlite('covid19.db')

    def insert_data(data):
        conn.insert('Countries', [
            data.get('Country'),
            data.get('CountryCode'),
            data.get('NewConfirmed'),
            data.get('TotalConfirmed'),
            data.get('Date')
        ])
        print('Data inserted into database')

    consumer = KafkaReceiver('localhost:9092', 'covid19_data')
    consumer.receive_data(insert_data)