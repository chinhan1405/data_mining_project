from lib.kafka_json import KafkaReceiver
from lib.sqlite import Sqlite


def insert_data(data):
    conn = Sqlite('covid19.db')

    conn.insert('Countries', [
        data.get('Country'),
        data.get('CountryCode'),
        data.get('NewConfirmed'),
        data.get('TotalConfirmed'),
        data.get('Date')
    ])
    
    conn.close()
    print('Data inserted into database')

if __name__ == '__main__':
    
    consumer = KafkaReceiver('localhost:9092', 'covid19_data')
    consumer.receive_data(insert_data)