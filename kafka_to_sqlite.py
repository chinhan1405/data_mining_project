from lib.kafka_json import KafkaReceiver
from lib.sqlite import Sqlite

if __name__ == '__main__':
    conn = Sqlite('covid19.db')

    def insert_data(data):
        for dataframe in data['Countries']:
            conn.insert('Countries', [
                dataframe.get('Country'),
                dataframe.get('CountryCode'),
                dataframe.get('NewConfirmed'),
                dataframe.get('TotalConfirmed'),
                dataframe.get('Date')
            ])
        print('Data inserted into database')

    consumer = KafkaReceiver('localhost:9092', 'covid19_data')
    consumer.receive_data(insert_data)