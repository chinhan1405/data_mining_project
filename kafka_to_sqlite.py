from lib.kafka_json import KafkaReceiver
from datetime import datetime
from lib.sqlite import Sqlite

if __name__ == '__main__':
    conn = Sqlite('covid19.db')

    def insert_data(data):
        data_temp = data['Global']
        conn.insert('Global', [
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            data_temp.get('NewConfirmed'),
            data_temp.get('TotalConfirmed'),
            data_temp.get('NewDeaths'),
            data_temp.get('TotalDeaths'),
            data_temp.get('NewRecovered'),
            data_temp.get('TotalRecovered')
        ])
        data_temp = data['Countries']
        for i in range(len(data_temp)):
            conn.insert('Countries', [
                data_temp[i].get('Country'),
                data_temp[i].get('CountryCode'),
                data_temp[i].get('Slug'),
                data_temp[i].get('NewConfirmed'),
                data_temp[i].get('TotalConfirmed'),
                data_temp[i].get('NewDeaths'),
                data_temp[i].get('TotalDeaths'),
                data_temp[i].get('NewRecovered'),
                data_temp[i].get('TotalRecovered'),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ])

    consumer = KafkaReceiver('localhost:9092', 'covid19_raw_data')
    consumer.receive_data(insert_data)