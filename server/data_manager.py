import json
import random
from datetime import datetime, timedelta


class DataManager:

    NUMERICAL_ATTRS = (
        'NewConfirmed', 
        'TotalConfirmed', 
        'NewDeaths', 
        'TotalDeaths', 
        'NewRecovered', 
        'TotalRecovered', 
    )

    def __init__(self, data_file: str) -> None:
        self.data = json.load(open(data_file, 'r'))
        self.global_data = self.data['Global']
        self.countries_data = self.data['Countries']
        self.date = datetime.strptime('2020-07-03', '%Y-%m-%d')

    def __update_attribute__(self, value) -> None:
        variation = max(1, int(value*0.2))
        return max(0, value + random.randint(-variation, variation))

    def pseudo_update(self):
        for country_data in self.countries_data:
            country_data['NewConfirmed'] = self.__update_attribute__(country_data['NewConfirmed'])
            country_data['TotalConfirmed'] += country_data['NewConfirmed']
            country_data['NewDeaths'] = self.__update_attribute__(country_data['NewDeaths'])
            country_data['TotalDeaths'] += country_data['NewDeaths']
            country_data['NewRecovered'] = self.__update_attribute__(country_data['NewRecovered'])
            country_data['TotalRecovered'] += country_data['NewRecovered']
            country_data['Date'] = self.date.strftime('%Y-%m-%d')
        for attr in self.NUMERICAL_ATTRS:
            self.global_data[attr] = sum([country_data[attr] for country_data in self.countries_data])
            self.global_data['Date'] = self.date.strftime('%Y-%m-%d')
        self.date += timedelta(days=1)
        print('Data updated')

    def update_data(self):
        json.dump(self.data, open('data.json', 'w'))
           