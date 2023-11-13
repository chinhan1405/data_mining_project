import json
import random
from datetime import datetime


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
            country_data['Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for attr in self.NUMERICAL_ATTRS:

            self.global_data[attr] = sum([country_data[attr] for country_data in self.countries_data])

        print('Data updated')

    def update_data(self):
        self.pseudo_update()
        json.dump(self.data, open('data.json', 'w'))
           