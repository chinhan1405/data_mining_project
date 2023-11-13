import sqlite3
from datetime import datetime, date

class Sqlite:
    def __init__(self, db:str):
        self.conn = sqlite3.connect(db)
        self.curs = self.conn.cursor()

    def create_table(self, table_name:str, *columns_name:str):
        columns = ', '.join(columns_name)
        self.curs.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
        self.conn.commit()

    def drop_table(self, table_name:str):
        self.curs.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.conn.commit()

    def __format_data__(self, data):
        if isinstance(data, str):
            return f'"{data}"'
        else:
            return f"{data}"

    def insert(self, table_name:str, values_list:list):
        values = ''
        for i in range(len(values_list)-1):
            values += f"{self.__format_data__(values_list[i])}, "
        values += f"{self.__format_data__(values_list[-1])}"
        self.curs.execute(f"INSERT INTO {table_name} VALUES ({values})")
        self.conn.commit()

    def select(self, table_name, columns, where):
        self.curs.execute(f"SELECT {columns} FROM {table_name} WHERE {where}")
        return self.curs.fetchall()

    def update(self, table_name, set, where):
        self.curs.execute(f"UPDATE {table_name} SET {set} WHERE {where}")
        self.conn.commit()

    def delete(self, table_name, where):
        self.curs.execute(f"DELETE FROM {table_name} WHERE {where}")
        self.conn.commit()

    def close(self):
        self.conn.close()

if __name__ == '__main__':
    conn = Sqlite('covid19.db')
    
    conn.drop_table('Countries')

    conn.create_table('Countries', 
        'Country NVARCHAR(50)', 
        'CountryCode VARCHAR(2) NOT NULL',
        'NewConfirmed INT', 
        'TotalConfirmed INT', 
        # 'NewDeaths INT', 
        # 'TotalDeaths INT', 
        # 'NewRecovered INT', 
        # 'TotalRecovered INT', 
        'Date TEXT NOT NULL'
    )

    # conn.create_table('Global',
    #     'DateTime PRIMARY KEY',
    #     'NewConfirmed INT',
    #     'TotalConfirmed INT',
    #     'NewDeaths INT',
    #     'TotalDeaths INT',
    #     'NewRecovered INT',
    #     'TotalRecovered INT'
    # )

    # conn.delete('Countries', "TRUE")
    # conn.drop_table('Global')
    # conn.drop_table('Countries')
    conn.close()