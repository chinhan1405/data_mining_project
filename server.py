from fastapi import FastAPI
from lib.sqlite import Sqlite

app = FastAPI()

db = Sqlite("covid19.db")

def get_data():
    res = { "result" : [] }
    for record in db.select("Countries", "*", "TRUE"):
        res["result"].append(
            {
                "Country" : record[0],
                "CountryCode" : record[1],
                "NewConfirmed" : record[2],
                "TotalConfirmed" : record[3],
                "Date" : record[4]
            }
        )
    return res


@app.get("/")
async def root():
    return get_data()