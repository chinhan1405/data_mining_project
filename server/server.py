from fastapi import FastAPI
from data_manager import DataManager

app = FastAPI()

data = DataManager('data.json')

@app.get("/")
async def root():
    return data.data

import threading

def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()
    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t

set_interval(data.pseudo_update, 10)
