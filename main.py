import asyncio

import orjson
import pandas as pd
from fastapi import FastAPI

from connect_mqtt import mqtt_client, messages_in_bytes
from process_data import get_average_temperature, save_to_db, get_all_data, extrapolate_db

app = FastAPI()


@app.get("/")
async def root():
    json = None
    if len(messages_in_bytes) != 0:
        json = orjson.loads(messages_in_bytes[-1])
    return {"last_message": json}


@app.get("/at")
async def average_temperature():
    res = None
    if len(messages_in_bytes) != 0:
        jsons: list[dict] = [orjson.loads(m) for m in messages_in_bytes]
        res: float = get_average_temperature(jsons)
        res = round(res, 2)
    return {"average_temp": res}


@app.get("/save_db")
async def save_db():
    saved_int: int = await save_to_db([orjson.loads(m) for m in messages_in_bytes])
    return {"saved": saved_int}


@app.get("/clear")
async def clear():
    lenght = len(messages_in_bytes)
    messages_in_bytes.clear()
    return {"cleared": lenght}


@app.get("/show_db")
async def show_db():
    return {"db": await get_all_data()}


@app.get("/extrapolate/:extrapolate_num")
async def extrapolate(extrapolate_num: int):
    if extrapolate_num <= 0:
        return {"extrapolated": None}
    df: pd.DataFrame = await extrapolate_db(extrapolate_num)
    return {"extrapolated": df}


async def read_loop_async():
    while True:
        mqtt_client.loop(timeout=0.1 ** 100)
        await asyncio.sleep(0.1)


asyncio.create_task(read_loop_async())
