import asyncio

import orjson
from fastapi import FastAPI

from connect_mqtt import last_message_in_bytes, mqtt_client

app = FastAPI()


@app.get("/")
async def root():
    json = None
    if len(last_message_in_bytes) != 0:
        json = orjson.loads(last_message_in_bytes[-1])
    return {"last message": json}


async def read_loop_async():
    while True:
        mqtt_client.loop(timeout=0.1 ** 100)
        await asyncio.sleep(0.1)


asyncio.create_task(read_loop_async())
