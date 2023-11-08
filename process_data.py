import asyncio
from datetime import datetime
from scipy.interpolate import interp1d
import pandas as pd
from pandas import DataFrame
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import WeatherModel, my_async_session


def get_average_temperature(messages: list[dict]) -> float:
    df = pd.DataFrame(messages)
    return df['temp'].mean()


async def save_to_db(messages: list[dict]) -> int:
    weathers: list[WeatherModel] = []

    for message in messages:
        message['datetime'][7] = None
        weather = WeatherModel(city=message['location'],
                               date=datetime(*message['datetime']),
                               temp=message['temp'],
                               humidity=message['humidity'])
        weathers.append(weather)

    async with my_async_session() as session:
        session: AsyncSession
        session.add_all(weathers)
        await session.commit()

    return len(weathers)


async def get_all_data() -> list[WeatherModel]:
    async with my_async_session() as session:
        session: AsyncSession
        result = await session.execute(select(WeatherModel))
        return [w for w in result.scalars()]


async def extrapolate_db(extrapolate_num: int = 0) -> DataFrame:

    async with my_async_session() as session:
        session: AsyncSession
        result = await session.execute(select(WeatherModel))
    list_of_dicts = [r.to_dict() for r in result.scalars()]
    df = pd.DataFrame(list_of_dicts)
    pos = len(df.index)
    for i in range(extrapolate_num):
        df.loc[pos + i] = [None] * len(df.columns)
        date = df["date"]
        print(date.iloc[pos + i - 1])
        date.loc[pos + i] = pd.Timestamp(date.loc[pos + i - 1]) + \
                            pd.to_timedelta(1, unit='s')
    for_inter = df[["id", "temp", "humidity"]]
    for_inter = for_inter.interpolate(method='spline', order=1)
    df[["id", "temp", "humidity"]] = for_inter
    df["city"].interpolate(method='pad', inplace=True)
    return df.T


# asyncio.run(extrapolate_db())
