import asyncio
import datetime

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase

engine = create_async_engine("sqlite+aiosqlite:///file.db", echo=True)

my_async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class WeatherModel(Base):
    __tablename__ = 'weather_data'

    id = sa.Column(sa.Integer, primary_key=True)
    city = sa.Column(sa.String(50), nullable=False)
    date = sa.Column(sa.DateTime, nullable=False)
    temp = sa.Column(sa.Float, nullable=False)
    humidity = sa.Column(sa.Float, nullable=False)

    def to_dict(self):
        return {
            'id': float(self.id),
            'city': self.city,
            'date': pd.to_datetime(self.date),
            'temp': float(self.temp),
            'humidity': float(self.humidity)
        }

    def __init__(self, city: sa.String, date: datetime.datetime, temp: sa.Float, humidity: sa.Integer):
        super().__init__(city=city, date=date, temp=temp, humidity=humidity)


async def create_models():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# asyncio.run(create_models())
