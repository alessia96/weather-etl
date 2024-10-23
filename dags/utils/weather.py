import datetime
from dotenv import load_dotenv
import json
import logging
import os
import requests
import psycopg2
from typing import Optional

from utils.cities import load_cities
from utils.db_utils import store_baseline_weather_data


load_dotenv()
APIKEY = os.getenv('WEATHER_API_KEY')
CITIES_TABLE = os.getenv('CITIES_TABLE')

POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_DB')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
conn_str = f"dbname='{POSTGRES_DB}' user='{POSTGRES_USER}' password='{POSTGRES_PASSWORD}' host='{POSTGRES_HOST}' port='{POSTGRES_PORT}'"

OPENWEATHER_URL = os.getenv(
    'OPENWEATHER_URL', 'https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}&units=metric'
)
RAW_WEATHER_TABLE = os.getenv('RAW_WEATHER_TABLE', 'raw_weather')


class WeatherDataIngestor:
    """
    Class for ingesting weather data from openweathermap
    """
    def __init__(self):
        self.cities_list = os.getenv('COMMASEP_CITIES').split(',')
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def get_weather_data(lat: float, lon: float) -> Optional[dict]:
        response = requests.get(OPENWEATHER_URL.format(lat, lon, APIKEY))
        if response.status_code != 200:
            logging.warning(f'Failed to retrieve weather data for {lat},{lon}. Reason: {response.content}')
            return
        content = response.json()
        return content

    def execute(self):
        timestamp = datetime.datetime.now().timestamp()
        cities_data = load_cities(cities_list=self.cities_list)
        rows = []
        for city in cities_data:
            raw_weather_data = self.get_weather_data(lat=city['lat'], lon=city['lon'])
            tmp = (city['id'], timestamp, json.dumps(raw_weather_data))
            rows.append(tmp)

        self.logger.info("Storing baseline weather data")
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()

        insert_query = f"""
        INSERT INTO {RAW_WEATHER_TABLE} (city_id, timestamp, raw_data)
        VALUES (%s, %s, %s);
        """
        cursor.executemany(insert_query, rows)
        conn.commit()
        conn.close()

