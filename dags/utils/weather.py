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



class WeatherProcessor:
    """
    Class for transforming raw weather data
    """
    def __init__(self):
        self.raw_data = None
        self.logger = logging.getLogger(__name__)

    def load_raw_data(self):
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()
        select_query = f"""
                SELECT {RAW_WEATHER_TABLE}.city_id, raw_data
                FROM {RAW_WEATHER_TABLE}
                INNER JOIN (
                    SELECT city_id, MAX(timestamp) AS max_timestamp
                    FROM {RAW_WEATHER_TABLE}
                    GROUP BY city_id
                ) AS latest
                ON {RAW_WEATHER_TABLE}.city_id = latest.city_id AND {RAW_WEATHER_TABLE}.timestamp = latest.max_timestamp;
                """
        try:
            cursor.execute(select_query)
            rows = cursor.fetchall()
        except (Exception, ) as e:
            self.logger.error(f"Failed to retrieve last raw data. Reason: {e}")
            rows = []


        raw_data = [{'city_id': row[0], **json.loads(row[1])} for row in rows]
        conn.close()
        return raw_data


    @staticmethod
    def clean_weather_data(weather_data: dict) -> dict:
        timestamp = weather_data['dt']
        date_time = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')
        temp = weather_data['main']['temp']
        weather_main = weather_data['weather'][0]['main']
        weather_desc = weather_data['weather'][0]['description']
        wind_speed = weather_data['wind']['speed']  # m/s
        wind_degree = weather_data['wind']['deg']
        cloudiness = weather_data['clouds']['all']
        rain = weather_data.get('rain', {}).get('1h')  # mm/h
        snow = weather_data.get('snow', {}).get('1h')  # mm/h

        return {
            'timestamp': timestamp,
            'datetime': date_time,
            'temperature': temp,
            'weather_main': weather_main,
            'weather_description': weather_desc,
            'wind_speed': wind_speed,
            'wind_degree': wind_degree,
            'cloudiness': cloudiness,
            'rain': rain,
            'snow': snow
        }

    def execute(self):
        if self.raw_data is None:
            self.raw_data = self.load_raw_data()

        for raw_weather in self.raw_data:
            city_id = raw_weather['city_id']
            transformed_data = self.clean_weather_data(weather_data=raw_weather)
            store_baseline_weather_data(city_id=city_id, cleaned_data=transformed_data)

