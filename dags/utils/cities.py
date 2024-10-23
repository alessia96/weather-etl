from dotenv import load_dotenv
import gzip
import io
import json
import logging
import os
import psycopg2
import requests

logger = logging.getLogger(__name__)

load_dotenv()
CITIES_TABLE = os.getenv('CITIES_TABLE')

POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_DB')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')

conn_str = f"dbname='{POSTGRES_DB}' user='{POSTGRES_USER}' password='{POSTGRES_PASSWORD}' host='{POSTGRES_HOST}' port='{POSTGRES_PORT}'"


def get_city_info(url: str):
    response = requests.get(url)
    response.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
        data = json.load(f)
    return data


def store_city_info(data: list):
    conn = psycopg2.connect(conn_str)
    logger.info("Storing cities data")
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {CITIES_TABLE} (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            state TEXT,
            country TEXT,
            lon REAL NOT NULL,
            lat REAL NOT NULL
        );
    ''')

    query = f'''
    INSERT INTO {CITIES_TABLE} (id, name, state, country, lon, lat) 
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    '''
    for city in data:
        params = (
            city['id'], city['name'], city.get('state'), city.get('country'), city['coord']['lon'], city['coord']['lat']
        )
        cursor.execute(query, params)
    logger.info("WROTE CITY DATA INTO DB")
    conn.commit()
    conn.close()


def load_cities(cities_list: list) -> list:
    out = list()
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    for city in cities_list:
        cursor.execute(f'''
            SELECT id, lon, lat 
            FROM {CITIES_TABLE} 
            WHERE name = %s;
        ''', (city,))

        row = cursor.fetchone()
        if row:
            city_data = {
                'id': row[0],
                'lon': row[1],
                'lat': row[2]
            }
            out.append(city_data)
        else:
            logger.warning(f'City {city} not found.')

    conn.close()
    return out


if __name__ == "__main__":
    cities_data = get_city_info(url='http://bulk.openweathermap.org/sample/city.list.json.gz')
    store_city_info(data=cities_data)