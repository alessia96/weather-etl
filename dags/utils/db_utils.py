from dotenv import load_dotenv
import logging
import os
import psycopg2


logger = logging.getLogger(__name__)

load_dotenv()

POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_DB')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')

conn_str = f"dbname='{POSTGRES_DB}' user='{POSTGRES_USER}' password='{POSTGRES_PASSWORD}' host='{POSTGRES_HOST}' port='{POSTGRES_PORT}'"

BASELINE_WEATHER_TABLE = os.getenv('BASELINE_WEATHER_TABLE', 'baseline_weather')
RAW_WEATHER_TABLE = os.getenv('RAW_WEATHER_TABLE', 'raw_weather')

def create_raw_weather_table() -> None:
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {RAW_WEATHER_TABLE} (
        id SERIAL PRIMARY KEY,
        city_id INT,
        timestamp INT,
        raw_data TEXT
    );
    """

    cursor.execute(create_table_query)
    conn.commit()
    conn.close()


def create_baseline_weather_table() -> None:
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {BASELINE_WEATHER_TABLE} (
            id SERIAL PRIMARY KEY,
            timestamp INT NOT NULL,
            datetime TIMESTAMP NOT NULL,
            city_id INT NOT NULL,
            temperature FLOAT,
            weather_main TEXT,
            weather_description TEXT,
            wind_speed FLOAT,
            wind_degree INT,
            cloudiness FLOAT,
            rain FLOAT,
            snow FLOAT
        );
        """
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()


def store_baseline_weather_data(cleaned_data: dict, city_id: int) -> None:
    logger.info("Storing baseline weather data")
    query = f"""
    INSERT INTO {BASELINE_WEATHER_TABLE} (
        city_id, timestamp, datetime, temperature, weather_main, weather_description, wind_speed, 
        wind_degree, cloudiness, rain, snow
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()
    data = (city_id, *cleaned_data.values())
    cursor.execute(query, data)
    conn.commit()


if __name__ == '__main__':
    create_raw_weather_table()
    create_baseline_weather_table()