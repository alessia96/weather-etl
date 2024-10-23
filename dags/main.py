from airflow.models.dag import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from dotenv import load_dotenv
import os

from utils.cities import get_city_info, store_city_info, load_cities
from utils.db_utils import create_baseline_weather_table, create_raw_weather_table
from utils.weather import WeatherDataIngestor, WeatherProcessor


load_dotenv()

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.now() - timedelta(minutes=5),
    'catchup': False
}

@task
def get_city_info_task():
    city_info_url = os.getenv('CITY_INFO_URL')
    if not city_info_url:
        raise Exception('Missing cities info url. Check openweathermap for the bulk download url.')
    return get_city_info(url=city_info_url)

@task
def store_city_info_task(data: list):
    store_city_info(data=data)

@task_group
def init_cities():
    cities_data = get_city_info_task()
    store_city_info_task(data=cities_data)


@task
def init_weather():
    create_raw_weather_table()
    create_baseline_weather_table()


def init_pipeline() -> DAG:
    dag = DAG(
        dag_id='init_dag',
        schedule='@once',
        default_args=default_args
    )
    with dag:
        init_cities() >> init_weather()
    return dag


@task
def get_cities_task(cities_list: list) -> list:
    cities_data = load_cities(cities_list=cities_list)
    return cities_data

def weather_pipeline() -> DAG:
    dag = DAG(
        dag_id='weather_pipeline',
        schedule=os.getenv("WEATHER_PIPELINE_CRON", '0 * * * *'),
        default_args=default_args
    )
    with dag:
        ingest_task = PythonOperator(
            task_id='ingest_weather_data',
            python_callable=WeatherDataIngestor().execute,
            dag=dag
        )

        baseline_task = PythonOperator(
            task_id='transform_weather_data',
            python_callable=WeatherProcessor().execute,
            dag=dag
        )

        ingest_task >> baseline_task

    return dag


init_pipeline()
weather_pipeline()
