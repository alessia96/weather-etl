# weather-etl

## Weather Data 

The following project allows to ingest and transform weather data from 
openweathermap API.

It is based on python, docker and apache airflow.

The data pipeline runs hourly and collects data for the cities of Milan, Naples, Rome, and Turin as default.

###
### Prerequisites
In order to run the project you need to:
* Install docker;
* Create a .env file starting from example.env and replacing the placeholder in WEATHER_API_KEY with your
  openweathermap API key
* [Optional] Add and/or remove cities from `COMMASEP_CITIES` in the .env file
* [Optional] Change `WEATHER_PIPELINE_CRON` to set a different schedule


###
### Build and run locally
In order to build and run the docker you should run:

    docker compose build
    docker compose up

Once your docker components are running:
1. open your browser and navigate to http://0.0.0.0:8080/;
2. Enter the credentials specified in the .env file. If you did not change them,
the username and password are both `airflow`;
3. Start the `init_pipeline` DAG (it will run only once);
4. Start the `weather_pipeline` to collect weather data (it will run hourly by default)


The `weather_pipeline` first collects raw weather data, and then transform it 
into a baseline (or cleaned) data.


