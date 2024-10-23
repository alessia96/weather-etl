FROM apache/airflow:2.10.2
COPY requirements.txt .
COPY .env .
RUN pip install -r requirements.txt