import requests

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook


def test_conn_function():
    pg_hook = PostgresHook(postgres_conn_id="airflow-postgres")
    records = pg_hook.get_records("SELECT * FROM hourly_forecast")
    print(records)

    
def create_table():
    request = """
        CREATE TABLE IF NOT EXISTS hourly_forecast(
            request_time  TIMESTAMP,
            forecast_time  TIMESTAMP,
            description  VARCHAR(100),
            temperature  NUMERIC,
            pressure  INT,
            humidity  INT,
            wind_speed  NUMERIC
        ) 
    """
    pg_hook = PostgresHook(postgres_conn_id="airflow-postgres")
    pg_hook.run(request)
    
    
def openweathermap_data():
    API_key = "217af0bd393daed4324dd4cc05636799"
    city_name = "Rio de Janeiro"

    url = f"http://api.openweathermap.org/data/2.5/forecast?q={city_name}&appid={API_key}"
    request_time = datetime.now()
    api_response = requests.get(url).json()

    forecast_time = api_response["list"][0]["dt_txt"]
    description = api_response["list"][0]["weather"][0]["description"]
    temperature = api_response["list"][0]["main"]["temp"]
    pressure = api_response["list"][0]["main"]["pressure"]
    humidity = api_response["list"][0]["main"]["humidity"]
    wind_speed = api_response["list"][0]["wind"]["speed"]
    print([request_time, forecast_time, description, temperature, pressure, humidity, wind_speed])

    sql_request = f"""
        INSERT INTO hourly_forecast
        VALUES ('{request_time}', '{forecast_time}', '{description}', '{temperature}', '{pressure}', '{humidity}', '{wind_speed}'); 
    """
    pg_hook = PostgresHook(postgres_conn_id="airflow-postgres")
    pg_hook.run(sql_request)



