import requests
import etl_functions

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}



with DAG(
    dag_id='dag_hourly_forecast_v02',
    default_args=default_args,
    start_date=datetime(2023, 8, 17),
    schedule_interval='0 * * * *',
    catchup=False
) as dag:
    task_create = PythonOperator(
        task_id='create_table', 
        python_callable=etl_functions.create_table
    )
    
    task_openweather = PythonOperator(
        task_id='openweather_record', 
        python_callable=etl_functions.openweathermap_data
    )

    task_create >> task_openweather
