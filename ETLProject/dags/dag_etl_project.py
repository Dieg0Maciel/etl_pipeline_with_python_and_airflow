import numpy
import requests

from etl_file import run_etl
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator



default_args = {
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='dag_etl_version_01',
    default_args=default_args,
    start_date=datetime(2023, 8, 2),
    schedule_interval='0 * * * *',
    max_active_tasks=1,
    catchup=False,
    template_searchpath='/opt/airflow/sql'
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='airflow-postgres',
        sql='create_postgres_table.sql'
    )
    
    task2 = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl
    )
    
    task3 = PostgresOperator(
        task_id='insert_record',
        postgres_conn_id='airflow-postgres',
        sql="""INSERT INTO hourly_forecast 
VALUES 
    ('{{ti.xcom_pull(task_ids="run_etl", key="request_time")}}', '{{ti.xcom_pull(task_ids="run_etl", key="forecast_time")}}', '{{ti.xcom_pull(task_ids="run_etl", key="description")}}', '{{ti.xcom_pull(task_ids="run_etl", key="temperature")}}', '{{ti.xcom_pull(task_ids="run_etl", key="pressure")}}', '{{ti.xcom_pull(task_ids="run_etl", key="humidity")}}', '{{ti.xcom_pull(task_ids="run_etl", key="wind_speed")}}')
""" 
    )
    
    task1 >> task2 >> task3

