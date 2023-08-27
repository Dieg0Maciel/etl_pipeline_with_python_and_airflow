# ETL PIPELINE WITH PYTHON AND AIRFLOW
## Table of Contents
* [Overview](#overview)
* [Requirements](#requirements)
* [Configure Docker-Compose](#configure-docker-compose)
* [Airflow Connection To PostgresQL](#airflow-connection-to-postgresql)
* [Openweathermap API](#openweathermap-api)
* [Airflow DAGs](#airflow-dags)
* [Resources](#resources)

## Overview
In this project we are going to build an ETL pipeline (*extract, transform and load*) on premise with Airflow running in a Docker container, where each task is organized and automated by Airflow DAGs. Data extraction is performed using the API provided by [https://openweathermap.org/](https://openweathermap.org/). This data will be transformed with Python and loaded to a PostgresQL database.

### Architecture Diagram
![](/images/pipeline.png)

[Table of Contents]()

## Requirements
* An account in [https://openweathermap.org/](https://openweathermap.org/) to have access to the Openweathermap API

* The following software should be installed:
    - Python, Pandas
    - PostgresQL
    - Docker, docker-coompose

[Table of Contents]()
    
## Configure Docker-Compose
The detailed documentation used to configure docker-compose cand be found
[here](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).
We start creating a project directory named *ETLProject*. Afterward we follow this steps:

1. Open a terminal inside the project directory and execute
    ```
    mkdir -p ./dags ./logs ./plugins ./config ./sql
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```
    
2. Download the *docker-compose.yaml* file from [this link](https://airflow.apache.org/docs/apache-airflow/2.6.3/docker-compose.yaml) and save it into the project directory.


3. Make the following changes to the *docker-compose.yaml* file:

    * Change `AIRFLOW__CORE__EXECUTOR: 'CeleryExecutor'` to

        ```
        AIRFLOW__CORE__EXECUTOR: 'LocalExecutor'
        ```
    
    * In *volumes* add `` ${AIRFLOW_PROJ_DIR:-.}/sql:/opt/airflow/sql ``
        ```
        volumes:
            - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
            - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
            - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
            - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
            - ${AIRFLOW_PROJ_DIR:-.}/sql:/opt/airflow/sql
        ```

    * Delete:
        - ```
          AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
          AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
          ```
        
        - ``` 
          redis:
          condition: service_healthy
          ```

        - ```
          redis:
            image: redis:latest
            expose:
            - 6379
            healthcheck:
            test: ["CMD", "redis-cli", "ping"]
            interval: 10s
            timeout: 30s
            retries: 50
            start_period: 30s
            restart: always
          ```

        - ``` 
            airflow-worker:
                <<: *airflow-common
                command: celery worker
                healthcheck:
                test:
                    - "CMD-SHELL"
                    - 'celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"'
                interval: 30s
                timeout: 10s
                retries: 5
                start_period: 30s
                environment:
                <<: *airflow-common-env
                # Required to handle warm shutdown of the celery workers properly
                # See https://airflow.apache.org/docs/docker-stack/entrypoint.html#signal-propagation
                DUMB_INIT_SETSID: "0"
                restart: always
                depends_on:
                <<: *airflow-common-depends-on
                airflow-init:
                    condition: service_completed_successfully
          ```
        - ``` 
            flower:
                <<: *airflow-common
                command: celery flower
                profiles:
                - flower
                ports:
                - "5555:5555"
                healthcheck:
                test: ["CMD", "curl", "--fail", "http://localhost:5555/"]
                interval: 30s
                timeout: 10s
                retries: 5
                start_period: 30s
                restart: always
                depends_on:
                <<: *airflow-common-depends-on
                airflow-init:
                    condition: service_completed_successfully
          ```

    * To remove the example dags change `AIRFLOW__CORE__LOAD_EXAMPLES: 'true'` to
            ```
            AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
            ```
    * In order to build the postgres image inside docker with port 5432 add
        ```
        ports:
        - 5432:5432
        ```
        ![](/images/yaml.png)
    
    
    
4. Open a terminal inside the project directory 
    * To stop PostgresQL execute 
        ```
        service postgresql stop
        ```
    * To run database migrations and create the user account execute
        ```
        docker-compose up airflow-init
        ```

    * To start all services and run them on the background
        ```
        docker-compose up -d
        ```    
   
[Table of Contents]()


## Airflow Connection To PostgresQL
First we need to create a connection to the *postgres* server with the user *airflow*, it can be done on the terminal with psql as well as using a ProstgresQL editor like pgAdmin or DBeaver. 
With DBeaver we can select *Database --> New Database Connection*

![](/images/newdata.png)

which will pop up the *Connect to a Database* window, choose *PostgresQL* and click *Next*

![](/images/connectdata.png)

use the username _airflow_ with password _airflow_ and click *finish*

![](/images/finishconnect.png)

Connect to the *postgres* database with the user *airflow*, select *Databases --> Create New Database* to create a database named *openweathermap* with owner *airflow*

![](/images/createdata.png)

Open the Airflow dashboard at [localhost:8080](http://localhost:8080) in a browser and log in with
```
user:      airflow
password:  airflow
```

Select *Admin --> Connections*

![](/images/conn1.png)

Add a new connection with the following parameters

parameter | value
--- | ---
**Connection Id** | *airflow-postgres*
**Connection Type** | Postgres
**Host** | *172.17.0.1* (**Linux**) or *host.docker.internal* (**Windows, Mac**)
**Schema** | openweathermap
**Login** | airflow
**Password** | airflow

After completing all the fields click *Test*, if the test is succesful save the connection

![](/images/addconn.png)

[Table of Contents]()

## Openweathermap API
By subscribing to the weather forecast service [OpenWeather](https://openweathermap.org/) we get acces to their [API](https://openweathermap.org/api). The [free tier](https://openweathermap.org/price#weather) subscription gave us access to the [Call 5 day / 3 hour forecast data service](https://openweathermap.org/forecast5). With the ``API_key`` provided by OpenWeather we can make an API call

```
http://api.openweathermap.org/data/2.5/forecast?q={city_name}&appid={API_key}
```

Using the ``requests`` python module we can get a JSON object from the API call
```
import requests

api_response = requests.get(
    "http://api.openweathermap.org/data/2.5/forecast?q={city_name}&appid={API_key}"
).json()
```
where all the fields in this response are described [here](https://openweathermap.org/forecast5#JSON). From the JSON object ``forecast`` we will collect the following data

```
forecast_time = api_response["list"][0]["dt_txt"]
description = api_response["list"][0]["weather"][0]["description"]
temperature = api_response["list"][0]["main"]["temp"]
pressure = api_response["list"][0]["main"]["pressure"]
humidity = api_response["list"][0]["main"]["humidity"]
wind_speed = api_response["list"][0]["wind"]["speed"]
```

[Table of Contents]()

## Airflow DAGs
### DAG: 'dag_hourly_forecast'

This is a DAG with an hourly schedule interval given by the [cron expression](https://crontab.cronhub.io/) ``'0 * * * *'`` . This DAG contains 2 tasks in a downstream:

```
with DAG(
    dag_id='dag_etl',
    default_args=default_args,
    start_date=datetime(2023, 8, 2),
    schedule_interval='0 * * * *',
    catchup=False
) as dag:
    task_create = PythonOperator(task_id='create_table', ...)
    task_openweather_ = PythonOperator(task_id='openweather_record', ...)
```

![](/images/dag_hourly_forecast.png)

#### First task: 'create_table'
Using *PostgresOperator* the first task creates the *hourly_forecast* table inside the *openweathermap* dataset, the execultable ``create _table`` runs sql code with a postgres hook
```
def create_table():
    request = """
        CREATE TABLE IF NOT EXISTS hourly_forecast(
            request_time  VARCHAR(50),
            forecast_time  VARCHAR(25),
            description  VARCHAR(100),
            temperature  NUMERIC,
            pressure  INT,
            humidity  INT,
            wind_speed  NUMERIC
        ) 
    """
    pg_hook = PostgresHook(postgres_conn_id="airflow-postgres")
    pg_hook.run(request)
```

#### Second task: 'openweather_record'
Using *PostgresOperator* the execultable ``openweathermap_data`` performs an API request to [OpenWeather](https://openweathermap.org/) and
runs sql code to insert the data as a record in a table with a postgres hook

```
def openweathermap_data():
    API_key = "API_key goes here"
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
```


[Table of Contents]()


## Resources
These are links to some useful learning resources:
* [Beginner Airflow Tutorial](https://www.youtube.com/watch?v=K9AnJ9_ZAXE)
* [OpenWeather API Tutorial in Python](https://www.youtube.com/watch?v=9P5MY_2i7K8)

[Table of Contents]()

 
