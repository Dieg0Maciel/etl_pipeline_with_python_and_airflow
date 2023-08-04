import requests

from datetime import datetime
        
def run_etl(ti):
    API_key = "insert_API_key_here"
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

    ti.xcom_push(key='request_time', value=f"{request_time}")
    ti.xcom_push(key='forecast_time', value=forecast_time)
    ti.xcom_push(key='description', value=description)
    ti.xcom_push(key='temperature', value=temperature)
    ti.xcom_push(key='pressure', value=pressure)
    ti.xcom_push(key='humidity', value=humidity)
    ti.xcom_push(key='wind_speed', value=wind_speed)
