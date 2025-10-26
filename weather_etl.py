from airflow.sdk import dag, task
import pandas as pd
import time
from datetime import datetime, timedelta
import os
import requests
from airflow.models import Variable

OPEN_WEATHER_API = Variable.get("OPEN_WEATHER_API")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
TMP_DIR = os.path.join(AIRFLOW_HOME, "tmp")
TRANSFORMED_DIR = os.path.join(AIRFLOW_HOME, "transformed")
os.makedirs(TMP_DIR,exist_ok=True)
os.makedirs(TRANSFORMED_DIR,exist_ok=True)
CITY="Trento"

@task()
def extract(city=CITY):
    cities_file = "cities.txt"
    cities_cache = {}
    print(city)
    if os.path.exists(cities_file):
        with open(cities_file, "r") as f:
            for line in f:
                parts = line.strip().split(",")
                if len(parts) == 3:
                    name, lat, lon = parts
                    cities_cache[name.lower()] = (float(lat), float(lon))
    
    if city.lower() in cities_cache:
        lat, lon = cities_cache[city.lower()]
    else:

        geo_url=f'https://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={OPEN_WEATHER_API}'
        geo_data=requests.get(geo_url).json()
        lat, lon= geo_data[0]['lat'],geo_data[0]["lon"]
        with open(cities_file, "a") as f:
            f.write(f"{city},{lat},{lon}\n")
    
    print(lon,lat)
    weather_url=f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPEN_WEATHER_API}&units=metric"
    data=requests.get(weather_url).json()
    pd.DataFrame([data]).to_json(os.path.join(TMP_DIR, "weather_raw.json"), orient="records", lines=True)



 

@task()
def transform(city=CITY):

    df = pd.read_json(os.path.join(TMP_DIR, "weather_raw.json"), lines=True)
    main=df.weather[0][0]["main"]
    description=df.weather[0][0]["description"]
    datetime=pd.to_datetime(df.dt,unit="s")[0]
    features=df.main[0]
    features["main"]=main
    features["datetime"]=datetime
    features["description"]=description
    columns=["datetime","main","description","temp","feels_like","temp_min","temp_max","humidity","pressure","sea_level","grnd_level"]
    transformed_row=pd.DataFrame([features],columns=columns)
    return transformed_row


@task()
def load(transformed_row, city=CITY):
    city_csv = os.path.join(TRANSFORMED_DIR, f"{city}.csv")

    if os.path.exists(city_csv):
        existing_df = pd.read_csv(city_csv)
        updated_df = pd.concat([existing_df, transformed_row], ignore_index=True)
        updated_df.to_csv(city_csv, index=False)
    else:
        transformed_row.to_csv(city_csv, index=False)


@dag(
    dag_id="weather_etl",
    schedule=timedelta(hours=1),
    start_date=datetime(2025, 10, 21),
    catchup=False,
    description="Hourly ETL pipeline for OpenWeather API data",
    tags=["weather","etl"]
)
def my_etl():
 extract_task=extract()
 tr_task=transform()
 load_task=load(tr_task)
 extract_task >> tr_task >> load_task

my_etl()

