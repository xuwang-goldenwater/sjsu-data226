from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import pandas as pd
import logging
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



@task
def extract():
    url = Variable.get("weather_forecast_url", default_var="https://api.open-meteo.com/v1/forecast")
    # on Airflow UI variable setting weather_locations as: 
    # {"San_Jose": [37.33, -121.88], "Cupertino": [37.32, -122.03]}
    locations = Variable.get("weather_locations", deserialize_json=True, 
                             default_var={"San_Jose": [37.33, -121.88], "Cupertino": [37.32, -122.03]})

    all_responses = []

    for city_name, coords in locations.items():
        params = {
            "latitude": coords[0],
            "longitude": coords[1],
            "past_days": 60, # Lab 1 requires historical data, so we set past_days to 60 to get 2 months of data
            "forecast_days": 0,
            "daily": ["temperature_2m_max", "temperature_2m_min","temperature_2m_mean", "precipitation_sum", "weather_code"],
            "timezone": "America/Los_Angeles"
        }
        logging.info(f"Fetching data for {city_name}...")
        response = requests.get(url, params=params)
        response.raise_for_status()

        # data saving includes city name for later Transform
        res_json = response.json()
        res_json['location_name'] = city_name
        all_responses.append(res_json)
        
    return all_responses

@task
def transform(all_data_json):
    #deal with multiple cities
    combined_df = pd.DataFrame()
    
    for data in all_data_json:
        df = pd.DataFrame({
            "location_name": data["location_name"], # add location_name to the DataFrame for later Load
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "date": data["daily"]["time"],
            "temp_max": data["daily"]["temperature_2m_max"],
            "temp_min": data["daily"]["temperature_2m_min"],
            "temp_mean": data["daily"]["temperature_2m_mean"],
            "precipitation": data["daily"]["precipitation_sum"],
            "weather_code": data["daily"]["weather_code"]
        })
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    
    combined_df["date"] = pd.to_datetime(combined_df["date"]).dt.date
    return combined_df.to_dict(orient='records')

@task
def load_V2(records):
    if not records:
        print("No records to load.")
        return
  
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    target_table = Variable.get("weather_target_table", default_var="USER_DB_FOX.raw.weather_historical")
    df = pd.DataFrame(records)
    
    conn = None
    cur = None

    try:
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("BEGIN;")
        # location_name + date as composite primary key to avoid duplicates when loading multiple cities
        cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {target_table} (
                  location_name STRING,
                  latitude FLOAT,
                  longitude FLOAT,
                  date DATE,
                  temp_max FLOAT,
                  temp_min FLOAT,
                  temp_mean FLOAT,
                  precipitation FLOAT,
                  weather_code INT,
                  PRIMARY KEY (location_name, date)
                )""")
        cur.execute(f"DELETE FROM {target_table}")
        # DataFrame convert to list of tuples for executemany
        rows = [tuple(x) for x in df[['location_name', 'latitude', 'longitude', 'date', 
                                      'temp_max', 'temp_min', 'temp_mean', 'precipitation', 'weather_code']].values]
        sql = f"""
                INSERT INTO {target_table}
                (location_name, latitude, longitude, date, temp_max, temp_min, temp_mean, precipitation, weather_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
        cur.executemany(sql, rows)
        cur.execute("COMMIT;")
        print(f"Successfully loaded {len(rows)} rows for multiple locations!")
    except Exception as e:
        if conn and cur:
            cur.execute("ROLLBACK;")
        raise e
    finally:
        if cur: cur.close()
        if conn: conn.close()


with DAG(
    dag_id = 'ETL_Weather',
    start_date = datetime(2026,2,28),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
   
   weather_raw_data = extract()
   transformed_data = transform(weather_raw_data)
   load_task = load_V2(transformed_data)

trigger_dbt = TriggerDagRunOperator(
    task_id="trigger_dbt_dag",
    trigger_dag_id="Lab2_dbt_Workflow",
)

load_task >> trigger_dbt