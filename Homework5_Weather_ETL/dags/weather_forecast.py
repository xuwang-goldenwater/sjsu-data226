from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import pandas as pd

@task
def extract():
    url = Variable.get("weather_forecast_url", default_var="https://api.open-meteo.com/v1/forecast")
    lat = Variable.get("LATITUDE", default_var=37.3852)
    lon = Variable.get("LONGITUDE", default_var=-122.1141)

    params = {
        "latitude": lat,
        "longitude": lon,
        "past_days": 60,
        "forecast_days": 0,
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "weather_code"],
        "timezone": "America/Los_Angeles"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

@task
def transform(data):
    df = pd.DataFrame({
        "latitude": data["latitude"],
        "longitude": data["longitude"],
        "date": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"],
        "weather_code": data["daily"]["weather_code"]
    })
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df.to_dict(orient='records')

@task
def load_V2(records):
    
    if not records:
        print("No records to load.")
        return
  
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    target_table = Variable.get("weather_target_table", default_var="USER_DB_FOX.raw.weather_los_altos")
    df = pd.DataFrame(records)
    
    conn = None
    cur = None

    try:
        print("Connecting to Snowflake...")
        conn = hook.get_conn()
        cur = conn.cursor()
        print("Successfully connected to Snowflake.")
      
        cur.execute("BEGIN;")
        cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {target_table} (
                  latitude FLOAT,
                  longitude FLOAT,
                  date DATE,
                  temp_max FLOAT,
                  temp_min FLOAT,
                  precipitation FLOAT,
                  weather_code INT,
                  PRIMARY KEY (latitude, longitude, date)
                )""")
        
        cur.execute(f"DELETE FROM {target_table}")
    
        rows = [tuple(x) for x in df.values]
        sql = f"""
                INSERT INTO {target_table}
                (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
        cur.executemany(sql, rows)
        cur.execute("COMMIT;")
        print(f"Successfully loaded {len(rows)} rows to Snowflake!")

    except Exception as e:
        if conn and cur:
            cur.execute("ROLLBACK;")
        print(f"Load failed: {str(e)}")
        raise e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


with DAG(
    dag_id = 'Weather_forecast_v2',
    start_date = datetime(2026,2,28),
    catchup=False,
    tags=['ETL_weather'],
    schedule = '30 2 * * *'
) as dag:
   
   weather_raw_data = extract()
   transformed_data = transform(weather_raw_data)
   load_V2(transformed_data)