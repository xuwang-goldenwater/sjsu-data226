from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

@task
def forecast_and_union():
        """
        Use Snowflake ML to train on 60 days of history and generate a 7-day forecast.
        """
        
        # 1. Retrieve table names from Airflow Variables
        source_table = Variable.get("weather_target_table", default_var="USER_DB_FOX.raw.weather_historical")
        forecast_result_table = "USER_DB_FOX.raw.weather_forecast_results"
        final_report_table = "USER_DB_FOX.raw.weather_final_report"
        
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            logging.info("Starting ML Training and Union operation...")
            cur.execute("BEGIN;")

            # 2. Train the ML Forecast model
            # SERIES_COLNAME => 'LOCATION_NAME' enables multi-city forecasting
            cur.execute(f"""
                CREATE OR REPLACE SNOWFLAKE.ML.FORECAST weather_prediction_model (
                    INPUT_DATA => TABLE({source_table}),
                    SERIES_COLNAME => 'LOCATION_NAME',
                    TIMESTAMP_COLNAME => 'DATE',
                    TARGET_COLNAME => 'TEMP_MAX'
                );
            """)

            # 3. Generate predictions for the next 7 days
            cur.execute(f"""
                CREATE OR REPLACE TABLE {forecast_result_table} AS 
                SELECT * FROM TABLE(weather_prediction_model!FORECAST(FORECASTING_PERIODS => 7));
            """)

            # 4. Perform UNION ALL operation to combine actual historical data with forecasted data
            # Combine the last 14 days of actual data with 7 days of forecasted data
            cur.execute(f"""
                CREATE OR REPLACE TABLE {final_report_table} AS
                -- Actual historical data
                SELECT 
                    LOCATION_NAME, 
                    DATE, 
                    TEMP_MAX, 
                    'ACTUAL' as DATA_TYPE 
                FROM {source_table}
                WHERE DATE >= DATEADD('day', -14, CURRENT_DATE())
                
                UNION ALL
                
                -- Forecasted data
                SELECT 
                    SERIES AS LOCATION_NAME, 
                    TS AS DATE, 
                    FORECAST AS TEMP_MAX, 
                    'FORECAST' as DATA_TYPE 
                FROM {forecast_result_table};
            """)

            cur.execute("COMMIT;")
            logging.info(f"ML Pipeline Success: {final_report_table} has been updated.")

        except Exception as e:
            if conn and cur:
                cur.execute("ROLLBACK;")
            logging.error(f"ML Pipeline failed: {str(e)}")
            raise e
        finally:
            if cur: cur.close()
            if conn: conn.close()

with DAG(
    dag_id = 'Lab1_ML_Forecasting',
    start_date = datetime(2026, 2, 28),
    catchup = False,
    tags = ['Lab1', 'ML_Forecast'],
    schedule = '45 2 * * *'  # Scheduled 15 minutes after the ETL DAG
) as dag:
 
    # Define the task flow
 forecast_and_union()