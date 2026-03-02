from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

@task
def forecast_and_union():
    """
    Orchestrates the ML lifecycle: Training on 60-day historical data, 
    generating a 7-day forecast, and consolidating results into a unified report.
    """
    
    # 1. Configuration: Retrieve table and model identifiers from Airflow Variables or Defaults
    source_table = Variable.get("weather_target_table", default_var="USER_DB_FOX.raw.weather_historical")
    model_name = "USER_DB_FOX.raw.weather_prediction_model"
    final_report_table = "USER_DB_FOX.raw.weather_final_report"
    
    # Initialize Snowflake connection via Airflow Hook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        logging.info("Initiating Snowflake Cortex ML Training Phase...")
        
        # Start explicit transaction to ensure data atomicity
        cur.execute("BEGIN;")

        # 2. Model Training: Create/Replace the forecasting model using the last 60 days of data
        # Filtering the input data ensures the model focuses on recent seasonal trends
        cur.execute(f"""
            CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model_name} (
                INPUT_DATA => TABLE(
                    SELECT LOCATION_NAME, DATE, TEMP_MAX 
                    FROM {source_table} 
                    WHERE DATE >= DATEADD('day', -60, CURRENT_DATE())
                ),
                SERIES_COLNAME => 'LOCATION_NAME',
                TIMESTAMP_COLNAME => 'DATE',
                TARGET_COLNAME => 'TEMP_MAX'
            );
        """)

     # 3. Inference & 4. Data Integration (Unified Operation)
        # We use a CTE approach to ensure clean data flow and explicit column mapping
        logging.info("Generating predictions and consolidating final report...")
        
        cur.execute(f"""
            CREATE OR REPLACE TABLE {final_report_table} AS
            WITH forecast_data AS (
                -- Call the model and immediately rename columns for consistency
                SELECT 
                    SERIES AS LOCATION_NAME, 
                    TS AS DATE, 
                    FORECAST AS TEMP_MAX,
                    'FORECAST' AS DATA_TYPE
                FROM TABLE({model_name}!FORECAST(FORECASTING_PERIODS => 7))
            ),
            historical_data AS (
                -- Filter 60 days of ground truth data
                SELECT 
                    LOCATION_NAME, 
                    DATE, 
                    TEMP_MAX, 
                    'ACTUAL' AS DATA_TYPE
                FROM {source_table}
                WHERE DATE >= DATEADD('day', -60, CURRENT_DATE())
            )
            -- Combine both datasets into the final analytical view
            SELECT * FROM historical_data
            UNION ALL
            SELECT * FROM forecast_data;
        """)

        # Commit all changes upon successful execution
        cur.execute("COMMIT;")
        logging.info(f"Pipeline Execution Successful: {final_report_table} is now synchronized.")

    except Exception as e:
        # Rollback in case of failure to maintain database consistency
        cur.execute("ROLLBACK;")
        logging.error(f"Critical error during ML Pipeline execution: {str(e)}")
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