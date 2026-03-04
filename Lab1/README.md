# Lab 1: End-to-End Weather Forecasting & ML Pipeline

## Project Description
An advanced data engineering pipeline that extends basic ETL capabilities with Machine Learning. This project automates the entire lifecycle from raw data ingestion to predictive analytics and unified reporting.

## Key Features
* **Multi-Location Scalability**: Dynamically processes multiple geographic coordinates (e.g., San Jose, Cupertino) using Airflow Variables and Series-based mapping.
* **Snowflake Cortex ML**: Leverages the `SNOWFLAKE.ML.FORECAST` engine to generate 7-day temperature predictions directly inside Snowflake.
* **Unified Reporting**: Automatically generates a `WEATHER_FINAL_REPORT` table by joining 14 days of historical "Actual" data with 7 days of "Forecast" data.
* **TaskFlow API**: Uses modern Airflow `@task` decorators for efficient XCom data passing and improved DAG readability.

## Directory Structure
* `dags/ETL.py`: Handles multi-location historical data ingestion.
* `dags/ML.py`: Manages ML model training, forecasting, and final table union.
* `docker-compose.yaml`: Local Airflow deployment configuration.

## Setup Instructions
1. Navigate to this directory: `cd Lab1`
2. Start environment: `docker-compose up -d`
3. Set Airflow Variables: `weather_locations` (JSON format) , `weather_forecast_url` and `weather_target_table`
4. Connections: `snowflake_conn`
5. Trigger `Lab1_ETL` followed by `Lab1_ML_Forecasting`.