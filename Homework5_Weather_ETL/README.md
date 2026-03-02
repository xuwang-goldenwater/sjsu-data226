# Homework 5: Weather Forecast ETL Pipeline

## Project Description
This project implements a foundational ETL (Extract, Transform, Load) pipeline designed to harvest meteorological data and ensure data integrity within a cloud warehouse environment.

## Key Features
* **Data Source**: Integration with the Open-Meteo API to fetch daily max/min temperatures and precipitation.
* **SQL Transactions**: Utilizes `BEGIN`, `COMMIT`, and `ROLLBACK` commands within Snowflake to ensure atomic data loading and prevent corruption.
* **Full Refresh Pattern**: Implements a "Delete-before-Insert" strategy to maintain data consistency during re-runs.
* **Orchestration**: Managed by Apache Airflow within a Dockerized environment to ensure portability.

## Technical Stack
* **Orchestrator**: Apache Airflow
* **Database**: Snowflake
* **Infrastucture**: Docker & Docker Compose
* **Language**: Python (Requests, Snowflake-Connector)

## Setup
1. Navigate to this directory: `cd Homework5_Weather_ETL`
2. Start environment: `docker-compose up -d`
3. Configure `snowflake_conn` in Airflow UI.