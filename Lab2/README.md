# Lab 2: End-to-End Data Analytics with Snowflake, Airflow, dbt, and Tableau

## Overview
This project implements an end-to-end data analytics pipeline using **Snowflake**, **Apache Airflow**, **dbt**, and **Tableau**.  
The pipeline ingests historical weather data from the Open-Meteo API, loads the raw records into Snowflake, transforms them into analytics-ready tables using dbt, and visualizes the results through a Tableau dashboard.

The project extends the ETL architecture from Lab 1 by adding a dbt-based ELT workflow that is triggered after ETL completion.

## Tech Stack
- Snowflake
- Apache Airflow
- dbt
- Tableau Public
- Docker / Docker Compose
- Python

## Project Objective
The objective of this lab is to build a complete analytics workflow that:
- loads raw weather data into Snowflake using Airflow,
- transforms the raw data into analytical tables using dbt,
- schedules dbt execution after ETL in Airflow,
- visualizes transformed data through a BI dashboard.

## Dataset
- **Source:** Open-Meteo API
- **Domain:** Historical weather data
- **Granularity:** Daily weather observations by location

### Main Raw Fields
- `location_name`
- `date`
- `temp_max`
- `temp_min`
- `temp_mean`
- `precipitation`
- `weather_code`

## System Architecture
The workflow follows this data flow:

Open-Meteo API  
→ Airflow ETL DAG  
→ Snowflake `RAW.WEATHER_HISTORICAL`  
→ Airflow triggers dbt DAG  
→ dbt models / tests / snapshot  
→ Snowflake `ANALYTICS.AVG_WEATHER_METRICS`  
→ Tableau Dashboard

## Project Structure
```text
Lab2/
├── dags/
│   ├── ETL.py
│   └── ELT.py
├── weather_transform/
│   ├── models/
│   │   ├── sources.yml
│   │   ├── schema.yml
│   │   └── avg_weather_metrics.sql
│   ├── snapshots/
│   │   └── weather_historical_snapshot.sql
│   ├── packages.yml
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── README.md
├── figures/
│   ├── airflow_etl_dag.png
│   ├── airflow_dbt_dag.png
|   ├── airflow_dags.png
|   ├── architecture.png
│   ├── dbt_run.png
│   ├── dbt_test.png
│   ├── dbt_snapshot.png
│   ├── dashboard_full.png
│   └── dashboard_filtered.png
├── docker-compose.yaml
├── Dockerfile
└── README.md