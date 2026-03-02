# DATA-226: Data Engineering Portfolio

This repository contains a collection of data engineering projects developed for the DATA-226 course at SJSU. It demonstrates end-to-end pipeline orchestration, cloud data warehousing, and integrated machine learning.

## 📂 Project Navigation

### 1. [Lab 1: Weather Forecasting & ML Pipeline](./Lab1)
* **Goal**: Build an automated system to predict weather trends using Snowflake Cortex ML.
* **Key Tech**: Airflow, Docker, Snowflake (ML.FORECAST), Multi-location support.
* **Status**: Completed.

### 2. [Homework 5: Weather Forecast ETL](./Homework5_Weather_ETL)
* **Goal**: Implement a robust ETL pipeline with transaction control.
* **Key Tech**: SQL Transactions (BEGIN/COMMIT), Full Refresh Pattern, Dockerized Airflow.
* **Status**: Completed.

---

## 🛠️ Global Setup Instructions

1. **Clone the repository**:
   ```bash
   git clone https://github.com/xuwang-goldenwater/sjsu-data226.git
   ```
2. **Navigate to a specific project**: Choose either `Lab1/` or `Homework5_Weather_ETL/`.
3. **Environment**: Ensure Docker Desktop is running, then use `docker-compose up -d`.

---
*Note: This repository is maintained for academic purposes. All lecture materials and binary files are excluded via .gitignore.*
