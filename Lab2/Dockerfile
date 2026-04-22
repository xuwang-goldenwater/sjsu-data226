FROM apache/airflow:2.10.1

USER root
RUN apt-get update && apt-get install -y build-essential git && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake==5.7.0 \
    snowflake-connector-python==3.12.2 \
    dbt-core==1.8.7 \
    dbt-snowflake==1.8.1