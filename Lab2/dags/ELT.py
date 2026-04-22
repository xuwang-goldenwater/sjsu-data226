from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

# get dbt environment variables from Airflow connection
def get_dbt_env():
    conn = BaseHook.get_connection('snowflake_conn')
    extra = conn.extra_dejson if conn.extra_dejson else {}
    return {
        'SNOWFLAKE_ACCOUNT': extra.get('account',''),
        'SNOWFLAKE_USER': conn.login,
        'SNOWFLAKE_PASSWORD': conn.password,
        'SNOWFLAKE_ROLE': extra.get('role', 'TRAINING_ROLE'),
        'SNOWFLAKE_DATABASE': extra.get('database', 'USER_DB_FOX'),
        'SNOWFLAKE_WAREHOUSE': extra.get('warehouse', 'FOX_QUERY_WH'),
    }

with DAG(
    dag_id='Lab2_dbt_Workflow',
    start_date=datetime(2026, 2, 28),
    description='A sample Airflow DAG to invoke dbt runs using a BashOperator',
    catchup=False,
    schedule=None, 
    tags=['Lab2', 'dbt', 'ELT']
) as dag:

    # set dbt project path
    dbt_path = "/opt/airflow/dbt_project"
    profiles_dir = "/opt/airflow/.dbt"

    # 1. dbt run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {dbt_path} && dbt run --profiles-dir {profiles_dir}',
        env=get_dbt_env(),
        append_env=True,
    )

    # 2. dbt test 
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {dbt_path} && dbt test --profiles-dir {profiles_dir}',
        env=get_dbt_env(),
        append_env=True,
    )

    # 3. dbt snapshot
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f'cd {dbt_path} && dbt snapshot --profiles-dir {profiles_dir}',
        env=get_dbt_env(),  
        append_env=True,
    )

    # define dependencies
    dbt_run >> dbt_test >> dbt_snapshot