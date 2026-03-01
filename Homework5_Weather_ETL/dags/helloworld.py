from airflow.decorators import task
from airflow import DAG
from datetime import datetime

@task
def print_hello():
    print("hello!")
    return "hello!"

@task
def print_goodbye():
    print("goodbye!")
    return "goodbye!"

with DAG(
    dag_id = 'HelloWorld',
    start_date = datetime(2026,2,24),
    catchup=False,
    tags=['example'],
    schedule = '0 2 * * *'
) as dag:
    # 按顺序运行两个任务
    print_hello() >> print_goodbye()