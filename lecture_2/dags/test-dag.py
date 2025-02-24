import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='test_dag',
    start_date=datetime.datetime(2023,11,19),
    schedule_interval='@daily'
    ) as dag:

    step1 = BashOperator(
        task_id='print_date',
        bash_command='date')

    step2 = BashOperator(
        task_id='echo',
        bash_command='echo 123')

    step1 >> step2