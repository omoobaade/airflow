from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src import Training

import pandas as pd


default_args = {
    'depends_on_past': False,
    'email': ['adebola.fagbule1@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date' : datetime(2022,3,25)
}

with DAG(
    dag_id='class_training',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False) as dag:

    t1 = BashOperator(
        task_id='print_start_date',
        bash_command='date',
    )

    t2 = PythonOperator(
        task_id='Convert',
        python_callable=Training.convert
    )

    t3 = PythonOperator(
        task_id='Create_Num_ID',
        python_callable=Training.create_num_id
    )

    t4 = PythonOperator(
        task_id='Square',
        python_callable=Training.square
    )

    t5 = BashOperator(
        task_id='Print_End_Time',
        bash_command='date'

    )

    t1 >> t2 >> t3 >> t4 >> t5


