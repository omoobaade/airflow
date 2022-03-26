from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


import pandas as pd

def convert():
    Payment_code = {'Electronic check': 1, 'Mailed check': 2, 'Bank transfer (automatic)': 3,
                    'Credit card (automatic)': 4}
    df = pd.read_csv('/Users/adebolafagbule/airflow/Telco.csv')


def create_num_id():
    df = pd.read_csv('/Users/adebolafagbule/airflow/Telco.csv')
    df['Customer_Num'] = df['customerID'].apply(lambda x: x.split('-')[0])
    df['Customer_Xter'] = df['customerID'].apply(lambda x: x.split('-')[1])

    df.to_csv('/Users/adebolafagbule/airflow/updated_file.csv')

def square():
    df = pd.read_csv('/Users/adebolafagbule/airflow/updated_file.csv')
    df['square'] = df['Customer_Num'].apply(lambda x: int(x) ** 2)
    df.to_csv('/Users/adebolafagbule/airflow/updated_file_1.csv')




default_args = {
    'depends_on_past': False,
    'email': ['adebola.fagbule1@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
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
        python_callable=convert
    )

    t3 = PythonOperator(
        task_id='Create_Num_ID',
        python_callable=create_num_id
    )

    t4 = PythonOperator(
        task_id='Square',
        python_callable=square
    )

    t5 = BashOperator(
        task_id='Print_End_Time',
        bash_command='date'

    )

    t1 >> t2 >> t3 >> t4 >> t5


