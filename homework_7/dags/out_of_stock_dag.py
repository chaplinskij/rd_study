from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from services.out_of_stock import store_data


default_args = {
    'owner': 'airflow',
    'email': ['chaplinskij@gmail.com'],
    'email_on_failure': False,
    'retries': 2,
}

dag = DAG(
    dag_id='out_of_stock_dag',
    description='Store out of stock data',
    schedule_interval='@daily',
    start_date=datetime(2021, 5, 16, 1, 0),
    default_args=default_args,
)

t1 = PythonOperator(
    task_id='out_of_stock_function',
    dag=dag,
    python_callable=store_data
)
