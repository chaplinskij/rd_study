from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from services.pg_dshop import store_data


default_args = {
    'owner': 'airflow',
    'email': ['chaplinskij@gmail.com'],
    'email_on_failure': False,
    'retries': 2,
}

dag = DAG(
    dag_id='pg_dshop_dag',
    description='Store data from PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime(2021, 5, 16, 1, 0),
    default_args=default_args,
)

t1 = PythonOperator(
    task_id='pg_dump_function',
    dag=dag,
    python_callable=store_data
)