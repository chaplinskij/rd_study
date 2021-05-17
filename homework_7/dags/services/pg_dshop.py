import os
import psycopg2
import yaml


BASE_DIR = os.path.join(os.getcwd(), 'airflow', 'dags')


def load_config():
    file_path = os.path.join(BASE_DIR, 'services', 'configs.yml')
    try:
        with open(file_path, 'r') as _file:
            return yaml.safe_load(_file)
    except FileNotFoundError:
        raise FileNotFoundError(f'No such file with configs: {file_path}')


def store_data():
    configs = load_config()
    store_folder = os.path.join(BASE_DIR, configs['application']['store_folder'], 'dshop')
    os.makedirs(store_folder, exist_ok=True)
    with psycopg2.connect(**configs['postgres']) as pg_connection:
        cursor = pg_connection.cursor()
        for table_name in configs['application']['dshop_tables']:
            path = os.path.join(store_folder, table_name + '.csv')
            with open(path, 'w') as output:
                cursor.copy_expert(f'COPY public.{table_name} TO STDOUT WITH HEADER CSV', output)
