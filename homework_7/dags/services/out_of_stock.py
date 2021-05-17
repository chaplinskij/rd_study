import json
import os
import requests
import yaml
from datetime import date
from requests import HTTPError

BASE_DIR = os.path.join(os.getcwd(), 'airflow', 'dags')


def load_config():
    file_path = os.path.join(BASE_DIR, 'services', 'configs.yml')
    try:
        with open(file_path, 'r') as _file:
            return yaml.safe_load(_file)
    except FileNotFoundError:
        raise FileNotFoundError(f'No such file with configs: {file_path}')


def authorize(auth_configs) -> str:
    headers = {'content-type': 'application/json'}
    url = auth_configs['url'] + auth_configs['endpoint']
    data = json.dumps(auth_configs['credentials'])
    try:
        res = requests.post(url, data=data, headers=headers)
        res.raise_for_status()
        return f"{auth_configs['type']} {res.json()['access_token']}"
    except HTTPError as e:
        print(f'HTTPError was raised. Message: {e}')


def get_data_from_api(payload_date: str, access_token: str, api_configs) -> list:
    headers = {
        'content-type': 'application/json',
        'Authorization': access_token
    }
    url = api_configs['url'] + api_configs['endpoint']
    payload = json.dumps({'date': payload_date})
    try:
        res = requests.get(url, data=payload, headers=headers)
        res.raise_for_status()
        return res.json()
    except HTTPError as e:
        print(f'HTTPError was raised. Message: {e}')


def save_data(payload_date: str, data: list, store_folder: str) -> None:
    path = os.path.join(BASE_DIR, store_folder, payload_date)
    os.makedirs(path, exist_ok=True)
    with open(f'{path}/data.json', 'w') as json_file:
        json.dump(data, json_file)


def store_data() -> None:
    configs = load_config()
    access_token = authorize(configs['auth'])
    store_folder = os.path.join(configs['application']['store_folder'], 'out_of_stock')
    for payload_date in configs['application']['dates']:
        if isinstance(payload_date, date):
            payload_date = payload_date.isoformat()
        data = get_data_from_api(payload_date, access_token, configs['api'])
        save_data(payload_date, data, store_folder)
