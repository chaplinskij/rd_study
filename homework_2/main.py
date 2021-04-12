import json
import os
import requests
import yaml
from datetime import date
from requests import HTTPError

BASE_DIR = os.getcwd()
try:
    file_path = os.path.join(BASE_DIR, 'configs.yml')
    with open(file_path, 'r') as _file:
        CONFIGS = yaml.safe_load(_file)
except FileNotFoundError:
    raise FileNotFoundError(f'No such file with configs: {file_path}')


def authorize() -> str:
    auth_configs = CONFIGS['auth']
    headers = {'content-type': 'application/json'}
    url = auth_configs['url'] + auth_configs['endpoint']
    data = json.dumps(auth_configs['credentials'])
    try:
        res = requests.post(url, data=data, headers=headers)
        res.raise_for_status()
        return f"{auth_configs['type']} {res.json()['access_token']}"
    except HTTPError as e:
        print(f'HTTPError was raised. Message: {e}')


def get_data_from_api(payload_date: str, access_token: str) -> list:
    api_configs = CONFIGS['api']
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


def check_existing_directory(path: str) -> None:
    try:
        os.makedirs(path)
    except FileExistsError:
        pass


def store_data(payload_date: str, data: list) -> None:
    path = os.path.join(BASE_DIR, CONFIGS['application']['store_folder'], payload_date)
    check_existing_directory(path)
    with open(f'{path}/data.json', 'w') as json_file:
        json.dump(data, json_file)


if __name__ == '__main__':
    access_token = authorize()
    for payload_date in CONFIGS['application']['dates']:
        if isinstance(payload_date, date):
            payload_date = payload_date.isoformat()
        data = get_data_from_api(payload_date, access_token)
        store_data(payload_date, data)
