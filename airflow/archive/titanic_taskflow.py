import os
import datetime as dt

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator


with DAG(
    dag_id='titanic_dag',
    start_date=dt.datetime(2021, 3, 1),
    schedule_interval='@once'
) as dag:

    check_if_file_exists = SimpleHttpOperator(
        method='HEAD',
        task_id='check_file_existence',
        http_conn_id='web_stanford_http_id',
        endpoint='/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv',
    )

    @task
    def download_titanic_dataset():
        url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
        response = requests.get(url, stream=True)
        response.raise_for_status()
        filepath = os.path.join(os.path.expanduser('~'), 'titanic.csv')
        with open(filepath, 'w', encoding='utf-8') as f:
            for chunk in response.iter_lines():
                f.write('{}\n'.format(chunk.decode('utf-8')))
        return filepath

    @task
    def get_number_of_lines(file_path):
        lines = 0
        with open(file_path) as f:
            for line in f:
                if line:
                    lines += 1
        return lines

    file_path = download_titanic_dataset()
    number_of_lines = get_number_of_lines(file_path)

    check_if_file_exists >> file_path

    # check_if_file_exists >> file_path >> number_of_lines  # так тоже можно