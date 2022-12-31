import os
import datetime as dt

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    response = requests.get(url, stream=True)
    response.raise_for_status()
    filepath = os.path.join(os.path.expanduser('~'), 'titanic.csv')
    with open(filepath, 'w', encoding='utf-8') as f:
        for chunk in response.iter_lines():
            f.write('{}\n'.format(chunk.decode('utf-8')))
    return filepath


def get_number_of_lines(file_path):
    lines = 0
    with open(file_path) as f:
        for line in f:
            if line:
                lines += 1
    return lines


with DAG(
        dag_id='titanic_old_style_dag',
        start_date=dt.datetime(2021, 3, 1),
        schedule_interval='@once'
) as dag:
    download_task = PythonOperator(
        task_id='download_task',
        python_callable=download_titanic_dataset,
    )

    get_lines_task = PythonOperator(
        task_id='get_lines',
        op_args=['{{ ti.xcom_pull(task_ids="download_task") }}'],
        python_callable=get_number_of_lines
    )

    download_task >> get_lines_task