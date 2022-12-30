import requests
import json 
import pendulum
import os

from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator


PRIMARY_TOKEN = os.environ.get("PRIMARY_TOKEN")
endpoint = 'https://gorest.co.in/public/v2/users'
headers = {"Authorization": f"Bearer {PRIMARY_TOKEN}", "Accept":"application/json", "Content-Type":"application/json"}

@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 12, 21, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def api_call_taskflow():
    @task()
    def get_requests_json(endpoint,headers):
        r = requests.get(endpoint,headers) 
        return r.json()
        # return r
    @task()
    def print_json(r):
        return print(f"output: {r}")
    output = get_requests_json(endpoint,headers)
    print_json(output)
api_call_taskflow()