import requests
import json 
import pendulum
import os

from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
PATH_TO_LOCAL_HOME = os.environ.get("PATH_TO_LOCAL_HOME")
PRIMARY_TOKEN = os.environ.get("PRIMARY_TOKEN")
endpoint = 'https://gorest.co.in/public/v2/users'
headers = {"Authorization": f"Bearer {PRIMARY_TOKEN}", "Accept":"application/json", "Content-Type":"application/json"}

@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 12, 21, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def api_call_taskflow_v1():
    @task()
    def get_requests_json(endpoint,headers):
        r = requests.get(endpoint, headers) 
        data = r.json()
        with open(f'{path_to_local_home}/data.json', 'w') as f:
            json.dump(data, f)
    @task()
    def upload_to_gcs(bucket, object_name, local_file):
        client = storage.Client()
        bucket = client.bucket(bucket)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_file)

dag = api_call_taskflow_v1()