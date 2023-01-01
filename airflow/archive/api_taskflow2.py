import requests
import json 
import pendulum
import os

from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from google.oauth2 import service_account


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
# PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
PRIMARY_TOKEN = os.environ.get("PRIMARY_TOKEN")
CREDENTIALS_DIR = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
SA_CREDENTIALS = service_account.Credentials.from_service_account_file(f"{CREDENTIALS_DIR}")
api_url = 'https://gorest.co.in/public/v2/users'
headers = {"Authorization": f"Bearer {PRIMARY_TOKEN}", "Accept":"application/json", "Content-Type":"application/json"}
dt = pendulum.now()
local_file = f'{AIRFLOW_HOME}/data_{dt}.json'
destination_blob_name = f"gorest/data_{dt}.json"

@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 12, 21, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def api_call_taskflow_v3():
    @task()
    def extract(api_url, headers):
        url = 'https://gorest.co.in/public/v2/users'
        response = requests.get(api_url, headers)
        response.raise_for_status()
        response_json = response.json()
        with open(local_file, 'w', encoding='utf-8') as f:
            f.write(response_json)
        return local_file
    @task()
    def upload_to_gcs(BUCKET, local_file, destination_blob_name):
        client = storage.Client(credentials=SA_CREDENTIALS)
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(local_file)
        return blob
    api_data = extract(api_url, headers)
    upload_to_gcs(BUCKET, api_data, destination_blob_name)
api_call_taskflow_v3()