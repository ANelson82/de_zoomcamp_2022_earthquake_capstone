import json
import os 
import pendulum
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from google.oauth2 import service_account

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
PRIMARY_TOKEN = os.environ.get("PRIMARY_TOKEN")
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
CREDENTIALS_DIR = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
SA_CREDENTIALS = service_account.Credentials.from_service_account_file(f"{CREDENTIALS_DIR}")

api_url = 'https://gorest.co.in/public/v2/users'
headers = {"Authorization": f"Bearer {PRIMARY_TOKEN}", "Accept":"application/json", "Content-Type":"application/json"}
dt = pendulum.now()
local_file = f'{AIRFLOW_HOME}/data_{dt}.json'
destination_blob_name = f"gorest/data_{dt}.json"

from airflow.decorators import dag, task
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def tutorial_taskflow_api():
    @task()
    def extract(api_url, headers):
        json_data = requests.get(api_url, headers).json()
        return json_data
    @task()
    def save_file_local(json_data: json):
        with open(f'{local_file}', "w") as f:
            json.dump(json_data, f)
        return local_file
    @task()
    def upload_to_gcs(BUCKET, local_file, destination_blob_name):
        client = storage.Client(credentials=SA_CREDENTIALS)
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(local_file)
    api_data = extract(api_url, headers)
    local_file_sent =  save_file_local(api_data)
    upload_to_gcs(BUCKET, local_file, destination_blob_name)
tutorial_taskflow_api()
