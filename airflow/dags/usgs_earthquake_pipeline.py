import json
import os 
import pendulum
import requests

# from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
from google.cloud import storage
from google.oauth2 import service_account

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
PRIMARY_TOKEN = os.environ.get("PRIMARY_TOKEN")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
CREDENTIALS_DIR = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
SERVICE_ACCOUNT_CREDENTIALS = service_account.Credentials.from_service_account_file(f"{CREDENTIALS_DIR}")

day_before_yesterday = pendulum.now().subtract(days=2).format('YYYY-MM-DD')
yesterday = pendulum.now().subtract(days=1).format('YYYY-MM-DD')
api_url = f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={day_before_yesterday}&endtime={yesterday}'
local_file = f'{AIRFLOW_HOME}/usgs_fdsn_{yesterday}.json'
destination_blob_name = f"usgs_fdsn_raw/data_{yesterday}.json"

@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 12, 1, tz="UTC"),
    catchup=False,
    tags=["USGS_FDSN"],
)
def usgs_earthquake_pipeline_v1():
    @task()
    def extract(api_url):
        json_data = requests.get(api_url, headers).json()
        return json_data
    @task()
    def save_file_local(json_data: json):
        with open(f'{local_file}', "w") as f:
            json.dump(json_data, f)
        local_file_saved = local_file
        return local_file_saved
    @task()
    def upload_to_gcs(BUCKET, local_file_saved, destination_blob_name):
        client = storage.Client(credentials=SERVICE_ACCOUNT_CREDENTIALS)
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file_saved)
    api_data = extract(api_url)
    local_file_sent =  save_file_local(api_data)
    upload_to_gcs(BUCKET, local_file_sent, destination_blob_name)
usgs_earthquake_pipeline_v1()
