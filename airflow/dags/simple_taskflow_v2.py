import json
import os 
import pendulum
import requests
import pandas as pd
import pyarrow

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from google.oauth2 import service_account

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
PRIMARY_TOKEN = os.environ.get("PRIMARY_TOKEN")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
CREDENTIALS_DIR = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
SA_CREDENTIALS = service_account.Credentials.from_service_account_file(f"{CREDENTIALS_DIR}")

api_url = 'https://gorest.co.in/public/v2/users'
headers = {"Authorization": f"Bearer {PRIMARY_TOKEN}", "Accept":"application/json", "Content-Type":"application/json"}
dt = pendulum.now()
local_parquet = f'{AIRFLOW_HOME}/data_{dt}.parquet'
destination_blob_parquet = f"gorest/data_{dt}.parquet"

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["gorest"],
)
def tutorial_taskflow_api_v3():
    @task()
    def python_requests_api(api_url, headers):
        json_data = requests.get(api_url, headers).json()
        return json_data
    @task()
    def json_to_df_to_parquet_to_local(json_data):
        df = pd.DataFrame.from_dict(json_data)
        df.to_parquet(f'{local_parquet}')
        local_parquet_save = local_parquet
        return local_parquet_save
    @task()
    def upload_to_gcs(BUCKET, local_parquet_save, destination_blob_parquet):
        client = storage.Client(credentials=SA_CREDENTIALS)
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(destination_blob_parquet)
        blob.upload_from_filename(local_parquet_save)
        return destination_blob_parquet
    api_data = python_requests_api(api_url, headers)
    local_file_sent =  json_to_df_to_parquet_to_local(api_data)
    upload_to_gcs(BUCKET, local_file_sent, destination_blob_parquet)
tutorial_taskflow_api_v3()
