import requests
import json 
import pendulum
import os

from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage


PRIMARY_TOKEN = os.environ.get("PRIMARY_TOKEN")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

endpoint = 'https://gorest.co.in/public/v2/users'
headers = {"Authorization": f"Bearer {PRIMARY_TOKEN}", "Accept":"application/json", "Content-Type":"application/json"}

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2022, 12, 21, tz="UTC"),
    catchup=False,
    tags=["earthquake"],
)
def API_to_GCS():
    @task()
    def extract_json_from_api():
        endpoint = 'https://gorest.co.in/public/v2/users'
        headers = {"Authorization": f"Bearer {PRIMARY_TOKEN}", "Accept":"application/json", "Content-Type":"application/json"}
        r = requests.get(endpoint,headers) 
        return r.json()
    @task()
    def upload_json_to_gcs(bucket, object_name, local_file):
        PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
        BUCKET = os.environ.get("GCP_GCS_BUCKET")
        client = storage.Client()
        bucket = client.bucket(bucket)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_file)
    data = extract_json_from_api(endpoint,headers)
    upload_json_to_gcs(bucket, data)
api_call_taskflow()
