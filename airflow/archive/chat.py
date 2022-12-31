from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage

import requests
import os 
import json 
from datetime import datetime, timedelta


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
PRIMARY_TOKEN = os.environ.get("PRIMARY_TOKEN")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")

api_url = 'https://gorest.co.in/public/v2/users'
headers = {"Authorization": f"Bearer {PRIMARY_TOKEN}", "Accept":"application/json", "Content-Type":"application/json"}

# Default Arguments for the DAG
default_args = {
    'owner': 'me',
    'start_date': days_ago(2),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Initialize the DAG
dag = DAG(
    'chat_v3',
    default_args=default_args,
    schedule_interval= "@daily",
)

# Define a function that retrieves data from the API and returns it
def get_data_from_api(**kwargs):
    data = (requests.get(api_url, headers)).json()
    with open('temp/data.json', 'w') as f:
        json.dump(data, f)

# Define a function that uploads a file to Google Cloud Storage
def upload_to_gcs():
    # Set up the Google Cloud Storage client
    storage_client = storage.Client()
    
    # Create a bucket if it doesn't already exist
    bucket_name = BUCKET
    bucket = storage_client.get_bucket(bucket_name)
    # if not bucket.exists():
    #     bucket = storage_client.create_bucket(bucket_name)
    
    # Create a blob and upload the data to it
    blob = bucket.blob('raw/data.json')
    blob.upload_from_filename('temp/data.json')

# Define the tasks in the DAG
get_data_task = PythonOperator(
    task_id='get_data_task',
    provide_context=True,
    python_callable=get_data_from_api,
    dag=dag,
)

upload_data_task = PythonOperator(
    task_id='upload_data_task',
    provide_context=True,
    python_callable=upload_to_gcs,
    dag=dag,
)
