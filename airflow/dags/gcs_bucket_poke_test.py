import os 
import pendulum
import requests
import pandas as pd
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
# from airflow.decorators import task
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage
from google.oauth2 import service_account

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'raw_usgs_earthquake')
BIGQUERY_TABLE = 'raw_earthquake_ingestion'
CREDENTIALS_DIR = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
SERVICE_ACCOUNT_CREDENTIALS = service_account.Credentials.from_service_account_file(f"{CREDENTIALS_DIR}")

DAY_BEFORE_YESTERDAY = '{{ macros.ds_add(ds, -2) }}'
YESTERDAY = '{{ macros.ds_add(ds, -1) }}'
API_URL = f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={DAY_BEFORE_YESTERDAY}&endtime={YESTERDAY}'
LOCAL_PARQUET = f'{AIRFLOW_HOME}/usgs_fdsn_{YESTERDAY}.parquet'
DESTINATION_BLOB_PARQUET = f"usgs_fdsn_raw/data_{YESTERDAY}.parquet"
DESTINATION_BLOB_PARQUET2 = f"usgs_fdsn_raw/data_{DAY_BEFORE_YESTERDAY}.parquet"

# # Define a function that returns True if the file is present, and False if it is not
# def check_if_file_exists(BUCKET, DESTINATION_BLOB_PARQUET):
#     from google.cloud import storage

#     # Connect to Google Cloud Storage
#     client = storage.Client()

#     # Get the bucket and file
#     bucket = client.get_bucket(BUCKET)
#     blob = bucket.blob(DESTINATION_BLOB_PARQUET)

#     # Return True if the file exists, and False if it does not
#     return blob.exists()

with DAG(
    dag_id='check_gcs_object_v3',
    schedule=None,
    start_date=pendulum.datetime(2022, 12, 1, tz="UTC"),
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=3),
    },
) as dag:

    also_run_this = BashOperator(
    task_id="also_run_this",
    bash_command=f'echo {DESTINATION_BLOB_PARQUET2}',
    )

    # Define the task that checks if the file is present using the GCSObjectCheckOperator
    check_gcs_file_task = GCSObjectExistenceSensor(
    task_id='check_gcs_file',
    bucket=BUCKET,
    mode='poke',
    object=DESTINATION_BLOB_PARQUET2,
    )

    also_run_this >> check_gcs_file_task
