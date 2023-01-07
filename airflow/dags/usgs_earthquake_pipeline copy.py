import os 
import pendulum
import requests
import pandas as pd
import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
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

BQ_LOAD_DATA_QUERY = (
                f"LOAD DATA INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}` \
                FROM FILES( \
                    format='PARQUET', \
                    uris = ['gs://{BUCKET}/{DESTINATION_BLOB_PARQUET}'] \
                )"
            )

# def parquet_exists_in_gcs(BUCKET, DESTINATION_BLOB_PARQUET):
#         client = storage.Client()
#         bucket = client.get_bucket(BUCKET)
#         blob = bucket.blob(DESTINATION_BLOB_PARQUET)
#         return blob.exists()

with DAG(
    dag_id='usgs_earthquake_pipeline_v15',
    schedule="@daily",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=3),
    },
    start_date=pendulum.datetime(2022, 12, 26, tz="UTC"),
    description="DE_Zoomcamp Capstone Project Pipeline by Andy Nelson",
    catchup=False,
    max_active_runs=1,
    tags=["USGS_FDSN_EARTHQUAKES"],
) as dag:

    # @task()
    # def echo_blob_file(DESTINATION_BLOB_PARQUET):
    #     bash_instance = BashOperator(
    #     task_id="also_run_this",
    #     bash_command=f'echo {DESTINATION_BLOB_PARQUET2}',
    #     )
    #     return DESTINATION_BLOB_PARQUET

    @task.short_circuit()
    def parquet_exists_in_gcs(BUCKET, DESTINATION_BLOB_PARQUET):
        client = storage.Client()
        bucket = client.get_bucket(BUCKET)
        blob = bucket.blob(DESTINATION_BLOB_PARQUET)
        return blob.exists()

    @task()
    def python_requests_api(API_URL):
        json_data = requests.get(API_URL).json()
        return json_data
    @task()
    def nested_json_to_df(json_data):
        df = pd.json_normalize(json_data, record_path=['features'], meta='metadata')
        df_meta = pd.json_normalize(df['metadata'])
        df = pd.concat([df, df_meta], axis=1, join="inner")
        df[['geometry.coordinates.longitude', 'geometry.coordinates.latitude', 'geometry.coordinates.depth']] = df['geometry.coordinates'].tolist()
        df.drop(['metadata', 'geometry.coordinates'], axis=1, inplace=True)
        df['properties.time.datetime'] = pd.to_datetime(df['properties.time'], unit='ms')
        df['properties.updated.datetime'] = pd.to_datetime(df['properties.updated'], unit='ms')
        df['metadata.generated.datetime'] = pd.to_datetime(df['generated'], unit='ms')
        return df
    @task()
    def list_to_df_to_parquet_to_local(df):
        df.to_parquet(f'{LOCAL_PARQUET}', use_deprecated_int96_timestamps=True)
        LOCAL_PARQUET_SAVE = LOCAL_PARQUET
        return LOCAL_PARQUET_SAVE
    @task()
    def upload_to_gcs(BUCKET, LOCAL_PARQUET_SAVE, DESTINATION_BLOB_PARQUET):
        client = storage.Client(credentials=SERVICE_ACCOUNT_CREDENTIALS)
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(DESTINATION_BLOB_PARQUET)
        blob.upload_from_filename(LOCAL_PARQUET_SAVE)
        return DESTINATION_BLOB_PARQUET
    @task()
    def delete_local_file(LOCAL_PARQUET_SAVE):
        myfile = f"{LOCAL_PARQUET_SAVE}"
        if os.path.isfile(myfile):
            os.remove(myfile)
        else:
            print("Error: %s file not found" % myfile)

    bq_load_data = BigQueryInsertJobOperator(
    task_id="bigquery_load_data_task",
    configuration={
        "query": {
            "query": BQ_LOAD_DATA_QUERY,
            "useLegacySql": False,
            }
        }
    )

    bash_instance = BashOperator(
    task_id="echo_destination_blob_parquet",
    bash_command=f'echo "{DESTINATION_BLOB_PARQUET}"',
    )

    # test_parquet_already_present = ShortCircuitOperator(
    #     task_id='test_parquet_already_present',
    #     provide_context=True,
    #     python_callable=parquet_exists_in_gcs,
    #     op_kwargs=
    # )

    # bash_echo_sent = echo_blob_file()
    # test_return = test_parquet_already_present()
    boolean_parquet_in_gcs = parquet_exists_in_gcs(BUCKET, DESTINATION_BLOB_PARQUET)
    api_data = python_requests_api(API_URL)
    exported_list = nested_json_to_list(api_data)
    local_file_sent =  list_to_df_to_parquet_to_local(exported_list)
    uploaded = upload_to_gcs(BUCKET, local_file_sent, DESTINATION_BLOB_PARQUET)
    local_file_deleted = delete_local_file(uploaded)
    bash_instance >> boolean_parquet_in_gcs >> api_data >> exported_list >> local_file_sent >> uploaded >> bq_load_data
  
