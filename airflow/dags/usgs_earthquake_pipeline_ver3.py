import json
import os 
import pendulum
import requests
import pandas as pd
import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage
from google.oauth2 import service_account

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'raw_usgs_earthquakes')
BIGQUERY_TABLE = 'raw_earthquakes'
CREDENTIALS_DIR = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
SERVICE_ACCOUNT_CREDENTIALS = service_account.Credentials.from_service_account_file(f"{CREDENTIALS_DIR}")

DAY_BEFORE_YESTERDAY = '{{ macros.ds_add(ds, -2) }}'
YESTERDAY = '{{ macros.ds_add(ds, -1) }}'
API_URL = f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={DAY_BEFORE_YESTERDAY}&endtime={YESTERDAY}'
LOCAL_JSON = f'{AIRFLOW_HOME}/{DAY_BEFORE_YESTERDAY}.json'
LOCAL_PARQUET = f'{AIRFLOW_HOME}/{DAY_BEFORE_YESTERDAY}.parquet'
DESTINATION_BLOB_JSON = f"usgs_raw_json/data_{DAY_BEFORE_YESTERDAY}.json"
DESTINATION_BLOB_PARQUET = f"usgs_raw_parquet/data_{DAY_BEFORE_YESTERDAY}.parquet"

BQ_LOAD_DATA_QUERY = (
                f"LOAD DATA INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}` \
                FROM FILES( \
                    format='PARQUET', \
                    uris = ['gs://{BUCKET}/{DESTINATION_BLOB_PARQUET}'] \
                )"
            )

with DAG(
    dag_id='usgs_earthquakes_pipeline_v33',
    schedule="@daily",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=1),
    },
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    description="DE_Zoomcamp Capstone Project Pipeline by Andy Nelson",
    catchup=True,
    max_active_runs=1,
    tags=["USGS_FDSN_EARTHQUAKES"],
) as dag:

    @task()
    def python_requests_api(API_URL):
        json_data = requests.get(API_URL).json()
        return json_data
    @task()
    def save_json_data_to_local(json_data, LOCAL_JSON):
        with open(f"{LOCAL_JSON}", "w") as outfile:
            json.dump(json_data, outfile)
        return LOCAL_JSON
    @task()
    def upload_json_data_to_gcs(BUCKET, LOCAL_JSON, DESTINATION_BLOB_JSON):
        client = storage.Client(credentials=SERVICE_ACCOUNT_CREDENTIALS)
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(DESTINATION_BLOB_JSON)
        blob.upload_from_filename(LOCAL_JSON)
        return DESTINATION_BLOB_JSON
    @task()
    def local_json_to_df(json_data, LOCAL_PARQUET):
        df = pd.json_normalize(json_data, record_path=['features'], meta='metadata')
        df_meta = pd.json_normalize(df['metadata'])
        df = pd.concat([df, df_meta], axis=1, join="inner")
        df[['geometry.coordinates.longitude', 'geometry.coordinates.latitude', 'geometry.coordinates.depth']] = df['geometry.coordinates'].tolist()
        df.drop(['metadata'], axis=1, inplace=True)
        df['properties.alert'] = df['properties.alert'].astype(str)
        df[['properties.mmi', 'properties.felt', 'properties.cdi', 'properties.tz', 'properties.tsunami', 'properties.sig']] = df[['properties.mmi', 'properties.felt', 'properties.cdi', 'properties.tz', 'properties.tsunami', 'properties.sig']].fillna(0)
        df[['properties.felt', 'properties.cdi', 'properties.tz', 'properties.tsunami', 'properties.sig']] = df[['properties.felt', 'properties.cdi', 'properties.tz', 'properties.tsunami', 'properties.sig']].astype(int)
        df['properties.mmi'] = df['properties.mmi'].astype(float)
        df['properties.time.datetime'] = pd.to_datetime(df['properties.time'], unit='ms')
        df['properties.updated.datetime'] = pd.to_datetime(df['properties.updated'], unit='ms')
        df['metadata.generated.datetime'] = pd.to_datetime(df['generated'], unit='ms')
        df['dataframe_timestamp_now'] = pd.Timestamp.now()
        df.columns = df.columns.str.replace(".","_", regex=False)
        df.to_parquet(f'{LOCAL_PARQUET}', use_deprecated_int96_timestamps=True)
        return LOCAL_PARQUET
    @task()
    def upload_parquet_to_gcs(BUCKET, LOCAL_PARQUET_SAVE, DESTINATION_BLOB_PARQUET):
        client = storage.Client(credentials=SERVICE_ACCOUNT_CREDENTIALS)
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(DESTINATION_BLOB_PARQUET)
        blob.upload_from_filename(LOCAL_PARQUET_SAVE)
        return DESTINATION_BLOB_PARQUET
    @task()
    def delete_local_parquet(LOCAL_PARQUET_SAVE):
        my_parquet = f"{LOCAL_PARQUET_SAVE}"
        if os.path.isfile(my_parquet):
            os.remove(my_parquet)
        else:
            print("Error: %s file not found" % my_parquet)
    @task()
    def delete_local_json(LOCAL_JSON):
        my_json = f"{LOCAL_JSON}"
        if os.path.isfile(my_json):
            os.remove(my_json)
        else:
            print("Error: %s file not found" % my_json)
    
    bq_load_data = BigQueryInsertJobOperator(
    task_id="bigquery_load_data",
    configuration={
        "query": {
            "query": BQ_LOAD_DATA_QUERY,
            "useLegacySql": False,
            }
        }
    )

    api_data = python_requests_api(API_URL)
    saved_json_local = save_json_data_to_local(api_data, LOCAL_JSON)
    uploaded_json =  upload_json_data_to_gcs(BUCKET, saved_json_local, DESTINATION_BLOB_JSON)
    create_df = local_json_to_df(api_data, LOCAL_PARQUET)
    uploaded_parquet = upload_parquet_to_gcs(BUCKET, create_df, DESTINATION_BLOB_PARQUET)
    local_json_deleted = delete_local_json(uploaded_json)
    local_parquet_deleted = delete_local_parquet(uploaded_parquet)
    local_parquet_deleted >> bq_load_data
  


    