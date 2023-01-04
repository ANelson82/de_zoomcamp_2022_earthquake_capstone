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

with DAG(
    dag_id='usgs_earthquake_pipeline_v12',
    schedule="@daily",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=3),
    },
    start_date=pendulum.datetime(2022, 12, 26, tz="UTC"),
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
    def nested_json_to_list(json_data):
        quakes = json_data
        quake_features = quakes['features']
        feature_list = []
        i = 0
        while i < len(quake_features):
            for items in quake_features:
                quake_dic = {}
                quake_dic['type'] = quake_features[i]['type']
                quake_dic['properties_mag'] = quake_features[i]['properties']['mag']
                quake_dic['properties_place'] = quake_features[i]['properties']['place']
                quake_dic['properties_time'] = quake_features[i]['properties']['time']
                quake_dic['properties_updated'] = quake_features[i]['properties']['updated']
                quake_dic['properties_tz'] = quake_features[i]['properties']['tz']
                quake_dic['properties_url'] = quake_features[i]['properties']['url']
                quake_dic['properties_detail'] = quake_features[i]['properties']['detail']
                quake_dic['properties_felt'] = quake_features[i]['properties']['felt']
                quake_dic['properties_cdi'] = quake_features[i]['properties']['cdi']
                quake_dic['properties_mmi'] = quake_features[i]['properties']['mmi']
                quake_dic['properties_alert'] = quake_features[i]['properties']['alert']
                quake_dic['properties_status'] = quake_features[i]['properties']['status']
                quake_dic['properties_tsunami'] = quake_features[i]['properties']['tsunami']
                quake_dic['properties_sig'] = quake_features[i]['properties']['sig']
                quake_dic['properties_net'] = quake_features[i]['properties']['net']
                quake_dic['properties_code'] = quake_features[i]['properties']['code']
                quake_dic['properties_ids'] = quake_features[i]['properties']['ids']
                quake_dic['properties_sources'] = quake_features[i]['properties']['sources']
                quake_dic['properties_types'] = quake_features[i]['properties']['types']
                quake_dic['properties_nst'] = quake_features[i]['properties']['nst']
                quake_dic['properties_dmin'] = quake_features[i]['properties']['dmin']
                quake_dic['properties_rms'] = quake_features[i]['properties']['rms']
                quake_dic['properties_gap'] = quake_features[i]['properties']['gap']
                quake_dic['properties_magType'] = quake_features[i]['properties']['magType']
                quake_dic['properties_type'] = quake_features[i]['properties']['type']
                quake_dic['properties_title'] = quake_features[i]['properties']['title']
                quake_dic['geometry_type'] = quake_features[i]['geometry']['type']
                quake_dic['geometry_long'] = quake_features[i]['geometry']['coordinates'][0]
                quake_dic['geometry_lat'] = quake_features[i]['geometry']['coordinates'][1]
                quake_dic['geometry_focaldepth'] = quake_features[i]['geometry']['coordinates'][2]
                quake_dic['id'] = quake_features[i]['id']
                quake_dic['quake_api_type'] = quakes['type']
                quake_dic['quake_metadata_generated'] = quakes['metadata']['generated']
                quake_dic['quake_metadata_url'] = quakes['metadata']['url']
                quake_dic['quake_metadata_title'] = quakes['metadata']['title']
                quake_dic['quake_metadata_status'] = quakes['metadata']['status']
                quake_dic['quake_metadata_api'] = quakes['metadata']['api']
                quake_dic['quake_metadata_count'] = quakes['metadata']['count']
                feature_list.append(quake_dic)
                i += 1
        return feature_list
    @task()
    def list_to_df_to_parquet_to_local(feature_list):
        df = pd.DataFrame.from_dict(feature_list)
        df['properties_time_datetime'] = pd.to_datetime(df['properties_time'])
        df['properties_updated_datetime'] = pd.to_datetime(df['properties_updated'])
        df['quake_metadata_generated_datetime'] = pd.to_datetime(df['quake_metadata_generated'])
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
        # If file exists, delete it.
        if os.path.isfile(myfile):
            os.remove(myfile)
        else:
            # If it fails, inform the user.
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

    api_data = python_requests_api(API_URL)
    exported_list = nested_json_to_list(api_data)
    local_file_sent =  list_to_df_to_parquet_to_local(exported_list)
    uploaded = upload_to_gcs(BUCKET, local_file_sent, DESTINATION_BLOB_PARQUET)
    local_file_deleted = delete_local_file(uploaded)
    api_data >> exported_list >> local_file_sent >> uploaded >> bq_load_data
  