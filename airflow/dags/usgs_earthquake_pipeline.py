import json
import os 
import pendulum
import requests
import pandas as pd
import pyarrow
import datetime

from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from google.cloud import storage
from google.oauth2 import service_account

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
CREDENTIALS_DIR = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
SERVICE_ACCOUNT_CREDENTIALS = service_account.Credentials.from_service_account_file(f"{CREDENTIALS_DIR}")

day_before_yesterday = '{{ macros.ds_add(ds, -2) }}'
yesterday = '{{ macros.ds_add(ds, -1) }}'
api_url = f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={day_before_yesterday}&endtime={yesterday}'
local_parquet = f'{AIRFLOW_HOME}/usgs_fdsn_{yesterday}.parquet'
destination_blob_parquet = f"usgs_fdsn_raw/data_{yesterday}.parquet"

@dag(
    schedule="@daily",
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=3),
    },
    start_date=pendulum.datetime(2022, 12, 26, tz="UTC"),
    description="DE_Zoomcamp Capstone Project Pipeline by Andy Nelson",
    catchup=False,
    tags=["USGS_FDSN_EARTHQUAKES"],
)

def usgs_earthquake_pipeline_v3():
    @task()
    def python_requests_api(api_url):
        json_data = requests.get(api_url).json()
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
        df.to_parquet(f'{local_parquet}')
        local_parquet_save = local_parquet
        return local_parquet_save
    @task()
    def upload_to_gcs(BUCKET, local_parquet_save, destination_blob_parquet):
        client = storage.Client(credentials=SERVICE_ACCOUNT_CREDENTIALS)
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(destination_blob_parquet)
        blob.upload_from_filename(local_parquet_save)
        return destination_blob_parquet
    @task()
    def gcs_2_bigquery_external(destination_blob_parquet):
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/{destination_blob_parquet}"],
            },
            },
        )
        return bigquery_external_table_task
    
    api_data = python_requests_api(api_url)
    exported_list = nested_json_to_list(api_data)
    local_file_sent =  list_to_df_to_parquet_to_local(exported_list)
    uploaded_parquet = upload_to_gcs(BUCKET, local_file_sent, destination_blob_parquet)
    gcs_2_bigquery_external(uploaded_parquet)
usgs_earthquake_pipeline_v3()
