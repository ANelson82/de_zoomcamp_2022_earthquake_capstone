# import datetime
# import json 
# import os 
# import pandas as pd
# import pendulum
# import requests

# from airflow import DAG
# from airflow.decorators import task
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# from google.cloud import storage
# from google.oauth2 import service_account

# AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
# GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# BUCKET = os.environ.get("GCP_GCS_BUCKET")
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'raw_usgs_earthquake')
# BIGQUERY_TABLE = 'raw_earthquake_ingestion'
# CREDENTIALS_DIR = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
# SERVICE_ACCOUNT_CREDENTIALS = service_account.Credentials.from_service_account_file(f"{CREDENTIALS_DIR}")

# DAY_BEFORE_YESTERDAY = '{{ macros.ds_add(ds, -2) }}'
# YESTERDAY = '{{ macros.ds_add(ds, -1) }}'
# API_URL = f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={DAY_BEFORE_YESTERDAY}&endtime={YESTERDAY}'
# LOCAL_JSON = f'{AIRFLOW_HOME}/{DAY_BEFORE_YESTERDAY}.json'
# LOCAL_PARQUET = f'{AIRFLOW_HOME}/{DAY_BEFORE_YESTERDAY}.parquet'
# DESTINATION_BLOB_JSON = f"usgs_raw_json/data_{DAY_BEFORE_YESTERDAY}.json"
# DESTINATION_BLOB_PARQUET = f"usgs_raw_parquet/data_{DAY_BEFORE_YESTERDAY}.parquet"

# # BQ_LOAD_DATA_QUERY = (
# #                 f"LOAD DATA INTO `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}` \
# #                 FROM FILES( \
# #                     format='PARQUET', \
# #                     uris = ['gs://{BUCKET}/{DESTINATION_BLOB_PARQUET}'] \
# #                 )"
# #             )

# # def nested_json_to_df(LOCAL_JSON):
# #     with open(f"{LOCAL_JSON}", 'r') as j:
# #         json_data_local = json.loads(j.read())
# #     df = pd.json_normalize(json_data_local, record_path=['features'], meta='metadata')
# #     df_meta = pd.json_normalize(df['metadata'])
# #     df = pd.concat([df, df_meta], axis=1, join="inner")
# #     df[['geometry.coordinates.longitude', 'geometry.coordinates.latitude', 'geometry.coordinates.depth']] = df['geometry.coordinates'].tolist()
# #     df.drop(['metadata', 'geometry.coordinates'], axis=1, inplace=True)
# #     df['properties.time.datetime'] = pd.to_datetime(df['properties.time'], unit='ms')
# #     df['properties.updated.datetime'] = pd.to_datetime(df['properties.updated'], unit='ms')
# #     df['metadata.generated.datetime'] = pd.to_datetime(df['generated'], unit='ms')
# #     return df

# # def list_to_df_to_parquet_to_local(df):
# #     df.to_parquet(f'{LOCAL_PARQUET}', use_deprecated_int96_timestamps=True)
# #     return LOCAL_PARQUET

# # def upload_to_gcs(BUCKET, LOCAL_PARQUET, DESTINATION_BLOB_PARQUET):
#     client = storage.Client(credentials=SERVICE_ACCOUNT_CREDENTIALS)
#     bucket = client.bucket(BUCKET)
#     blob = bucket.blob(DESTINATION_BLOB_PARQUET)
#     blob.upload_from_filename(LOCAL_PARQUET)
#     return DESTINATION_BLOB_PARQUET
    

# with DAG(
#     dag_id='usgs_earthquake_pipeline_v19',
#     schedule="@daily",
#     default_args={
#         "depends_on_past": False,
#         "retries": 1,
#         "retry_delay": datetime.timedelta(minutes=1),
#     },
#     start_date=pendulum.datetime(2022, 12, 25, tz="UTC"),
#     description="DE_Zoomcamp Capstone Project Pipeline by Andy Nelson",
#     catchup=False,
#     max_active_runs=1,
#     tags=["USGS_EARTHQUAKES"],
# ) as dag:

#     @task()
#     def python_requests_api(API_URL):
#         json_data = requests.get(API_URL).json()
#         return json_data
#     @task()
#     def save_json_data_to_local(json_data, LOCAL_JSON):
#         with open(f"{LOCAL_JSON}", "w") as outfile:
#             json.dump(json_data, outfile)
#         return LOCAL_JSON
#     @task()
#     def upload_json_data_to_gcs(BUCKET, LOCAL_JSON, DESTINATION_BLOB_JSON):
#         client = storage.Client(credentials=SERVICE_ACCOUNT_CREDENTIALS)
#         bucket = client.bucket(BUCKET)
#         blob = bucket.blob(DESTINATION_BLOB_JSON)
#         blob.upload_from_filename(LOCAL_JSON)
#         return DESTINATION_BLOB_JSON

#     api_data = python_requests_api(API_URL)
#     exported_list = nested_json_to_list(api_data)
#     local_file_sent =  list_to_df_to_parquet_to_local(exported_list)

#     # upload_json_data_to_gcs_task = PythonOperator(
#     #     task_id='upload_json_data_to_gcs_task',
#     #     python_callable =upload_json_data_to_gcs,
#     # )

  
#     # @task()
#     # def delete_local_json(LOCAL_JSON):
#     #     myfile = f"{LOCAL_JSON}"
#     #     if os.path.isfile(myfile):
#     #         os.remove(myfile)
#     #     else:
#     #         print("Error: %s file not found" % myfile)    
#     # @task()
#     # def delete_local_parquet(LOCAL_PARQUET_SAVE):
#     #     myfile = f"{LOCAL_PARQUET_SAVE}"
#     #     if os.path.isfile(myfile):
#     #         os.remove(myfile)
#     #     else:
#     #         print("Error: %s file not found" % myfile)
    
#     # bq_load_data = BigQueryInsertJobOperator(
#     # task_id="bigquery_load_data_task",
#     # configuration={
#     #     "query": {
#     #         "query": BQ_LOAD_DATA_QUERY,
#     #         "useLegacySql": False,
#     #         }
#     #     }
#     # )