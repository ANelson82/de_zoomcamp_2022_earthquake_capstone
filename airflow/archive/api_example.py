import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.simple_http_operator import SimpleHttpOperator
# import gcs

start_date = x 
end_date = y
format = geojson
api = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format={format}&starttime={start_date}&endtime={end_date}

default_args = {
    'owner': 'Andy Nelson',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    'my_dag_id',
    default_args=default_args,
    schedule_interval=timedelta(daily),
)

#todo: Set up the connection to Google Cloud Storage

# Set up the SimpleHttpOperator to make the API request
api_endpoint = 'https://api.example.com/endpoint'

response = SimpleHttpOperator(
    task_id='make_request',
    method='GET',
    endpoint=api_endpoint,
    dag=dag,
)

# Set up the GoogleCloudStorageToGoogleCloudStorageOperator to upload the response to GCS
upload_to_gcs = GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id='upload_to_gcs',
    source_bucket='source_bucket',
    source_object='response.json',
    destination_bucket='destination_bucket',
    destination_object='response.json',
    google_cloud_storage_conn_id=gcs_conn_id,
    dag=dag,
)

# Set up the dependencies between the tasks
response >> upload_to_gcs