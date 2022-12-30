import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

PRIMARY_TOKEN = os.environ.get("PRIMARY_TOKEN")

default_args = {
    'owner': 'Andy Nelson',
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'python_requests_v1',
    default_args=default_args,
    schedule_interval=timedelta(daily),
)

endpoint = 'https://gorest.co.in/public/v2/users'
headers = {"Authorization": f"Bearer {PRIMARY_TOKEN}", "Accept:application/json", "Content-Type:application/json"}
r = requests.get(endpoint)

@task(task_id="make_api_call")
def get_requests(endpoint,headers):
    return requests.get(endpoint,headers)

run_this = get_requests()
