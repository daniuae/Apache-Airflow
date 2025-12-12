from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import json
import requests

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sample_dag',
    default_args=default_args,
    description='A simple sample DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example']
)

def extract_data_from_api():
    url = "https://api.example.com/data"
    response = requests.get(url)
    response.raise_for_status() # Raise an exception for bad status codes
    return response.json()

def transform_data(ti):
    data = ti.xcom_pull(task_ids='extract_data')
    transformed_data = {
        "count": len(data),
        "first_element": data[0] if data else None
    }
    return transformed_data

def load_data_to_file(ti):
    data = ti.xcom_pull(task_ids='transform_data')
    with open("/tmp/output.txt", "w") as f:
        json.dump(data, f)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_from_api,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_file,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
