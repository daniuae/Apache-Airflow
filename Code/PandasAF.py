import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import PythonOperator


def transform_data_callable(raw_data):
    # Transform response to a list
    transformed_data = [
        [
            raw_data.get("date"),
            raw_data.get("location"),
            raw_data.get("weather").get("temp"),
            raw_data.get("weather").get("conditions")
        ]
    ]
    return transformed_data


transform_data = PythonOperator(
    dag="mydag_trans",
    task_id="transform_data",
    python_callable=transform_data_callable,
    op_kwargs={"raw_data": "{{ ti.xcom_pull(task_ids='extract_data') }}"}
)

def load_data_callable(transformed_data):
    # Load the data to a DataFrame, set the columns
    loaded_data = pd.DataFrame(transformed_data)
    loaded_data.columns = [
        "date",
        "location",
        "weather_temp",
        "weather_conditions"
    ]
    print(loaded_data)


load_data = PythonOperator(
    dag="my_dag_id",
    task_id="load_data",
    python_callable=load_data_callable,
    op_kwargs={"transformed_data": "{{ ti.xcom_pull(task_ids='transform_data') }}"}
)


