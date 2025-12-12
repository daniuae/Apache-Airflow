from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import psycopg2

DATA_PATH = "/opt/airflow/dags/data.csv"

def extract():
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Ravi", "Priya", "Anita"],
        "age": [29, 34, 41]
    })
    df.to_csv(DATA_PATH, index=False)
    #df.show()
    #print("extract")
def transform():
    df = pd.read_csv(DATA_PATH)
    df["age"] = df["age"] + 1
    df.to_csv(DATA_PATH, index=False)
    #print("transform")
def load():
    conn = psycopg2.connect("dbname=airflow user=airflow password=airflow host=postgres")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS people (id INT, name TEXT, age INT);")
    df = pd.read_csv(DATA_PATH)
    #print("load")

    for _, row in df.iterrows():
        cur.execute("INSERT INTO people VALUES (%s, %s, %s)", (row.id, row.name, row.age))
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    "etl_pipeline",
    start_date=datetime(2025, 9, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load", python_callable=load)

    t1 >> t2 >> t3
