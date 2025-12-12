from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime
import pandas as pd

# Extract function
def extract_postgres_to_csv():
    hook = PostgresHook(postgres_conn_id="local_postgres")
    engine = hook.get_sqlalchemy_engine()

    query = "SELECT * FROM iris WHERE species = 'setosa';"
    df = pd.read_sql(query, engine)
    print(df)
    output_path = "file:///Users/dhandapanidhandapaniyedappalli/Downloads/airflow/iris.csv"  # update path
    df.to_csv(output_path, index=False)
    print(f"Extracted data written to {output_path}")

# Define DAG
with DAG(
    dag_id="etl_postgres_spark",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgres", "spark", "etl"]
) as dag:

    # Task 1: Extract from Postgres
    extract_task = PythonOperator(
        task_id="extract_postgres",
        python_callable=extract_postgres_to_csv
    )

    # Task 2: Run Spark job
    spark_task = SparkSubmitOperator(
        task_id="transform_spark",
        application="/Users/dhandapanidhandapaniyedappalli/PythonTutorial/pythonProject/.venv/Airflow/spark_job1.py",
        conn_id="spark_default",  # configure Spark connection in Airflow
        executor_memory="2g",
        num_executors=2
    )

    extract_task >> spark_task
