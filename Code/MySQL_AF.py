from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import pandas as pd
from airflow.hooks.mysql_hook import MySqlHook

def extract():
    hook = MySqlHook(mysql_conn_id='mysql')
    df = pd.read_sql("SELECT * FROM iris WHERE species='setosa'", hook.get_sqlalchemy_engine())
    df.to_csv('/airflow/data/iris.csv', index=False)

with DAG('etl_spark', start_date=datetime(2025,1,1), schedule_interval=None) as dag:
    t1 = PythonOperator(task_id='extract', python_callable=extract)
    t2 = SparkSubmitOperator(task_id='transform',
                             application='/airflow/jobs/spark.py',
                             conn_id='spark',
                             conf={
                               "spark.eventLog.enabled": True,
                               "spark.eventLog.dir": "/airflow/spark_logs"
                             })
    t1 >> t2
