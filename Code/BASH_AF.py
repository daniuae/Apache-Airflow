from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='example_dag',
    start_date=datetime(2023, 1, 1),
    schedule="None",
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date"
    )
    print("task1 completed ")
    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3
    )
    print("task1 completed ")
    t1 >> t2
