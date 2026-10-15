from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args = {
    "owner": "quang_nguyen",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False
}

def extract():
    print("Extracting data...")

def transform():
    print("Transforming data...")

def load():
    print("Loading data...")

with DAG(
    dag_id="dummy_dag",
    schedule="@daily",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test"]
) as dag:
    extract_op = PythonOperator(
        task_id="extract_task",
        python_callable=extract
    )
    transform_op = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )
    load_op = PythonOperator(
        task_id="load_task",
        python_callable=load
    )

    extract_op >> transform_op >> load_op