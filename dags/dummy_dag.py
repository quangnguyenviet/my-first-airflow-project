import requests
from airflow.models import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

download_url = r"https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
input_file = r"web-server-access-log.txt"
extracted_file = r"extracted-data.txt"
transformed_file = r"transformed-data.txt"
loaded_file = r"capitalized.txt"

def download_file():
    resp = requests.get(download_url)
    with open(input_file, 'wb') as f1:
        f1.write(resp.content)

def extract():
    print("Inside extract")
    with open(input_file, 'r') as infile, \
        open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split("#")
            field_1 = fields[0]
            field_4 = fields[3]
            outfile.write(field_1 + "#" + field_4 + "\n")

def transform():
    print("Inside transform")
    with open(extracted_file, 'r') as infile,\
        open(transformed_file, 'w') as outfile:
        for line in infile:
            fields = line.split("#")
            processed_visitorid = fields[1].upper()
            outfile.write(fields[0] + "#" + processed_visitorid)
        
def load():
    print("Inside load")
    with open(transformed_file, 'r') as infile,\
        open(loaded_file, 'w') as outfile:
        for line in infile:
            outfile.write(line + "\n")

default_args = {
    'owner': "Quang Nguyen",
    'start_date': datetime.now(),
    "email": ["blabla"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'etl-server-access-log-dag',
    default_args=default_args,
    description="My first dag",
    schedule=timedelta(days=1)
)

# Define the task named download to call the download_file function
download = PythonOperator(
    task_id = 'download',
    python_callable=download_file,
    dag = dag
)

# Define the task named extract to call the extract function
extract = PythonOperator(
    task_id = "extract",
    python_callable=extract,
    dag=dag
)

# Define the task named transform to call the transform function
transform = PythonOperator(
    task_id="transform",
    python_callable=transform,
    dag=dag
)

# Define the task named load to call the load function
load = PythonOperator(
    task_id = 'load',
    python_callable= load,
    dag=dag
)

# Task pipeline
download >> extract >> transform >> load
