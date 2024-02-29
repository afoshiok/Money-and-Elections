from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

import pendulum
from pendulum import datetime, duration 
import requests
import os
import pandas as pd

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=1),
}



@dag(
    start_date=datetime(2024, 1, 1), schedule="@once", default_args=default_args
)
def committee_ingestion():
    run_date = pendulum.now().to_date_string()
    zip_path = "./file_store/zipped/"
    unzipped_path = "./file_store/unzipped/"

    @task #Example of TaskFlow use, here's the docs: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html
    def begin():
        EmptyOperator(task_id="begin")
    @task
    def download_header_file():
        if not os.path.exists(zip_path):
            os.makedirs(zip_path)

        if not os.path.exists(unzipped_path):
            os.makedirs(unzipped_path)

        
        committees_header_url = "https://www.fec.gov/files/bulk-downloads/data_dictionaries/cm_header_file.csv"
        header_req = requests.get(committees_header_url)

        header_dir = unzipped_path + f"{run_date}_committees_headers.csv"

        with open(header_dir, "wb") as committee_header_file:
            committee_header_file.write(header_req.content)
        
        
    @task        
    def download_zipped_file():
        committees_url = "https://www.fec.gov/files/bulk-downloads/2024/cm24.zip"
        committees_req = requests.get(committees_url)

        committees_dir = zip_path + f"{run_date}_cm24.zip"
        with open(committees_dir, "wb") as committees_file:
            committees_file.write(committees_req.content)
        
    
    @task
    def clean_up():
        os.remove(zip_path + f"{run_date}_cm24.zip")
        os.remove(unzipped_path + f"{run_date}_committees_headers.csv")
        os.removedirs(zip_path)
        os.removedirs(unzipped_path)


    @task
    def end():
        EmptyOperator(task_id="end")

    begin() >> [ download_header_file() , download_zipped_file() ] >> clean_up() >> end()

committee_ingestion()