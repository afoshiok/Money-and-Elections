from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.hooks.S3_hook import S3Hook

import pendulum
from pendulum import datetime, duration 
import requests
import os
import shutil 
import zipfile
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
def candidate_ingestion():
    run_date = pendulum.now().to_date_string()
    zip_path = "./file_store/candidates/zipped/"
    unzipped_path = "./file_store/candidates/unzipped/"
    final_path = "./file_store/candidates/final/"

    @task
    def begin():
        EmptyOperator(task_id="begin")
    
    @task
    def create_staging_folders():
        if not os.path.exists(zip_path):
            os.makedirs(zip_path)

        if not os.path.exists(unzipped_path):
            os.makedirs(unzipped_path)

        if not os.path.exists(final_path):
            os.makedirs(final_path)
    
    @task
    def download_header_file():
        header_url = "https://www.fec.gov/files/bulk-downloads/data_dictionaries/cn_header_file.csv"
        header_req = requests.get(header_url)

        header_dir = unzipped_path + f"{run_date}_candidates_header.csv"
        with open(header_dir, "wb") as candidates_header:
            candidates_header.write(header_req._content)

    @task()
    def download_zipped_file():
        candidate_url = "https://www.fec.gov/files/bulk-downloads/2024/cn24.zip"
        candidate_req = requests.get(candidate_url)
        candidate_dir = zip_path + f"{run_date}_candidates.zip"

        with open(candidate_dir, "wb") as candidate_zip:
            candidate_zip.write(candidate_req.content)

    begin() >> create_staging_folders() >> [ download_header_file(), download_zipped_file() ] 

candidate_ingestion()