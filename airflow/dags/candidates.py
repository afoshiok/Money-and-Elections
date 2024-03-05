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

    @task
    def download_zipped_file():
        candidate_url = "https://www.fec.gov/files/bulk-downloads/2024/cn24.zip"
        candidate_req = requests.get(candidate_url)
        candidate_dir = zip_path + f"{run_date}_candidates.zip"

        with open(candidate_dir, "wb") as candidate_zip:
            candidate_zip.write(candidate_req.content)

    @task
    def extract_files():
        candidate_zip_path = zip_path + f"{run_date}_candidates.zip"
        source = "cn.txt"
        extract_path = unzipped_path + f"{run_date}_candidates.csv"
        with zipfile.ZipFile(candidate_zip_path, "r") as file:
            file.getinfo(source).filename = extract_path
            file.extract(source)

    @task
    def process_data():
        header_file = unzipped_path + f"{run_date}_candidates_header.csv"
        candidate_file = unzipped_path + f"{run_date}_candidates.csv"

        candidate_header = pd.read_csv(header_file)
        candidate_df = pd.read_csv(candidate_file, sep="|", names=candidate_header.columns, dtype={'CAND_OFFICE_DISTRICT': "Int64", "CAND_ZIP" : "Int64"}) #This fixes the issue where pandas with convert Int columns with NaN to float 

        export_path = final_path + f"{run_date}_candidates.csv"

        candidate_df.to_csv(export_path, sep=",", index=False)

    @task
    def upload_to_S3():
        hook = S3Hook(aws_conn_id='aws_conn')
        local_path = final_path + f"{run_date}_candidates.csv"
        hook.load_file(filename=local_path, key=f"s3://fec-data/candidates/{run_date}_committees.csv")
    
    @task
    def clean_up():
        shutil.rmtree(zip_path)
        shutil.rmtree(unzipped_path)
        shutil.rmtree(final_path)
        shutil.rmtree("./file_store/candidates/")

    @task
    def end():
        EmptyOperator(task_id="end")

    begin() >> create_staging_folders() >> [ download_header_file(), download_zipped_file() ] >> extract_files() >> process_data() >> upload_to_S3() >> clean_up() >> end()

candidate_ingestion()