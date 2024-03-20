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
    start_date=datetime(2024, 1, 1), schedule=None, default_args=default_args
)
def individual_cont_ingestion():
    run_date = pendulum.now().to_date_string()
    zip_path = "./file_store/indiv_cont/zipped/"
    unzipped_path = "./file_store/indiv_cont/unzipped/"
    final_path = "./file_store/indiv_cont/final/"

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
        header_url = "https://www.fec.gov/files/bulk-downloads/data_dictionaries/indiv_header_file.csv"
        header_req = requests.get(header_url)

        header_dir = unzipped_path + f"{run_date}_indiv_cont_headers.csv"
        with open(header_dir, "wb") as indiv_cont_header:
            indiv_cont_header.write(header_req.content)

    @task
    def download_zipped_file():
        indiv_cont_url = "https://www.fec.gov/files/bulk-downloads/2024/indiv24.zip"
        indiv_cont_req = requests.get(indiv_cont_url)
        indiv_cont_dir = zip_path + f"{run_date}_ic24.zip"

        with open(indiv_cont_dir, "wb") as indiv_cont_file:
            indiv_cont_file.write(indiv_cont_req.content)

    @task
    def extract_files():
        indiv_cont_zip_path = zip_path + f"{run_date}_ic24.zip"
        extract_output = unzipped_path
        with zipfile.ZipFile(indiv_cont_zip_path, "r") as extract_ic:
            extract_ic.extract("itcont.txt", path=extract_output)

    @task
    def process_data():
        header_file = unzipped_path + f"{run_date}_indiv_cont_headers.csv"
        indiv_cont_file = unzipped_path + "itcont.txt"

        indiv_cont_header = pd.read_csv(header_file)
        indiv_cont_df = pd.read_csv(indiv_cont_file, sep="|", names=indiv_cont_header.columns)

        try:
            indiv_cont_df["TRANSACTION_DT"] = pd.to_datetime(indiv_cont_df["TRANSACTION_DT"], format="%m%d%Y", errors='coerce')
        except ValueError:
            pass
        
        export_path = final_path + f"{run_date}_indiv_cont.csv"

        indiv_cont_df.to_csv(export_path, sep="|", index=False) #Changed delim to be as unqiue as possible to fix MERGE issue in db.

    @task
    def upload_to_S3():
        hook = S3Hook(aws_conn_id='aws_conn')
        local_path = final_path + f"{run_date}_indiv_cont.csv"
        s3_key = f"s3://fec-data/individual_contributions/{run_date}_indiv_cont.csv" 
        hook.load_file(filename=local_path, key=s3_key)

    @task
    def clean_up():
        shutil.rmtree(zip_path)
        shutil.rmtree(unzipped_path)
        shutil.rmtree(final_path)
        shutil.rmtree("./file_store/indiv_cont/")

    @task
    def end():
        EmptyOperator(task_id="end")
    
    begin() >> create_staging_folders() >> [ download_header_file(), download_zipped_file() ] >> extract_files() >> process_data() >> upload_to_S3() >> clean_up() >> end()

individual_cont_ingestion()