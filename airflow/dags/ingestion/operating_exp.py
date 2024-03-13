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
def operating_exp_ingestion():
    run_date = pendulum.now().to_date_string()
    zip_path = "./file_store/operating_exp/zipped/"
    unzipped_path = "./file_store/operating_exp/unzipped/"
    final_path = "./file_store/operating_exp/final/"

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
        header_url = "https://www.fec.gov/files/bulk-downloads/data_dictionaries/oppexp_header_file.csv"
        header_req = requests.get(header_url)

        header_dir = unzipped_path + f"{run_date}_operating_exp_header.csv"
        with open(header_dir, "wb") as operating_header:
            operating_header.write(header_req.content)

    @task
    def download_zip_files():
        operating_exp_url = "https://www.fec.gov/files/bulk-downloads/2024/oppexp24.zip"
        operating_exp_req = requests.get(operating_exp_url)
        operating_exp_dir = zip_path + f"{run_date}_operating_exp.zip"

        with open(operating_exp_dir, "wb") as operating_exp_zip:
            operating_exp_zip.write(operating_exp_req.content)

    @task
    def extract_files():
        operating_exp_zip_path = zip_path + f"{run_date}_operating_exp.zip"
        source = "oppexp.txt"
        extract_path = unzipped_path + f"{run_date}_operating_exp.csv"

        with zipfile.ZipFile(operating_exp_zip_path, "r") as file:
            file.getinfo(source).filename = extract_path
            file.extract(source)

    @task
    def process_data():
        header_file = unzipped_path + f"{run_date}_operating_exp_header.csv"
        operating_exp_file = unzipped_path + f"{run_date}_operating_exp.csv"

        operating_exp_header = pd.read_csv(header_file)

        columns = operating_exp_header.columns.tolist()
        operating_exp_df = pd.read_csv(operating_exp_file, sep="|", header=None, dtype={16: "object"}) #Won't add a header yet, need to drop an extra column.
        operating_exp_df = operating_exp_df.drop(operating_exp_df.columns[-1], axis=1) #Removing the extra column Pandas added
        operating_exp_df = operating_exp_df.set_axis(columns, axis=1)
        
        export_path = final_path + f"{run_date}_operating_exp.csv"
        operating_exp_df.to_csv(export_path, sep="|", index=False)

    @task
    def upload_to_S3():
        hook = S3Hook(aws_conn_id = 'aws_conn')
        local_path = final_path + f"{run_date}_operating_exp.csv"
        hook.load_file(filename=local_path, key=f"s3://fec-data/operating_expenditures/{run_date}_operating_exp.csv")

    @task
    def clean_up():
        shutil.rmtree(zip_path)
        shutil.rmtree(unzipped_path)
        shutil.rmtree(final_path)
        shutil.rmtree("./file_store/operating_exp/")

    @task
    def end():
        EmptyOperator(task_id="end")

    begin() >> create_staging_folders() >> download_header_file() >> download_zip_files() >> extract_files() >> process_data() >> upload_to_S3() >> clean_up() >> end()

operating_exp_ingestion()