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
def committee_transactions_ingestion():
    run_date = pendulum.now().to_date_string()
    zip_path = "./file_store/committee_transactions/zipped/"
    unzipped_path = "./file_store/committee_transactions/unzipped/"
    final_path = "./file_store/committee_transactions/final/"

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
        header_url = "https://www.fec.gov/files/bulk-downloads/data_dictionaries/oth_header_file.csv"
        header_req = requests.get(header_url)

        header_dir = unzipped_path + f"{run_date}_committee_trans_header.csv"
        with open(header_dir, "wb") as committee_trans_header:
            committee_trans_header.write(header_req.content)

    @task
    def download_zipped_file():
        committee_trans_url = "https://www.fec.gov/files/bulk-downloads/2024/oth24.zip"
        committee_trans_req = requests.get(committee_trans_url)
        committee_trans_dir = zip_path + f"{run_date}_committee_transactions.zip"

        with open(committee_trans_dir, "wb") as committee_trans_file:
            committee_trans_file.write(committee_trans_req.content)

    @task
    def extract_files():
        committee_trans_zip_path = zip_path + f"{run_date}_committee_transactions.zip"
        source = "itoth.txt"
        extract_path = unzipped_path + f"{run_date}_committee_transactions.csv"

        with zipfile.ZipFile(committee_trans_zip_path, "r") as file:
            file.getinfo(source).filename = extract_path
            file.extract(source)

    @task
    def process_data():
        header_file = unzipped_path + f"{run_date}_committee_trans_header.csv"
        transaction_file = unzipped_path + f"{run_date}_committee_transactions.csv"

        headers = pd.read_csv(header_file)
        columns = headers.columns.to_list()
        committee_trans_df = pd.read_csv(transaction_file, sep="|", names=columns, dtype={"TRANSACTION_DT": "object", "ZIP_CODE": "object" })

        try:
            committee_trans_df["TRANSACTION_DT"] = pd.to_datetime(committee_trans_df["TRANSACTION_DT"], format="%m%d%Y", errors='coerce')
        except ValueError:
            pass

        export_path = final_path + f"{run_date}_committee_transactions.csv"
        committee_trans_df.to_csv(export_path, sep=",", index=False)
    
    @task
    def upload_to_S3():
        hook = S3Hook(aws_conn_id = 'aws_conn')
        local_path = final_path + f"{run_date}_committee_transactions.csv"
        hook.load_file(filename=local_path, key=f"s3://fec-data/committee_transactions/{run_date}_committee_transactions.csv")

    @task
    def clean_up():
        shutil.rmtree(zip_path)
        shutil.rmtree(unzipped_path)
        shutil.rmtree(final_path)
        shutil.rmtree("./file_store/committee_transactions/")
    
    @task
    def end():
        EmptyOperator(task_id="end")

    
    begin() >> create_staging_folders() >>  download_header_file() >> download_zipped_file() >> extract_files() >> process_data() >> upload_to_S3() >> clean_up() >> end()
committee_transactions_ingestion()