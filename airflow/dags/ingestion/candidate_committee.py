from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.hooks.S3_hook import S3Hook

import pendulum
from pendulum import datetime, duration 
import requests
import os
import shutil 
import zipfile
import polars as pl

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
def candidate_committee_ingestion():
    run_date = pendulum.now().to_date_string()
    zip_path = "./file_store/candidate_committee/zipped/"
    unzipped_path = "./file_store/candidate_committee/unzipped/"
    final_path = "./file_store/candidate_committee/final/"

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
        header_url = "https://www.fec.gov/files/bulk-downloads/data_dictionaries/ccl_header_file.csv"
        header_req = requests.get(header_url)

        header_dir = unzipped_path + f"{run_date}_ccl_header.csv"
        with open(header_dir, "wb") as header_file:
            header_file.write(header_req.content)
    
    @task
    def download_zipped_file():
        candidate_com_url = "https://www.fec.gov/files/bulk-downloads/2024/ccl24.zip"
        candidate_com_req = requests.get(candidate_com_url)
        candidate_com_dir = zip_path + f"{run_date}_candidate_committee.zip"

        with open(candidate_com_dir, "wb") as candidate_com_file:
            candidate_com_file.write(candidate_com_req.content)

    @task
    def extract_files():
        ccl_path = zip_path + f"{run_date}_candidate_committee.zip"
        source = "ccl.txt"
        extract_path = unzipped_path + f"{run_date}_ccl.csv"

        with zipfile.ZipFile(ccl_path, "r") as file:
            file.getinfo(source).filename = extract_path
            file.extract(source)
    @task
    def process_data():
        header_file =  unzipped_path + f"{run_date}_ccl_header.csv"
        candidate_committe_file = unzipped_path + f"{run_date}_ccl.csv"

        ccl_schema = {
            "CAND_ID": pl.String,
            "CAND_ELECTION_YR": pl.Int32,
            "FEC_ELECTION_YR": pl.Int32,
            "CMTE_ID": pl.String,
            "CMTE_TP": pl.String,
            "CMTE_DSGN": pl.String,
            "LINKAGE_ID": pl.Int32
        }

        ccl_header = pl.read_csv(source=header_file, has_header=True)
        ccl_df = pl.read_csv(source=candidate_committe_file, separator="|", new_columns=ccl_header.columns, schema=ccl_schema)


        export_path = final_path + f"{run_date}_ccl.csv"
        ccl_df.write_csv(export_path, separator="|")
    
    @task
    def upload_to_S3():
        hook = S3Hook(aws_conn_id="aws_conn")
        local_path = final_path + f"{run_date}_ccl.csv"
        hook.load_file(filename=local_path, key=f"s3://fec-data/candidate-committee_link/{run_date}_ccl.csv")

    @task
    def clean_up():
        shutil.rmtree(zip_path)
        shutil.rmtree(unzipped_path)
        shutil.rmtree(final_path)
        shutil.rmtree("./file_store/candidate_committee/")
    
    @task
    def end():
        EmptyOperator(task_id="end")

    begin() >> create_staging_folders() >> download_header_file() >> download_zipped_file()  >> extract_files() >> process_data() >> upload_to_S3() >> clean_up() >> end()

candidate_committee_ingestion()