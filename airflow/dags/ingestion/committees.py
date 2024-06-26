from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
def committee_ingestion():
    """
    This DAG downloads the Committees bulk data from the FEC website, unzips the folder, does some minor processing and places the 
    final .csv in a S3 bucket. 
    
    """
    run_date = pendulum.now().to_date_string()
    zip_path = "./file_store/committees/zipped/"
    unzipped_path = "./file_store/committees/unzipped/"
    final_path = "./file_store/committees/final/"

    @task #Example of TaskFlow use, here's the docs: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html
    def begin():
        EmptyOperator(task_id="begin")
    @task
    def download_header_file():
        if not os.path.exists(zip_path):
            os.makedirs(zip_path)

        if not os.path.exists(unzipped_path):
            os.makedirs(unzipped_path)

        if not os.path.exists(final_path):
            os.makedirs(final_path)

        
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
    def extract_files():
        committees_zip_path = zip_path + f"{run_date}_cm24.zip"
        extract_output = unzipped_path + f"{run_date}_cm24/"
        with zipfile.ZipFile(committees_zip_path) as extracted_cm24:
            extracted_cm24.extractall(extract_output)

    @task
    def process_data():
        header_file = unzipped_path + f"{run_date}_committees_headers.csv"
        committee_file = unzipped_path + f"{run_date}_cm24/" + "cm.txt"

        committees_schema = {
            'CMTE_ID': pl.String,
            'CMTE_NM': pl.String,
            'TRESNAM': pl.String,
            'CMTE_ST1': pl.String,
            'CMTE_ST2': pl.String,
            'CMTE_CITY': pl.String,
            'CMTE_ST': pl.String,
            'CMTE_ZIP': pl.String,
            'CMTE_DSGN': pl.String,
            'CMTE_TP': pl.String,
            'CMTE_PTY_AFFILIATION': pl.String,
            'CMTE_FILING_FREQ': pl.String,
            'ORG_TP': pl.String,
            'CONNECTED_ORG_NM': pl.String,
            'CAND_ID': pl.String
        }
        #Adding headers to Committe data
        committee_header = pl.read_csv(source=header_file, has_header=True)
        committee_df = pl.read_csv(source=committee_file, has_header=False, separator="|", new_columns=committee_header.columns, schema=committees_schema)
        export_path = final_path + f"{run_date}_cm24.parquet"

        committee_df.write_parquet(export_path)

    @task
    def upload_to_S3():
        hook = S3Hook(aws_conn_id='aws_conn')
        local_path = final_path + f"{run_date}_cm24.parquet"
        hook.load_file(filename=local_path, key=f"s3://fec-data/committees/{run_date}_committees.parquet")

    @task
    def end():
        EmptyOperator(task_id="end")

    trigger_snowflake_copy = TriggerDagRunOperator(
        task_id="trigger_snowflake_copy",
        trigger_dag_id="copy_committees_table",
        conf= {"run_date": run_date},
        wait_for_completion= True
    )

    @task
    def clean_up():
        shutil.rmtree(zip_path)
        shutil.rmtree(unzipped_path)
        shutil.rmtree(final_path)
        shutil.rmtree("./file_store/committees/")

    begin() >> [ download_header_file() , download_zipped_file() ] >> extract_files() >> process_data() >> upload_to_S3() >> clean_up() >> trigger_snowflake_copy >> end()

committee_ingestion()