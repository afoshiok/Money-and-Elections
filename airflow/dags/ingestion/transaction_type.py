from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.hooks.S3_hook import S3Hook

import pendulum
from pendulum import datetime, duration 
import requests
import os
import shutil 
import pandas as pd
import io

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
def transaction_types_ingestion():
    run_date = pendulum.now().to_date_string()
    final_path = "./file_store/transaction_types/final/"

    @task
    def begin():
        EmptyOperator(task_id="begin")
    
    @task
    def create_staging_folders():
        if not os.path.exists(final_path):
            os.makedirs(final_path)

    @task
    def scrape_data():
        trans_type_url = "https://www.fec.gov/campaign-finance-data/report-type-code-descriptions/"
        trans_type_req = requests.get(trans_type_url)
        trans_type_page = io.StringIO(trans_type_req.text)
        trans_type_df = pd.read_html(trans_type_page, flavor="lxml", header=0)[0]

        trans_type_path = final_path + f"{run_date}_trans_types.csv"
        trans_type_df.to_csv(trans_type_path, sep=",", index=False)

    @task
    def upload_to_S3():
        hook = S3Hook(aws_conn_id='aws_conn')
        local_path = final_path + f"{run_date}_trans_types.csv"
        hook.load_file(filename=local_path, key=f"s3://fec-data/transaction_types/{run_date}_trans_types.csv")

    @task
    def clean_up():
        shutil.rmtree(final_path)
        shutil.rmtree("./file_store/transaction_types/")

    @task
    def end():
        EmptyOperator(task_id="end")

    begin() >> create_staging_folders() >> scrape_data() >> upload_to_S3() >> clean_up() >> end()
transaction_types_ingestion()