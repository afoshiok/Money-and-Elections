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
def political_parties_ingestion():
    run_date = pendulum.now().to_date_string()
    zip_path = "./file_store/political_parties/zipped/"
    unzipped_path = "./file_store/political_parties/unzipped/"
    final_path = "./file_store/political_parties/final/"

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
    def scrape_data():
        pp_url = "https://www.fec.gov/campaign-finance-data/party-code-descriptions/"
        pp_req = requests.get(pp_url)
        pp_page = io.StringIO(pp_req.text)
        pp_df = pd.read_html(pp_page, flavor="lxml", header=0)[0]

        pp_csv_path = final_path + f"{run_date}_political_parties.csv"
        pp_df.to_csv(pp_csv_path, sep="|", index=False)

    @task
    def upload_to_S3():
        hook = S3Hook(aws_conn_id='aws_conn')
        local_path = final_path + f"{run_date}_political_parties.csv"
        hook.load_file(filename=local_path, key=f"s3://fec-data/political_parties/{run_date}_political_parties.csv")

    @task
    def clean_up():
        shutil.rmtree(zip_path)
        shutil.rmtree(unzipped_path)
        shutil.rmtree(final_path)
        shutil.rmtree("./file_store/political_parties/")
    
    @task
    def end():
        EmptyOperator(task_id="end")

    begin() >> create_staging_folders() >> scrape_data() >> upload_to_S3() >> clean_up() >> end()
political_parties_ingestion()