from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
def committee_types_ingestion():
    run_date = pendulum.now().to_date_string()
    final_path = "./file_store/committee_types/final/"

    @task
    def begin():
        EmptyOperator(task_id="begin")
    
    @task
    def create_staging_folders():
        if not os.path.exists(final_path):
            os.makedirs(final_path)

    @task
    def scrape_data():
        committee_types_url = "https://www.fec.gov/campaign-finance-data/committee-type-code-descriptions/"
        committee_types_req = requests.get(committee_types_url)
        commmittee_types_page = io.StringIO(committee_types_req.text)
        committee_types_df = pd.read_html(commmittee_types_page, flavor="lxml", header=0)[0]

        committee_types_path = final_path + f"{run_date}_committee_types.csv"
        committee_types_df.to_csv(committee_types_path, sep="|", index=False)

    @task
    def upload_to_S3():
        hook = S3Hook(aws_conn_id='aws_conn')
        local_path = final_path + f"{run_date}_committee_types.csv"
        hook.load_file(filename=local_path, key=f"s3://fec-data/committee_types/{run_date}_committee_types.csv")

    @task
    def clean_up():
        shutil.rmtree(final_path)
        shutil.rmtree("./file_store/committee_types/")

    trigger_snowflake_copy = TriggerDagRunOperator(
        task_id="trigger_snowflake_copy",
        trigger_dag_id="copy_committee_types_table",
        conf= {"run_date": run_date},
        wait_for_completion= True
    )


    @task
    def end():
        EmptyOperator(task_id="end")

    begin() >> create_staging_folders() >> scrape_data() >> upload_to_S3() >> clean_up() >> trigger_snowflake_copy >> end()
committee_types_ingestion()