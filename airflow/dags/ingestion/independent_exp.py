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
def independent_exp_ingestion():
    run_date = pendulum.now().to_date_string()
    zip_path = "./file_store/independent_exp/zipped/"
    unzipped_path = "./file_store/independent_exp/unzipped/"
    final_path = "./file_store/independent_exp/final/"

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
        header_url = "https://www.fec.gov/files/bulk-downloads/data_dictionaries/pas2_header_file.csv"
        header_req = requests.get(header_url)

        header_dir = unzipped_path + f"{run_date}_independent_exp_header.csv"
        with open(header_dir, "wb") as independent_exp_header:
            independent_exp_header.write(header_req.content)
    
    @task
    def download_zipped_file():
        independent_exp_url = "https://www.fec.gov/files/bulk-downloads/2024/pas224.zip"
        ind_exp_req = requests.get(independent_exp_url)
        ind_exp_dir = zip_path + f"{run_date}_independent_exp.zip"

        with open(ind_exp_dir, "wb") as ind_exp_file:
            ind_exp_file.write(ind_exp_req._content)

    @task
    def extract_files():
        ind_exp_zip_path = zip_path + f"{run_date}_independent_exp.zip"
        source = "itpas2.txt"
        extract_path = unzipped_path +  f"{run_date}_independent_exp.csv"
        with zipfile.ZipFile(ind_exp_zip_path, "r") as file:
            file.getinfo(source).filename = extract_path
            file.extract(source)

    @task
    def process_data():
        header_file = unzipped_path + f"{run_date}_independent_exp_header.csv"
        independent_exp_file = unzipped_path +  f"{run_date}_independent_exp.csv"

        independent_exp_schema = {
            "CMTE_ID": pl.String,
            "AMNDT_IND": pl.String,
            "RPT_TP": pl.String,
            "TRANSACTION_PGI": pl.String,
            "IMAGE_NUM": pl.String,
            "TRANSACTION_TP": pl.String,
            "ENTITY_TP": pl.String,
            "NAME": pl.String,
            "CITY": pl.String,
            "STATE": pl.String,
            "ZIP_CODE": pl.String,
            "EMPLOYER": pl.String,
            "OCCUPATION": pl.String,
            "TRANSACTION_DT": pl.String, #Will be converted into a date later on
            "TRANSACTION_AMT": pl.Float32,
            "OTHER_ID": pl.String,
            "CAND_ID":pl.String,
            "TRAN_ID": pl.String,
            "FILE_NUM": pl.Int32,
            "MEMO_CD": pl.String,
            "MEMO_TEXT": pl.String,
            "SUB_ID": pl.String
        }

        ind_exp_header = pl.read_csv(source=header_file, has_header=True)
        ind_exp_df = pl.read_csv(source=independent_exp_file, has_header=False, separator="|", new_columns=ind_exp_header.columns, schema=independent_exp_schema)

        final_df = ind_exp_df.with_columns(
            pl.col('TRANSACTION_DT').str.to_date(format='%m%d%Y')
        )

        export_path = final_path + f"{run_date}_independent_exp.csv"
        final_df.write_csv(export_path, separator="|")

    @task
    def upload_to_S3():
        hook = S3Hook(aws_conn_id='aws_conn')
        local_path = final_path + f"{run_date}_independent_exp.csv"
        hook.load_file(filename=local_path, key=f"s3://fec-data/independent_expenditures/{run_date}_independent_exp.csv")

    @task
    def clean_up():
        shutil.rmtree(zip_path)
        shutil.rmtree(unzipped_path)
        shutil.rmtree(final_path)
        shutil.rmtree("./file_store/independent_exp/")
    
    @task
    def end():
        EmptyOperator(task_id="end")


    begin() >> create_staging_folders() >> download_header_file() >> download_zipped_file() >> extract_files() >> process_data() >> upload_to_S3() >> clean_up() >> end()

independent_exp_ingestion()