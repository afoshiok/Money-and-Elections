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

        operating_exp_schema = {
            'CMTE_ID': pl.String,
            'AMNDT_IND': pl.String,
            'RPT_YR': pl.Int16,
            'RPT_TP': pl.String,
            'IMAGE_NUM': pl.String,
            'LINE_NUM': pl.String,
            'FORM_TP_CD': pl.String,
            'SCHED_TP_CD': pl.String,
            'NAME': pl.String,
            'CITY': pl.String,
            'STATE': pl.String,
            'ZIP_CODE': pl.String,
            'TRANSACTION_DT': pl.String,
            'TRANSACTION_AMT': pl.Float32,
            'TRANSACTION_PGI': pl.String,
            'PURPOSE': pl.String,
            'CATEGORY': pl.String,
            'CATEGORY_DESC': pl.String,
            'MEMO_CD': pl.String,
            'MEMO_TEXT': pl.String,
            'ENTITY_TP': pl.String,
            'SUB_ID': pl.Int64,
            'FILE_NUM': pl.Int64,
            'TRAN_ID': pl.String,
            'BACK_REF_TRAN_ID': pl.String
        }

        operating_exp_header = pl.read_csv(source=header_file, has_header=True)

        operating_exp_df = pl.read_csv(source=operating_exp_file, separator="|", ignore_errors=True, has_header=False) #Won't add a header yet, need to drop an extra column.
        operating_exp_df = operating_exp_df.drop(operating_exp_df.columns[-1]) #Dropping the extra column added to this df
        current_columns = operating_exp_df.columns #List of current column names ("column_1", "column_2", etc.)
        new_columns = operating_exp_header.columns

        column_rename = dict(zip(current_columns,new_columns)) #Creating a dict of old:new names
        operating_exp_df = operating_exp_df.rename(column_rename) #Equivalent to set_axis in Pandas

        operating_exp_df = operating_exp_df.cast(operating_exp_schema)# Finally adding the schema to the df

        final_df = operating_exp_df.with_columns(
            pl.col('TRANSACTION_DT').str.to_date(format='%m/%d/%Y')
        )
        
        export_path = final_path + f"{run_date}_operating_exp.parquet"
        final_df.write_parquet(export_path)

    @task
    def upload_to_S3():
        hook = S3Hook(aws_conn_id = 'aws_conn')
        local_path = final_path + f"{run_date}_operating_exp.parquet"
        hook.load_file(filename=local_path, key=f"s3://fec-data/operating_expenditures/{run_date}_operating_exp.parquet")

    @task
    def clean_up():
        shutil.rmtree(zip_path)
        shutil.rmtree(unzipped_path)
        shutil.rmtree(final_path)
        shutil.rmtree("./file_store/operating_exp/")

    trigger_snowflake_copy = TriggerDagRunOperator(
        task_id="trigger_snowflake_copy",
        trigger_dag_id="copy_operating_expenditures",
        conf= {"run_date": run_date},
        wait_for_completion= True
    )

    @task
    def end():
        EmptyOperator(task_id="end")

    begin() >> create_staging_folders() >> download_header_file() >> download_zip_files() >> extract_files() >> process_data() >> upload_to_S3() >> clean_up() >> trigger_snowflake_copy >> end()

operating_exp_ingestion()