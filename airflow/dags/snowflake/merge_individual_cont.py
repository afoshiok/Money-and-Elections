from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration 
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=1),
}

merge_individual_cont = """
USE ELECTION.PUBLIC;
USE ROLE transform;

MERGE INTO election.raw.src_individual_contributions AS target
USING (
  SELECT DISTINCT
        $21 as "SUB_ID",
        $1 as "CMTE_ID",
        $2 as "AMNDT_IND",
        $3 as "RPT_TP",
        $4 as "TRANSACTION_PGI",
        $5 as "IMAGE_NUM",
        $6 as "TRANSACTION_TP",
        $7 as "ENTITY_TP",
        $8 as "NAME",
        $9 as "CITY",
        $10 as "STATE",
        $11 as "ZIP_CODE",
        $12 as "EMPLOYER",
        $13 as "OCCUPATION",
        $14 as "TRANSACTION_DT",
        TRY_CAST($15 AS INTEGER) as "TRANSACTION_AMT",
        $16 as "OTHER_ID",
        $17 as "TRAN_ID",
        $18 as "FILE_NUM",
        $19 as "MEMO_CD",
        $20 as "MEMO_TEXT"
  FROM @INDIVIDUAL_CONTRIBUTIONS_STAGE
  WHERE TRY_CAST($15 AS INTEGER) IS NOT NULL -- Filter out records where TRY_CAST fails
) AS source
ON target.sub_id = source.sub_id
WHEN MATCHED THEN
  UPDATE SET
    target.cmte_id = source.cmte_id,
    target.amndt_ind = source.amndt_ind,
    target.rpt_tp = source.rpt_tp,
    target.transaction_pgi = source.transaction_pgi,
    target.image_num = source.image_num,
    target.transaction_tp = source.transaction_tp,
    target.entity_tp = source.entity_tp,
    target.name = source.name,
    target.city = source.city,
    target.state = source.state,
    target.zip_code = source.zip_code,
    target.employer = source.employer,
    target.occupation = source.occupation,
    target.transaction_dt = source.transaction_dt,
    target.transaction_amt = source.transaction_amt, -- This will now only be integers
    target.other_id = source.other_id,
    target.tran_id = source.tran_id,
    target.file_num = source.file_num,
    target.memo_cd = source.memo_cd,
    target.memo_text = source.memo_text,
    target.sub_id = source.sub_id
WHEN NOT MATCHED THEN
  INSERT (cmte_id, amndt_ind, rpt_tp, transaction_pgi, image_num, transaction_tp, entity_tp,
  name, city, state, zip_code, employer, occupation, transaction_dt, transaction_amt,
  other_id, tran_id, file_num, memo_cd, memo_text, sub_id)
  VALUES (source.cmte_id, source.amndt_ind, source.rpt_tp, source.transaction_pgi, source.image_num, 
  source.transaction_tp, source.entity_tp, source.name, source.city, source.state, source.zip_code, source.employer, 
  source.occupation, source.transaction_dt, source.transaction_amt, source.other_id, source.tran_id, source.file_num, 
  source.memo_cd, source.memo_text, source.sub_id);
"""

@dag(
    start_date=datetime(2024, 1, 1), schedule=None, default_args=default_args
)
def merge_individual_contributions_table():
    @task
    def begin():
        EmptyOperator(task_id="begin")

    @task
    def merge_table():
        snowflake_query = SnowflakeOperator(
            task_id="snowflake_query",
            sql=merge_individual_cont,
            snowflake_conn_id="snowflake_conn"
        )
        snowflake_query.execute(context={})
    
    @task
    def end():
        EmptyOperator(task_id="end")

    begin() >> merge_table() >> end()

merge_individual_contributions_table()