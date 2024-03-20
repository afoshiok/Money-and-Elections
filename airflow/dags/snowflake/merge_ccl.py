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

merge_ccl = """
USE ELECTION.PUBLIC;

MERGE INTO election.raw.src_cand_cmte_link AS target
USING (
    SELECT DISTINCT
        $7 as "LINKAGE_ID",
        $1 as "CAND_ID",
        $2 as "CAND_ELECTION_YR",
        $3 as "FEC_ELECTION_YR",
        $4 as "CMTE_ID",
        $5 as "CMTE_TP",
        $6 as "CMTE_DSGN"
    FROM @candidate_committee_stage
) AS source
ON target.linkage_id = source.linkage_id 
WHEN MATCHED THEN
    UPDATE SET
        target.cand_id = source.cand_id,
        target.cand_election_yr = source.cand_election_yr,
        target.fec_election_yr = source.fec_election_yr,
        target.cmte_id = source.cmte_id,
        target.cmte_tp = source.cmte_tp,
        target.cmte_dsgn = source.cmte_dsgn,
        target.linkage_id = source.linkage_id
WHEN NOT MATCHED THEN
    INSERT (cand_id,cand_election_yr,fec_election_yr,cmte_id,cmte_tp,cmte_dsgn,linkage_id)
    VALUES (source.cand_id,source.cand_election_yr,source.fec_election_yr,source.cmte_id,source.cmte_tp,source.cmte_dsgn,source.linkage_id);
"""


@dag(
    start_date=datetime(2024, 1, 1), schedule=None, default_args=default_args
)
def merge_ccl_table():
    @task
    def begin():
        EmptyOperator(task_id="begin")

    @task
    def merge_table():
        snowflake_query = SnowflakeOperator(
            task_id="snowflake_query",
            sql=merge_ccl,
            snowflake_conn_id="snowflake_conn"
        )
        snowflake_query.execute(context={})

    @task
    def end():
        EmptyOperator(task_id="end")

    begin() >> merge_table() >> end()

merge_ccl_table()