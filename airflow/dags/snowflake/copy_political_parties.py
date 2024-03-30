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

truncate_political_parties = """
USE ELECTION.RAW;

TRUNCATE TABLE election.raw.SRC_POLITICAL_PARTIES;
"""

copy_political_parties = """
USE ELECTION.PUBLIC;

COPY INTO election.raw.src_political_parties
FROM @POLITICAL_PARTIES_STAGE
ON_ERROR = 'SKIP_FILE_5%'
FILE_FORMAT = FEC_CSV;
"""

@dag(
    start_date=datetime(2024, 1, 1), schedule=None, default_args=default_args
)
def copy_political_parties_table():
    @task
    def begin():
        EmptyOperator(task_id="begin")

    @task
    def truncate_table():
        snowflake_query = SnowflakeOperator(
            task_id="snowflake_query",
            sql=truncate_political_parties,
            snowflake_conn_id="snowflake_conn"
        )
        snowflake_query.execute(context={})

    @task
    def copy_table():
        snowflake_query = SnowflakeOperator(
            task_id="snowflake_query",
            sql=copy_political_parties,
            snowflake_conn_id="snowflake_conn"
        )
        snowflake_query.execute(context={})
    
    @task
    def end():
        EmptyOperator(task_id="end")

    begin() >> truncate_table() >> copy_table() >> end()
copy_political_parties_table()