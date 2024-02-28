from airflow.decorators import dag, task
from pendulum import datetime, duration 
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=1),
}

@dag(
    start_date=datetime(2024, 1, 1), schedule="@daily"
)
def generate_dag():
    @task #Example of TaskFlow use, here's the docs: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html
    def empty_opt1():
        EmptyOperator()

    @task
    def empty_opt2():
        EmptyOperator()

    empty_opt1() >> empty_opt2()

generate_dag()