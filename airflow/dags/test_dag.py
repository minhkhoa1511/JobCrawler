from airflow.decorators import dag, task
from datetime import datetime, timedelta


@dag(
    dag_id='test_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['test']
)

def test_dag():
    @task
    def print_hello():
        print("Hello, Minh Khoa!")

    @task
    def print_how_are_you():
        print("How are you?")
    print_hello() >> print_how_are_you()

dag = test_dag()
