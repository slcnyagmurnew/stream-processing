import os
from airflow.decorators import dag, task
import pendulum
from src.stream import read_stream_from_kafka

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', default=['kafka:9092'])
TOPIC_NAME = os.getenv('TOPIC_NAME', default=None)


@dag(
    dag_id="process_data",
    schedule_interval="* * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False
)
def process_data_etl():
    @task
    def get_stream_data():
        read_stream_from_kafka()

    get_stream_data()


data_process_dag = process_data_etl()
