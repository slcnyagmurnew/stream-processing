from airflow.decorators import dag, task
import pendulum
from src.utils import init_kafka


@dag(
    dag_id="init_process",
    schedule_interval='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False
)
def init_process_etl():
    @task
    def create_kafka_topics():
        """
        create desired kafka topics using config file
        :return:
        """
        init_kafka()

    create_kafka_topics()


process_init_dag = init_process_etl()


