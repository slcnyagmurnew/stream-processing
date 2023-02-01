import logging
import os.path
from airflow.decorators import dag, task
import pendulum
# from airflow.sensors.external_task import ExternalTaskSensor
from src.utils import get_api_data, send_data
# from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="generate_data",
    schedule_interval="* * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False
)
def generate_data_etl():

    # @task.sensor
    # def wait_initialization():
    #     return ExternalTaskSensor(task_id="wait_upstream", external_dag_id="init_process", external_task_id=None)

    @task
    def get_weather_api_data():
        print("in weather api")
        return get_api_data()

    @task
    def send_to_kafka(obj):
        print(f"kafka data: {obj}")
        send_data(data=obj)

    send_to_kafka(get_weather_api_data())


data_send_dag = generate_data_etl()
