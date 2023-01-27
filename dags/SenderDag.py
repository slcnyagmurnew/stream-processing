import logging
import os.path

import ujson
from airflow.decorators import dag, task
import pendulum
# from airflow.sensors.external_task import ExternalTaskSensor
from src.stream import send_data
# from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="generate_data",
    schedule_interval="*/5 * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False
)
def generate_data_etl():
    with open('/opt/airflow/dags/config.json', 'r') as conf_file:
        conf = ujson.load(conf_file)
    conf_file.close()

    # @task
    # def wait_initialization():
    #     return ExternalTaskSensor(task_id="wait_upstream", external_dag_id="init_process", external_task_id=None)

    @task
    def get_data_from_file():
        file_name = conf['last_position']
        dir_path = conf['data_dir']
        f = open(file=os.path.join(dir_path, str(file_name) + ".json"))
        json_obj = ujson.load(f)
        f.close()
        return json_obj

    @task
    def send_to_kafka(obj):
        send_data(data=obj)

    @task
    def update_config():
        conf['last_position'] += 1
        update_file = open("/opt/airflow/dags/config.json", "w")
        ujson.dump(conf, update_file)
        update_file.close()
        logging.info("Last position updated successfully..")

    send_to_kafka(get_data_from_file()) >> update_config()


data_send_dag = generate_data_etl()
