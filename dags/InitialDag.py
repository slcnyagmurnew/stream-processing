import pendulum
from src.utils import init_kafka
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    "init_process",
    schedule_interval='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    """
    create desired kafka topics using config file
    :return:
    """
    create_kafka_topics = PythonOperator(
        task_id="init_kafka_topics",
        python_callable=init_kafka,
        dag=dag
    )

    trigger = TriggerDagRunOperator(
        task_id="trigger_sender_dag",
        trigger_dag_id="generate_data",
        dag=dag,
    )

    create_kafka_topics >> trigger



