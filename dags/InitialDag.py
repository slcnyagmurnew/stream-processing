import pendulum
from src.ops import init_kafka, init_redis
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
    location_list = {"ankara": 100323786, "istanbul": 100745044, "izmir": 100311046}  # to config maybe

    create_kafka_topics = PythonOperator(
        task_id="init_kafka_topics",
        python_callable=init_kafka,
        dag=dag
    )

    create_redis_ts_metadata = PythonOperator(
        task_id="init_redis_ts",
        python_callable=init_redis,
        op_kwargs={
            "location_lst": location_list
        },
        dag=dag
    )

    trigger = TriggerDagRunOperator(
        task_id="trigger_sender_dag",
        trigger_dag_id="generate_data",
        dag=dag,
    )

    [create_kafka_topics, create_redis_ts_metadata] >> trigger



