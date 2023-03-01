import airflow
from src.ops import init_kafka, init_redis
from airflow.operators.python import PythonOperator
from airflow import DAG

with DAG(
        "init_process_for_stream_processing",
        schedule_interval='@once',
        start_date=airflow.utils.dates.days_ago(2),
        catchup=False,
        is_paused_upon_creation=False
) as dag:

    create_kafka_topics_task = PythonOperator(
        task_id="init_kafka_topics",
        python_callable=init_kafka,
        dag=dag
    )

    create_redis_ts_metadata_task = PythonOperator(
        task_id="init_redis_ts",
        python_callable=init_redis,
        op_kwargs={
            "data_type": "unq_dst_ip"
        },
        dag=dag
    )

    # trigger = TriggerDagRunOperator(
    #     task_id="trigger_sender_dag",
    #     trigger_dag_id="generate_data",
    #     dag=dag,
    # )

    [create_kafka_topics_task, create_redis_ts_metadata_task]
