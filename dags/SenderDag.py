from datetime import timedelta
import airflow
from src.utils import get_log_data, get_config, update_config
from src.ops import redis_connection, send_to_redis, send_to_kafka
from airflow.operators.python import PythonOperator
from airflow import DAG

BASE_CONF_DIR = "/opt/airflow/dags"

with DAG(
        "generate_data_for_stream_processing",
        schedule_interval=timedelta(seconds=20),
        start_date=airflow.utils.dates.days_ago(2),
        max_active_runs=1,
        catchup=False,
        is_paused_upon_creation=False
) as dag:
    conf = get_config()

    current_data = get_log_data(conf["dag_configs"]["last_processed_file"])

    check_redis_connection_task = PythonOperator(
        python_callable=redis_connection,
        task_id="check_redis_connection",
        dag=dag
    )

    generate_data_to_kafka_task = PythonOperator(
        task_id="send_data_to_kafka",
        python_callable=send_to_kafka,
        op_kwargs={
            "data": current_data
        },
        dag=dag
    )

    generate_data_to_redis_task = PythonOperator(
        task_id="send_data_to_redis",
        python_callable=send_to_redis,
        op_kwargs={
            "data": current_data,
            "data_type": "unq_dst_ip"
        },
        dag=dag
    )

    update_config_task = PythonOperator(
        task_id="update_config_file",
        python_callable=update_config,
        dag=dag
    )

    check_redis_connection_task >> [generate_data_to_kafka_task, generate_data_to_redis_task] >> update_config_task
