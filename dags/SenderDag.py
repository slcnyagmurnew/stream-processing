import time
import pendulum
from src.utils import get_api_data
from src.ops import redis_connection, send_to_redis, send_to_kafka
from airflow.operators.python import PythonOperator
from airflow import DAG


with DAG(
    "generate_data",
    schedule_interval="* * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    location_list = {"ankara": 100323786, "istanbul": 100745044, "izmir": 100311046}  # to config maybe
    current_data = []
    for location in list(location_list.values()):
        current_data.append(get_api_data(location_id=location))
        time.sleep(0.5)

    check_redis_connection = PythonOperator(
        python_callable=redis_connection,
        task_id="check_redis_connection",
        dag=dag
    )

    generate_data_to_kafka = PythonOperator(
        task_id="send_data_to_kafka",
        python_callable=send_to_kafka,
        op_kwargs={
            "location_data": current_data,
            "location_lst": location_list
        },
        dag=dag
    )

    generate_data_to_redis = PythonOperator(
        task_id="send_data_to_redis",
        python_callable=send_to_redis,
        op_kwargs={
            "location_data": current_data,
            "location_lst": location_list
        },
        dag=dag
    )

    check_redis_connection >> [generate_data_to_kafka, generate_data_to_redis]

