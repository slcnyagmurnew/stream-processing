# from airflow.decorators import dag, task
import airflow
from src.ops import retrieve_redis_data, \
    redis_time_series_to_dataframe, \
    dump_dataframe_to_postgres, remove_redis_cache_data
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG

# conf = {"spark.master": master, "spark.app.name": "StreamProcessingDemo",

with DAG(
        'process_data_for_stream_processing',
        schedule_interval=timedelta(minutes=10),
        start_date=airflow.utils.dates.days_ago(2),
        max_active_runs=1,
        is_paused_upon_creation=False,
        catchup=False
) as dag:
    get_redis_data_task = PythonOperator(
        task_id='get_stream_data',
        python_callable=retrieve_redis_data,
        op_kwargs={
            "data_type": "unq_dst_ip"
        },
        do_xcom_push=True,
        dag=dag
    )

    redis_to_dataframe_task = PythonOperator(
        task_id="convert_to_dataframe",
        python_callable=redis_time_series_to_dataframe,
        op_kwargs={
            "train_type": "unq_dst_ip"
        },
        do_xcom_push=True,
        dag=dag
    )

    train_model_task = SparkSubmitOperator(
        task_id="create_model_with_training",
        application="/opt/airflow/src/forecasting.py",
        application_args=["{{ti.xcom_pull(task_ids='convert_to_dataframe')}}", "unq_dst_ip"],
        conn_id="spark_master",
        dag=dag
    )

    dump_to_database_task = PythonOperator(
        task_id="dump_redis_data_to_postgres",
        python_callable=dump_dataframe_to_postgres,
        dag=dag
    )

    delete_cache_task = PythonOperator(
        task_id="clean_redis_data",
        python_callable=remove_redis_cache_data,
        dag=dag
    )

    get_redis_data_task >> redis_to_dataframe_task >> [train_model_task, dump_to_database_task] >> delete_cache_task

# @dag(
#     dag_id="process_data",
#     schedule_interval="* * * * *",
#     start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
#     catchup=False,
#     is_paused_upon_creation=True
# )
# def process_data_etl():
#     @task
#     def get_stream_data():
#
#
#     # @task
#     # def stream_to_batch_data():
#     #
#
#     get_stream_data()
#
#
# data_process_dag = process_data_etl()
