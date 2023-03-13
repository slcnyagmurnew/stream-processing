from redis import Redis
import os
import ujson
from redis.exceptions import ConnectionError
import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from src.utils import get_config, convert_to_timestamp
from src.KafkaAdapter import KafkaAdapter

adapter = KafkaAdapter()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", default=["kafka:9092"])
TOPIC_NAME = os.getenv("TOPIC_NAME", default=None)
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_CONN_STRING = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")

r = Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT"))
)

db = create_engine(POSTGRES_CONN_STRING)  # need for Pandas to sql, not working with usual connection string
conn_for_pandas = db.connect()
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)

# TODO add kafka connection check


def retrieve_redis_data(data_type="unq_dst_ip") -> dict:
    """
    Get related Redis data with using data type parameter
    :param data_type: use later for another data types like frequency etc.
                        default=unq_dst_ip
    :return: json data object
    """
    logging.info(f"Get all of the values from type of {data_type} from earliest to latest...")
    mrange_filters = [f"dataType={data_type}"]
    mrange_reply = r.ts().mrange(0, "+", mrange_filters, with_labels=True)
    # print(type(mrange_reply), mrange_reply)
    print(f"Result: {ujson.dumps(mrange_reply, indent=4)}")
    return mrange_reply


def redis_time_series_to_dataframe(**kwargs) -> str:
    """
    Get Redis data from arguments with xcom_pull and convert data to Pandas dataframe
    Get train type for training
    :return: created dataframe
    """
    train_type = kwargs["train_type"]
    rts_data = kwargs['ti'].xcom_pull(task_ids='get_stream_data')

    logging.info(f"Conversion of Redis Time Series data to Pandas dataframe started..")

    df = pd.DataFrame(columns=["ip", "ds", train_type])

    for data in rts_data:
        ip_dict = {}
        content = list(data.values())[0]

        ip_info = content[0]["srcIP"]
        ip_data = content[1]

        ip_dict["ip"] = ip_info  # put ip information into dict

        # if there is no log data about ip, continue without append to dataframe
        for time_based_data in ip_data:
            ip_dict["ds"] = time_based_data[0]
            ip_dict[train_type] = time_based_data[1]
            df = df.append(ip_dict, ignore_index=True)

    return df.to_json()


def init_kafka():
    """
    Initialize Kafka topics and its configurations
    :return:
    """
    logging.info("Kafka initialization started..")
    conf = get_config()
    try:
        adapter.create_topics(bootstrap_servers=BOOTSTRAP_SERVERS, topic_config_list=conf['topic_configs'])
    except Exception as err:
        logging.error(err)


def send_to_kafka(data) -> None:
    """
    Send one log data to Kafka service
    :param data: log data
        example ==> {
          "src_ip": "60.73.131.213",
          "unq_dst_ip": "3",
          "allow": "17",
          "drop": "0",
          "frequency": "17",
          "pkts_sent": "133",
          "pkts_received": "114",
          "bytesin": "18543",
          "bytesout": "68942",
          "unq_dst_port": "2",
          "timestamp": "2022-01-03 08:00:00"
        }
    :return:
    """
    logging.info(f"Data obtained from API resource for Kafka.. {data}")
    conf = get_config()
    try:
        partition = conf["source_partition_map"][data["src_ip"]]
        adapter.produce(topic_name=TOPIC_NAME, data=data,
                        bootstrap_servers=BOOTSTRAP_SERVERS, partition=partition)
        logging.info('Data sent to Kafka broker..')
    except Exception as err:
        logging.error(err)


def init_redis(data_type="unq_dst_ip"):
    """
    Initialize Redis Time Series DB keys with source ips
    Unique keys with ips
    Example of key-values appearance: {
        "unq_dst_ip_135.131.48.35": [
            {
                "srcIP": "135.131.48.35",
                "dataType": "unq_dst_ip"
            },
            [
                [
                    1641196800,
                    0.0
                ]
            ]
        ]
    }
    :param data_type: (str) data type for training (exp: frequency, allow etc.)
    :return:
    """
    conf = get_config()
    ip_list = list(conf["source_partition_map"].keys())
    for i, ip in enumerate(ip_list):
        labels = {"srcIP": ip,
                  "dataType": data_type}
        src_key = f"{data_type}_{ip}"
        r.ts().create(key=src_key, labels=labels)
        logging.info(f"TS created in Redis for {src_key}")


def send_to_redis(data, data_type: str):
    """
    Send collected API data to Redis database
    :param data_type:
    :param data:
    :return:
    """
    src_key = f"{data_type}_{data['src_ip']}"
    logging.info(f"Data obtained from stream resource for Redis.. {data}")
    try:
        r.ts().add(key=src_key, timestamp=convert_to_timestamp(data["timestamp"]), value=data[data_type])
        logging.info(f'Data sent to Redis: {src_key}..')
    except Exception as err:
        logging.error(err)


def redis_connection():
    """
    Check if Redis connection established or not
    :return: use in Airflow task so no return
    """
    count, max_retries, f = 0, 5, False
    while max_retries > count and f is False:
        if r.ping():
            logging.info("Redis connection established..")
            f = True
        else:
            count += 1
    if f is False:
        raise ConnectionError("Redis can not connected..")


def dump_dataframe_to_postgres(**kwargs):
    """
    Redis Time Series Database to Postgres database
    Get dataframe from convert_to_dataframe task
    :param kwargs: dataframe (in json format)
    :return:
    """
    data = kwargs["ti"].xcom_pull(task_ids='convert_to_dataframe')  # dataframe in JSON string
    df = pd.read_json(data, orient="columns")  # convert JSON to Pandas dataframe

    try:
        # set index as False because it results in duplicate "index" column in sql
        # every "to_sql" job, dataframe starts indexing from 0
        df.to_sql(name="logs", con=conn_for_pandas, if_exists="append", index=False)
        logging.info("New log data added to existing table..")
    except Exception as err:
        logging.error(err)

    conn.commit()
    conn.close()


def remove_redis_cache_data():
    """
    Remove Redis cache data to avoid duplication in Postgres table
    Add new data to existing empty Redis Time Series keys in every DAG run
    :return:
    """
    logging.info("Delete range of values from keys...")
    for key in r.keys():
        key = key.decode("ASCII")
        r.ts().delete(key, 0, "+")
        range_reply = r.ts().range(key, 0, "+")
        print(ujson.dumps(range_reply, indent=4) + "\n")
