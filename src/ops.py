from redis import Redis
import os
import ujson
from redis.exceptions import ConnectionError
import logging
from src.utils import get_config, convert_to_timestamp
from src.KafkaAdapter import KafkaAdapter

adapter = KafkaAdapter()
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", default=["kafka:9092"])
TOPIC_NAME = os.getenv("TOPIC_NAME", default=None)

r = Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT"))
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
    # with open("/opt/airflow/src/result.json", "w") as f:
    #     ujson.dump(mrange_reply, f)
    # f.close()
    return mrange_reply


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
    logging.info(f"Data obtained from API resource for Redis.. {data}")
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
