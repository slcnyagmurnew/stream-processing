from redis import Redis
import os
import ujson
from redis.exceptions import ConnectionError
from src.utils import preprocess_data
import logging
from src.KafkaAdapter import KafkaAdapter

adapter = KafkaAdapter()
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", default=["kafka:9092"])
TOPIC_NAME = os.getenv("TOPIC_NAME", default=None)

r = Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT"))
)


# TODO add kafka connection check


def retrieve_redis_data(data_type="temperature"):
    """
    Get redis temperature data from all provinces
    :param data_type: use later for another data types like humidity etc.
                        default=temperature
    :return: json data object
    """
    logging.info(f"Get all of the values from type of {data_type} from earliest to latest...")
    mrange_filters = [f"dataType={data_type}"]
    mrange_reply = r.ts().mrange(0, "+", mrange_filters, with_labels=True)
    # print(type(mrange_reply), mrange_reply)
    print(f"Result: {ujson.dumps(mrange_reply, indent=4)}")
    return mrange_reply


def init_kafka():
    """
    Initialize Kafka topics and its configurations
    :return:
    """
    try:
        with open('/opt/airflow/dags/config.json', 'r') as f:
            conf = ujson.load(f)
        f.close()
        adapter.create_topics(bootstrap_servers=BOOTSTRAP_SERVERS, topic_config_list=conf['topic_configs'])
    except Exception as err:
        logging.error(err)


def send_to_kafka(location_lst, location_data):
    """

    :param location_lst:
    :param location_data:
    :return:
    """
    location_ids = list(location_lst.values())
    for p, data in enumerate(location_data):
        print(f"Api data for Kafka: {data} partition: {p}")
        each_data_to_kafka(loc_id=location_ids[p], data=data, partition=p)


def each_data_to_kafka(loc_id, data, partition) -> None:
    """

    :param loc_id:
    :param data:
    :param partition:
    :return:
    """
    logging.info("Data obtained from API resource..")
    extracted_data = preprocess_data(loc_id, data)
    try:
        adapter.produce(topic_name=TOPIC_NAME, data=extracted_data,
                        bootstrap_servers=BOOTSTRAP_SERVERS, partition=partition)
        logging.info('Data sent to Kafka broker..')
    except Exception as err:
        logging.error(err)


def each_data_to_redis(location_name, data) -> None:
    """
    Each of API data to Redis
    Preprocessing is not necessary but data time must be in timestamp format
    :param location_name:
    :param data:
    :return:
    """
    extracted_data = preprocess_data(location_name, data)
    try:
        r.ts().add(key=location_name, timestamp=extracted_data["time"], value=extracted_data["feelsLikeTemp"])
        logging.info(f'Data sent to Redis: {location_name}..')
    except Exception as err:
        logging.error(err)


def init_redis(location_lst: dict):
    """

    :param location_lst:
    :return:
    """
    location_names = list(location_lst.keys())
    location_ids = list(location_lst.values())
    for i, loc in enumerate(location_ids):
        labels = {"locId": str(loc),
                  "dataType": "temperature"}
        temperature_key = location_names[i]
        r.ts().create(key=temperature_key, labels=labels)
        logging.info(f"TS created in Redis for {temperature_key}")


def send_to_redis(location_lst: dict, location_data):
    """
    Send collected API data to Redis database
    :param location_lst: unique location name list
    :param location_data: current location data
    :return:
    """
    location_names = list(location_lst.keys())
    for p, data in enumerate(location_data):
        logging.info(f"Data for Redis: {data}")
        each_data_to_redis(location_name=location_names[p], data=data)


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
