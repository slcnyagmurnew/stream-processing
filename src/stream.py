from src.KafkaAdapter import KafkaAdapter
import os
import logging


adapter = KafkaAdapter()
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", default=["kafka:9092"])


def send_data(data) -> None:
    try:
        adapter.produce(topic_name="WeatherTopic", data=data, bootstrap_servers=bootstrap_servers)
        logging.info('Data sent to Kafka broker..')
    except Exception as err:
        logging.error(err)

