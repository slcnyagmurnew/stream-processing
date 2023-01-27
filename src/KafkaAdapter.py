from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from typing import Dict, List
import ujson
import logging


class KafkaAdapter:
    def __init__(self, value_deserializer=lambda x: ujson.loads(x.decode('utf-8')),
                 value_serializer=lambda value: ujson.dumps(value).encode()):
        self.name = "KafkaAdapter"
        self.value_deserializer = value_deserializer
        self.value_serializer = value_serializer

    def produce(self, bootstrap_servers: List, topic_name, data):
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=self.value_serializer
        )
        try:
            producer.send(topic=topic_name, value=data)
            logging.info(f"Data sent to {topic_name}")

        except Exception as err:
            logging.error(err)
        producer.close()

    def consume(self, topic_name, bootstrap_servers, group: str, auto_offset_reset=False,
                enable_auto_commit='earliest',
                consumer_timeout=3000):
        try:
            consumer = KafkaConsumer(
                topic_name,
                group_id=group + topic_name,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=self.value_deserializer,
                enable_auto_commit=enable_auto_commit,
                auto_offset_reset=auto_offset_reset,
                consumer_timeout=consumer_timeout
            )
            logging.info(f"Data consumed from {topic_name}")
            return consumer

        except Exception as err:
            logging.error(err)

    @staticmethod
    def create_topics(bootstrap_servers, topic_config_list: List[Dict]):
        client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topics = []
        try:
            for topic in topic_config_list:
                topics.append(NewTopic(name=topic['name'], num_partitions=topic['num_partitions'],
                                       replication_factor=topic['replication_factor'],
                                       topic_configs=topic['topic_configs']))

            client.create_topics(new_topics=topics)
        except KeyError as err:
            logging.error(f"Provide necessary keys for topic creation.. {err}")

    def create_producer(self, bootstrap_servers):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=self.value_serializer
            )
        except Exception as err:
            logging.error(err)


