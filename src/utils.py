import csv
import os

import requests
import ujson
import logging
from src.KafkaAdapter import KafkaAdapter

adapter = KafkaAdapter()
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", default=["kafka:9092"])


def init_kafka():
    try:
        with open('config.json', 'r') as f:
            conf = ujson.load(f)
        f.close()
        adapter.create_topics(bootstrap_servers=bootstrap_servers, topic_config_list=conf['topic_configs'])
        logging.info('Topics created successfully..')
    except Exception as err:
        logging.error(err)


def get_api_data() -> str:
    url = "https://weatherapi-com.p.rapidapi.com/current.json"

    querystring = {"q": "Istanbul"}

    headers = {
        # secrets
        "X-RapidAPI-Key": os.getenv('X-RapidAPI-Key'),
        "X-RapidAPI-Host": os.getenv('X-RapidAPI-Host')
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    return response.text


def convert():
    """
    Method for creating json files shown as stream data from csv file
    :return:
    """
    file = open('../data/BitcoinTweets.csv', 'r')
    reader = csv.DictReader(file, fieldnames=('table_key', 'tweet_id', 'text', 'date', 'favorites', 'retweets'))
    i, row_count = 0, 30000  # max json file count
    for row in reader:
        out = ujson.dumps(row)
        out_file = open(f'../tweets/{str(i)}.json', 'w')
        out_file.write(out)
        if i == row_count:
            break
        i += 1


