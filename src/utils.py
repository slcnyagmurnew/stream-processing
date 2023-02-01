import csv
import os

import requests
import ujson
import logging
from src.KafkaAdapter import KafkaAdapter

adapter = KafkaAdapter()
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", default=["kafka:9092"])
TOPIC_NAME = os.getenv("TOPIC_NAME", default=None)


def init_kafka():
    try:

        with open('/opt/airflow/dags/config.json', 'r') as f:
            conf = ujson.load(f)
        f.close()
        adapter.create_topics(bootstrap_servers=BOOTSTRAP_SERVERS, topic_config_list=conf['topic_configs'])
    except Exception as err:
        logging.error(err)


def send_data(loc_id, data, partition) -> None:
    logging.info("Data obtained from API resource..")
    extracted_data = {
        "id": str(loc_id),
        "time": data["current"]["time"],
        "temperature": data["current"]["temperature"],
        "feelsLikeTemp": data["current"]["feelsLikeTemp"],
        "relHumidity": data["current"]["relHumidity"],
        "windSpeed": data["current"]["windSpeed"],
        "windDir": data["current"]["windDir"],
        "pressure": data["current"]["pressure"],
        "symbolPhrase": data["current"]["symbolPhrase"]
    }
    try:
        adapter.produce(topic_name=TOPIC_NAME, data=extracted_data,
                        bootstrap_servers=BOOTSTRAP_SERVERS, partition=partition)
        logging.info('Data sent to Kafka broker..')
    except Exception as err:
        logging.error(err)


def get_api_data(location_id) -> str:
    # print("get data from api")
    # url = "https://weatherapi-com.p.rapidapi.com/current.json"
    #
    # querystring = {"q": "Istanbul"}
    #
    # headers = {
    #     # secrets
    #     "X-RapidAPI-Key": os.getenv('X-RapidAPI-Key'),
    #     "X-RapidAPI-Host": os.getenv('X-RapidAPI-Host')
    # }
    # response = requests.request("GET", url, headers=headers, params=querystring)
    # print(f"response: {response.text}")
    # return response.json()
    # istanbul 100745044 izmir 100311046 ankara 100323786
    url = f"https://foreca-weather.p.rapidapi.com/current/{location_id}"

    querystring = {"alt": "0", "tempunit": "C", "windunit": "MS", "tz": "Europe/Istanbul", "lang": "en"}

    headers = {
        "X-RapidAPI-Key": os.getenv('X-RapidAPI-Key'),
        "X-RapidAPI-Host": os.getenv('X-RapidAPI-Host')
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    print(response.text)
    return response.json()


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


