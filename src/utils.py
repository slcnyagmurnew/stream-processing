import csv
import os
import requests
import ujson
import logging
import time
import datetime


def preprocess_data(loc_id, data):
    """

    :param loc_id:
    :param data:
    :return:
    """
    logging.info("Data preprocess started")
    try:
        return {
            "id": str(loc_id),
            "time": convert_to_timestamp(data["current"]["time"][0:16]),
            # "temperature": data["current"]["temperature"],
            "feelsLikeTemp": data["current"]["feelsLikeTemp"],
            "relHumidity": data["current"]["relHumidity"]
            # "windSpeed": data["current"]["windSpeed"]
            # "windDir": data["current"]["windDir"],
            # "pressure": data["current"]["pressure"],
            # "symbolPhrase": data["current"]["symbolPhrase"]
        }
    except Exception:
        raise KeyError


def get_api_data(location_id) -> str:
    """

    :param location_id:
    :return:
    """
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


def convert_to_timestamp(data: str):
    """

    :param data:
    :return:
    """
    return int(time.mktime(datetime.datetime.strptime(data,
                                                      "%Y-%m-%dT%H:%M").timetuple()))


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
