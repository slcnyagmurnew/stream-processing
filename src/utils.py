import csv
import os
import pandas as pd
import ujson
import time
import datetime
from prophet.serialize import model_to_json
import random
import socket
import struct
import re

BASE_LOG_DIR = "/opt/airflow/cl_logs"
BASE_DIR = "/opt/airflow/dags"
BASE_CONF_FILE_NAME = "config.json"
OUT_DATA_FILE = "../data/logs.csv"
IN_DATA_FILE = "../data/f_w_01-07_ctlogs.csv"

SELECTED_DATA = os.getenv("SELECTED_DATA", None)


def get_log_data(file_name) -> str:
    """
    Get log data
    :param file_name: log file name
    :return:
    """
    file_dir = os.path.join(BASE_LOG_DIR, file_name)
    with open(file_dir, mode="r") as f:
        log_data = ujson.load(f)
    f.close()
    return log_data


def generate_random_host() -> str:
    return socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))


def create_ip_generation_dict(unq_ip_list: list):
    """
    Generate random ip with using Python libraries
    Match with unique real-data
    :param unq_ip_list: base ip list
    :return:
    """
    ip_generation_dict = {}
    for ip in unq_ip_list:
        generated_ip = generate_random_host()
        while generated_ip in list(ip_generation_dict.values()):
            generated_ip = generate_random_host()
        ip_generation_dict[ip] = generated_ip

    return ip_generation_dict


def convert_to_timestamp(data: str):
    """
    Convert basic time data to timestamp for use them in Redis Time Series DB
    Other types not allowed in RTSDB
    :param data: base time value (ex: 2022-12-03 19:00:02)
    :return: new timestamp value
    """
    return int(time.mktime(datetime.datetime.strptime(data,
                                                      "%Y-%m-%d %H:%M:%S").timetuple()))


def concat_models(folder, train_info, save_file='serialized_model.json'):
    all_dicts = {}
    for file in os.listdir(folder):
        full_filename = "%s/%s" % (folder, file)
        src_ip = re.findall(r'[^\/]+(?=\.)', full_filename)[0]
        with open(full_filename, 'r') as f:
            dict = ujson.load(f)
            all_dicts[src_ip] = dict
        f.close()
    all_dicts["train_info"] = train_info
    with open(save_file, "w") as fp:
        ujson.dump(all_dicts, fp)

    print('JSON models concatenated and saved into one model successfully..')


def create_folder(folder):
    os.makedirs(folder, exist_ok=True)


def save_model(model, file="serialized_model.json"):
    with open(file, 'w') as fout:
        fout.write(model_to_json(model))  # Save model
    print('Model saved successfully..')


def convert():
    """
    Method for creating json files shown as stream data from csv file
    Change column values for security
    :return:
    """

    # replace some values for security
    df = pd.read_csv(IN_DATA_FILE)
    generated_ips = create_ip_generation_dict(unq_ip_list=df.src_ip.unique())
    df.sort_values(by=["timestamp"], inplace=True)
    # get generated ip from dictionary (for each row value)
    df = df[df["src_ip"].isin(SELECTED_DATA)]
    df["src_ip"] = df["src_ip"].apply(replace_with_new_value, args=(generated_ips, ))
    df.to_csv(OUT_DATA_FILE, index=False)  # new data file

    file = open(OUT_DATA_FILE, 'r')
    reader = csv.DictReader(file, fieldnames=('src_ip', 'unq_dst_ip', 'allow', 'drop',
                                              'frequency', 'pkts_sent', 'pkts_received',
                                              'bytesin', 'bytesout', 'unq_dst_port', 'timestamp'))
    i = 0  # max json file count
    # split data into small json files (use as stream)
    for row in reader:
        out = ujson.dumps(row)
        out_file = open(f'../cl_logs/{str(i)}.json', 'w')
        out_file.write(out)
        i += 1


def map_source_ip_to_partition_number():
    """
    Send unique ip to specific Kafka partition number
    Create map between source ip and partition number and dump values into config file
    :return:
    """
    dict_map = dict()
    df = pd.read_csv(OUT_DATA_FILE)
    partition_num = 0
    src_list = df["src_ip"].unique().tolist()  # get unique ip list
    for src in src_list:
        dict_map[src] = partition_num
        partition_num += 1

    with open("../dags/config.json", "r") as fin:
        json_obj = ujson.load(fin)
    fin.close()

    json_obj["source_partition_map"] = dict_map  # dump created dict to config file

    with open("../dags/config.json", "w") as fout:
        ujson.dump(json_obj, fout)
    fout.close()


def get_config() -> dict:
    """
    Get global configuration file
    :return: configuration object: dict
    """
    conf_dir = os.path.join(BASE_DIR, BASE_CONF_FILE_NAME)

    with open(conf_dir, 'r') as f:
        conf = ujson.load(f)
    f.close()
    return conf


def replace_with_new_value(old_ip: str, new_ip_dict: dict) -> str:
    """
    Replace old column values with generated values row by row
    :param old_ip: value that will change
    :param new_ip_dict: created new values based on old values
    :return:
    """
    return new_ip_dict[old_ip]


def update_config():
    current_conf = get_config()
    last_file = current_conf["dag_configs"]["last_processed_file"]
    new_file = ".".join([str(int(last_file.split(".")[0]) + 1), "json"])
    current_conf["dag_configs"]["last_processed_file"] = new_file

    conf_file = os.path.join(BASE_DIR, BASE_CONF_FILE_NAME)
    with open(conf_file, "w") as f:
        ujson.dump(current_conf, f)
    f.close()

