import os.path
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pandas as pd
from datetime import datetime, timedelta
import logging
from prophet import Prophet
from src.utils import create_folder, save_model, load_model
from pyspark.sql.functions import current_date
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType, StringType
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType
import sys
import numpy as np
from functools import wraps
import time

BASE_DIR = "/opt/airflow/dags"

result_schema = StructType([
    StructField('ds', TimestampType()),
    StructField('ip', StringType()),
    StructField('y', DoubleType()),
    StructField('yhat', DoubleType()),
    StructField('yhat_upper', DoubleType()),
    StructField('yhat_lower', DoubleType())
])


def timer(func):
    """
    Calculate function execution time
    Use as decorator
    :param func:
    :return:
    """
    @wraps(func)
    def wrapper(*args):
        start_time = time.time()
        retval = func(*args)
        print("The execution ends in ", time.time() - start_time, "secs")
        return retval

    return wrapper


def add_day_to_hour(date, add_hour=1):
    """
    adds given hour to given date
    :param date:
    :param add_hour:
    :return:
    """
    updated_time = date + timedelta(hours=add_hour)
    # print(updated_time, type(updated_time))
    return updated_time


def save_data_as_dataframe(dataframe: pd.DataFrame, file_name: str, folder_name: str):
    """
    Save data function both historical and forecasting data
    Functionalities:
        ==> Create csv file if it does not exist
        ==> Update csv file if it exists
    :param file_name: save file name
    :param folder_name: save folder name
    :param dataframe: save dataframe
    :return:
    """
    folder_path = os.path.join(BASE_DIR, folder_name)
    file_path = os.path.join(folder_path, f"{file_name}.csv")
    dataframe = dataframe.sort_values(by="ds", ascending=True)  # sort data with timestamp
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        new_df = pd.concat([df, dataframe], ignore_index=True)
        new_df.to_csv(file_path, index=False)
        logging.info("Data file already exists.. New data appended..")
    else:
        create_folder(folder=folder_path)
        dataframe.to_csv(file_path, index=False)
        logging.info("Data file created..")


def update_future(interval, dataframe, start_hour=8, last_hour=18, max_interval=11):
    """
    Check created future dataframe for forecasting whether if its hours is upper last desired hour
    :param interval: desired future forecast time (exp: 5, 10 in hours)
    :param dataframe: created future dataframe for each src ip
    :param start_hour: from hour (exp: start working hour 8, 9)
    :param last_hour: to hour (exp: end working hour 18, 19)
    :param max_interval: period of start and end hours (include last hour) (exp: 18-8=11 hours)
    :return: updated dataframe
    """
    if interval > max_interval:
        return None
    forecast_data = dataframe.tail(interval)
    forecast_date = None
    last_date = None
    for index, row in forecast_data.iterrows():
        forecast_date = row['ds']
        if last_date is not None:
            new_date = add_day_to_hour(last_date, add_hour=1)
            dataframe.loc[index, ['ds']] = new_date
            last_date = new_date
        else:
            if forecast_date.hour > last_hour:
                new_date = add_day_to_hour(forecast_date, add_hour=((24 - forecast_date.hour) + start_hour))
                dataframe.loc[index, ['ds']] = new_date
                last_date = new_date
    return dataframe


@timer
@pandas_udf(result_schema, PandasUDFType.GROUPED_MAP)
def forecast_ip(history_pd):
    """
    Function that runs for every ip grouped data
    Prophet model is created and used for forecasting
    :param history_pd: specified ip data
    :return:
    """
    model = Prophet(
        interval_width=0.95,
        growth='linear',
        daily_seasonality=True,
        seasonality_mode='multiplicative'
    )
    # get fitted src ip to use it model file
    src_ip = history_pd['ip'].iloc[0]

    # fit the model
    ip_model_path = os.path.join(BASE_DIR, f'models/{g_train_type}/{src_ip}.json')

    if os.path.exists(ip_model_path):  # retrain model if there is older one
        history_path = os.path.join(BASE_DIR, f"history/{src_ip}.csv")
        old_model = load_model(ip_model_path)
        history_data = pd.read_csv(history_path)
        model.fit(history_data, init=warm_start_params(old_model))
        logging.info(f"Retraining operation successfully finished..")
    else:  # train model for first time
        model.fit(history_pd)
        save_model(model=model, file=ip_model_path)
        logging.info(f"First training operation successfully finished..")

    # configure predictions, forecast 8 hours
    future_pd = model.make_future_dataframe(
        periods=8,
        freq='H',
        include_history=True
    )

    # make predictions
    results_pd = model.predict(future_pd)
    results_pd = update_future(dataframe=results_pd, interval=8)
    f_pd = results_pd[['ds', 'yhat', 'yhat_upper', 'yhat_lower']].set_index('ds')
    st_pd = history_pd[['ds', 'ip', 'y']].set_index('ds')

    result_pd = f_pd.join(st_pd, how='left')
    result_pd.reset_index(level=0, inplace=True)

    result_pd['ip'] = history_pd['ip'].iloc[0]
    save_data_as_dataframe(history_pd, file_name=src_ip,
                           folder_name="history")  # save old data to retrain model in the future
    return result_pd[['ds', 'ip', 'y', 'yhat', 'yhat_upper', 'yhat_lower']]


def warm_start_params(m):
    """
    Retrieve parameters from a trained model in the format used to initialize a new Stan model.
    Note that the new Stan model must have these same settings:
        n_changepoints, seasonality features, mcmc sampling
    for the retrieved parameters to be valid for the new model.

    Parameters
    ----------
    m: A trained model of the Prophet class.

    Returns
    -------
    A Dictionary containing retrieved parameters of m.
    """
    res = {}
    for pname in ['k', 'm', 'sigma_obs']:
        if m.mcmc_samples == 0:
            res[pname] = m.params[pname][0][0]
        else:
            res[pname] = np.mean(m.params[pname])
    for pname in ['delta', 'beta']:
        if m.mcmc_samples == 0:
            res[pname] = m.params[pname][0]
        else:
            res[pname] = np.mean(m.params[pname], axis=0)
    return res


@timer
def train(data, train_type):
    """
    Train Prophet models according to ip data and train type
    :param data: specified ip data
    :param train_type: column name for training
    :return:
    """
    global g_save_date
    global g_train_type

    print(data)
    # dataframe = kwargs["ti"].xcom_pull(task_ids='convert_to_dataframe')
    dataframe = pd.read_json(data, orient="columns")  # get data from convert_to_dataframe task

    dataframe["ds"] = dataframe["ds"].apply(lambda x: datetime.fromtimestamp(x))  # convert epoch time to timestamp

    if not dataframe.empty:
        print(f'Dataframe: {dataframe}')
        # start = time.time()

        g_save_date = f"{datetime.now():%y%m%d_%H%M}"
        g_train_type = train_type

        folder_path = os.path.join(BASE_DIR, f'models/{train_type}')
        create_folder(folder=folder_path)

        dataframe['ds'] = pd.to_datetime(dataframe['ds'], infer_datetime_format=True)
        sdf = spark.createDataFrame(dataframe)
        sdf.createOrReplaceTempView(train_type)

        sql = f"SELECT ip, ds, sum({train_type}) as y FROM {train_type} GROUP BY ip, ds ORDER BY ip, ds"

        src_part = (spark.sql(sql).repartition(spark.sparkContext.defaultParallelism, ['ip'])).cache()
        results = (src_part.groupby('ip').apply(forecast_ip).withColumn('training_date', current_date()))
        # results.cache()

        results.coalesce(1)

        results.createOrReplaceTempView('forecasted')

        # spark.sql("SELECT ip, count(*) FROM forecasted GROUP BY ip").show()
        final_df = results.toPandas()
        save_data_as_dataframe(final_df, folder_name="forecasts", file_name=f"forecast_{train_type}")
        logging.info(f"Training operation successfully finished..")

    else:  # if dataframe is empty
        logging.warning("There is no data for training operation..")


if __name__ == '__main__':
    # it is not used but must be set, otherwise airflow throws error (can not init spark context)
    sc = SparkContext(conf=SparkConf().setMaster("local"))
    spark = SparkSession \
        .builder \
        .appName('StreamProcessingDemo').getOrCreate()
    print(sys.argv)
    # Example sys.argv result: (['{"ip":{},"ds":{},"unq_dst_ip":{}}', 'unq_dst_ip'],)

    try:
        print(type(sys.argv[1]))
        train(sys.argv[1], sys.argv[2])
    except IndexError as err:
        # if sys argv does not come as expected, catch exception
        logging.error(err)
