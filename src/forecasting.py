import os.path
import time
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pandas as pd
from datetime import datetime
import logging
from prophet import Prophet
from src.utils import create_folder, save_model, load_model
from pyspark.sql.functions import current_date
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType, StringType
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType
import sys
import numpy as np

BASE_DIR = "/opt/airflow/dags"

result_schema = StructType([
    StructField('ds', TimestampType()),
    StructField('ip', StringType()),
    StructField('y', DoubleType()),
    StructField('yhat', DoubleType()),
    StructField('yhat_upper', DoubleType()),
    StructField('yhat_lower', DoubleType())
])


def save_historical_data_as_dataframe(dataframe: pd.DataFrame, ip: str):
    """

    :param dataframe:
    :param ip:
    :return:
    """
    folder_path = os.path.join(BASE_DIR, "history")
    file_path = os.path.join(folder_path, f"{ip}.csv")
    dataframe = dataframe.sort_values(by="ds", ascending=True)
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        new_df = pd.concat([df, dataframe], ignore_index=True)
        new_df.to_csv(file_path, index=False)
        logging.info("Data file already exists.. New data appended..")
    else:
        create_folder(folder=folder_path)
        dataframe.to_csv(file_path, index=False)
        logging.info("Data file created..")


@pandas_udf(result_schema, PandasUDFType.GROUPED_MAP)
def forecast_ip(history_pd):
    model = Prophet(
        interval_width=0.95,
        growth='linear',
        daily_seasonality=True,
        seasonality_mode='multiplicative'
    )
    # fit the model
    model.fit(history_pd)

    # get fitted src ip to use it model file
    src_ip = history_pd['ip'].iloc[0]
    save_model(model=model, file=os.path.join(BASE_DIR, f'models/{g_train_type}/{src_ip}.json'))

    # configure predictions
    future_pd = model.make_future_dataframe(
        periods=5,
        freq='H',
        include_history=True
    )

    # make predictions
    results_pd = model.predict(future_pd)
    f_pd = results_pd[['ds', 'yhat', 'yhat_upper', 'yhat_lower']].set_index('ds')
    st_pd = history_pd[['ds', 'ip', 'y']].set_index('ds')

    result_pd = f_pd.join(st_pd, how='left')
    result_pd.reset_index(level=0, inplace=True)

    result_pd['ip'] = history_pd['ip'].iloc[0]
    save_historical_data_as_dataframe(history_pd, ip=src_ip)  # save old data to retrain model in the future
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


def train(data, train_type):
    global g_save_date
    global g_train_type

    print(data)
    # dataframe = kwargs["ti"].xcom_pull(task_ids='convert_to_dataframe')
    dataframe = pd.read_json(data, orient="columns")

    dataframe["ds"] = dataframe["ds"].apply(lambda x: datetime.fromtimestamp(x))

    if os.path.exists(os.path.join(BASE_DIR, f"models/{train_type}")):
        start = time.time()
        dataframe = dataframe.rename(columns={train_type: 'y'})
        for unq_src_ip in dataframe["ip"].unique():
            model = Prophet(
                interval_width=0.95,
                growth='linear',
                daily_seasonality=True,
                seasonality_mode='multiplicative'
            )

            ip_model_path = os.path.join(BASE_DIR, f"models/{train_type}/{unq_src_ip}.json")
            old_model = load_model(ip_model_path)
            history_data = dataframe.loc[dataframe['ip'] == unq_src_ip]
            print(f"History data for src: {unq_src_ip} -> {history_data}")
            new_model = model.fit(history_data, init=warm_start_params(old_model))

            save_model(new_model, ip_model_path)
            save_historical_data_as_dataframe(dataframe=history_data, ip=unq_src_ip)

            logging.info(f"New model for {unq_src_ip} saved successfully..")

        logging.info(f"Retraining operation successfully finished in {time.time() - start} seconds..")

    elif not dataframe.empty:
        print(f'Dataframe: {dataframe}')
        start = time.time()

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
        forecast_path = os.path.join(BASE_DIR, "forecasts")
        create_folder(folder=forecast_path)
        final_df.to_csv(os.path.join(forecast_path, f"{g_save_date}_forecast_{train_type}.csv"), index=False)

        logging.info(f"Training operation successfully finished in {time.time() - start} seconds..")

    else:
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
        train(data=sys.argv[1], train_type=sys.argv[2])
    except IndexError as err:
        # if sys argv does not come as expected, catch exception
        logging.error(err)
