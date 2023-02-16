import os.path
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pandas as pd
from datetime import datetime
import logging
from prophet import Prophet
from src.utils import create_folder, save_model, concat_models
from pyspark.sql.functions import current_date
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType, StringType
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType
import sys

BASE_DIR = "/opt/airflow/dags"

result_schema = StructType([
    StructField('ds', TimestampType()),
    StructField('ip', StringType()),
    StructField('y', DoubleType()),
    StructField('yhat', DoubleType()),
    StructField('yhat_upper', DoubleType()),
    StructField('yhat_lower', DoubleType())
])


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
    save_model(model=model, file=os.path.join(BASE_DIR, f'models/{g_train_type}/{g_save_date}/{src_ip}.json'))

    # configure predictions
    future_pd = model.make_future_dataframe(
        periods=1,
        freq='M',
        include_history=True
    )

    # make predictions
    results_pd = model.predict(future_pd)
    f_pd = results_pd[['ds', 'yhat', 'yhat_upper', 'yhat_lower']].set_index('ds')
    st_pd = history_pd[['ds', 'ip', 'y']].set_index('ds')

    result_pd = f_pd.join(st_pd, how='left')
    result_pd.reset_index(level=0, inplace=True)

    return result_pd[['ds', 'ip', 'y', 'yhat', 'yhat_upper', 'yhat_lower']]


def train(data, train_type):
    global g_save_date
    global g_train_type

    # dataframe = kwargs["ti"].xcom_pull(task_ids='convert_to_dataframe')
    dataframe = pd.read_json(data, orient="columns")

    print(dataframe.empty)

    if not dataframe.empty:
        print(f'dataframe: {dataframe}')
        g_save_date = f"{datetime.now():%y%m%d_%H%M}"
        g_train_type = train_type

        folder_path = os.path.join(BASE_DIR, f'models/{train_type}/{g_save_date}')
        create_folder(folder=folder_path)

        dataframe['ds'] = pd.to_datetime(dataframe['ds'], infer_datetime_format=True)
        sdf = spark.createDataFrame(dataframe)
        sdf.createOrReplaceTempView(train_type)

        sql = f"SELECT ip, ds, sum({train_type}) as y FROM {train_type} GROUP BY ip, ds ORDER BY ip, ds"

        src_part = (spark.sql(sql).repartition(spark.sparkContext.defaultParallelism, ['ip'])).cache()
        results = (src_part.groupby('ip').apply(forecast_ip).withColumn('training_date', current_date()))
        results.cache()

        results.coalesce(1)

        results.createOrReplaceTempView('forecasted')

        concat_models(folder=folder_path, train_info=f"Saved date: {g_save_date} Train type: {g_train_type}",
                      save_file=f"{folder_path}.json")
        # spark.sql("SELECT ip, count(*) FROM forecasted GROUP BY ip").show()
        final_df = results.toPandas()
        forecast_path = os.path.join(BASE_DIR, "forecasts")
        create_folder(folder=forecast_path)
        final_df.to_csv(os.path.join(forecast_path, f"{g_save_date}_forecast_{train_type}.csv"), index=False)


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
