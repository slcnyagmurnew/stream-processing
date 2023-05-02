from pyspark.context import SparkContext, SparkConf
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import Window as w
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.sql import SparkSession
import os
from utils import create_folder
from pyspark.ml import Pipeline
from KafkaAdapter import KafkaAdapter
from pyspark.sql.functions import lag

SPARK_KAFKA_SERVER = os.getenv("SPARK_KAFKA_SERVER", default="kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", default=None)
adapter = KafkaAdapter()

BASE_DIR = "/opt/airflow/src"
global model_path

struct = StructType(
    [
        StructField("src_ip", StringType()),
        StructField("unq_dst_ip", IntegerType()),
        StructField("allow", IntegerType()),
        StructField("drop", IntegerType()),
        StructField("frequency", IntegerType()),
        StructField("pkts_sent", IntegerType()),
        StructField("pkts_received", IntegerType()),
        StructField("bytesin", IntegerType()),
        StructField("bytesout", IntegerType()),
        StructField("unq_dst_port", IntegerType()),
        StructField("timestamp", TimestampType())
    ]
)


def eval_metrics(pred):
    """
    Evaluate GBT Regression model
    Calculate RMSE and MAE scores for predicted data
    :param pred:
    :return:
    """
    rmse_calc = RegressionEvaluator(
        metricName="rmse",
        predictionCol="prediction",
        labelCol="unq_dst_ip"
    )

    mae_calc = RegressionEvaluator(
        metricName="mae",
        predictionCol="prediction",
        labelCol="unq_dst_ip"
    )

    rmse_score = rmse_calc.evaluate(pred)
    mae_score = mae_calc.evaluate(pred)

    print(f"RMSE Score: {rmse_score}")
    print(f"MAE Score: {mae_score}")


def create_spark_ml(prev_col_name="prev_unq_dst_ip"):
    """
    Create Spark ML pipeline for existing log data (in csv format)
    It is assumed that data file always changes (in my case data does not change, no data flow exists)
    Or windowing function can be implemented to data (ignoring past data, focusing new data etc.)
    :param prev_col_name: lag function used in this column
    :return:
    """
    sdf = spark \
        .read \
        .format("csv") \
        .schema(schema=struct) \
        .option("header", True) \
        .option("timestampFormat", 'yyyy-MM-dd hh:mm:ss') \
        .load("/opt/airflow/data/logs.csv")

    print(sdf)

    sdf.printSchema()

    sdf = sdf.withColumn(prev_col_name, lag("unq_dst_ip")
                         .over(w.partitionBy("src_ip").orderBy(
        ["timestamp"])))  # group by src ip and add previous value as a new column with lag function

    sdf.show(50)

    (train, test) = sdf.randomSplit([0.75, 0.25])

    indexer = StringIndexer(inputCol="src_ip", outputCol="ip_num",
                            handleInvalid='keep')  # convert categorical data into numerical

    # index_data.show(50)

    encoder = OneHotEncoder(inputCol="ip_num", outputCol="ip_vec")

    vector_assembler = VectorAssembler(
        inputCols=["ip_vec", prev_col_name, "allow", "drop", "frequency", "pkts_sent",
                   "pkts_received", "bytesin", "bytesout", "unq_dst_port"],
        outputCol="features"  # include all columns except timestamp as feature
    )
    vector_assembler.setHandleInvalid("skip")  # Spark throws error when there is null value, so skip them

    # out_data.show(50)
    gbt = GBTRegressor(featuresCol="features", labelCol="unq_dst_ip")

    pipeline = Pipeline(stages=[indexer, encoder, vector_assembler, gbt])  # create pipeline

    model = pipeline.fit(train)

    model.write().overwrite().save(model_path)  # if path exists, Spark throws error without overwrite function

    pred = model.transform(dataset=test)
    eval_metrics(pred=pred)
    pred.show(50)


if __name__ == '__main__':
    sc = SparkContext(conf=SparkConf().setMaster("local"))
    spark = SparkSession \
        .builder \
        .appName('SparkMLDemo').getOrCreate()
    model_path = os.path.join(BASE_DIR, "model")
    create_folder(folder=model_path)
    create_spark_ml()
