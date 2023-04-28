import glob
from pyspark.context import SparkContext, SparkConf
from pyspark.ml.regression import GBTRegressor
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Window as w
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.sql import SparkSession
import pandas as pd
import os
from KafkaAdapter import KafkaAdapter
from pyspark.sql.functions import lag

SPARK_KAFKA_SERVER = os.getenv("SPARK_KAFKA_SERVER", default="kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", default=None)
adapter = KafkaAdapter()

struct = StructType(
    [
        StructField("ip", StringType()),
        StructField("ds", StringType()),
        StructField("y", StringType())
    ]
)


def concat_ip_histories(folder_name):
    files = glob.glob(folder_name + "/*.csv")
    if not files:
        print("Could not find any history of data..")
    else:
        dataframe = pd.DataFrame(columns=["ip", "ds", "y"])
        for file in files:
            dataframe = pd.concat([dataframe, pd.read_csv(file)], ignore_index=True)
        print(f"Concatenated history dataframe: {dataframe}")
        dataframe.to_csv("/opt/airflow/dags/raw_data.csv", index=False)


def create_spark_ml(prev_col_name="prevHourUnqDstIp"):
    if os.path.exists("/opt/airflow/dags/raw_data.csv"):
        sdf = spark \
            .read \
            .format("csv") \
            .schema(schema=struct) \
            .option("header", True) \
            .option("timestampFormat", 'yyyy/MM/dd hh:mm:ss') \
            .load("/opt/airflow/dags/raw_data.csv")

        print(sdf)

        sdf.printSchema()

        # dataframe = pd.read_json(data, orient="columns")  # get data from convert_to_dataframe task
        #
        # sdf = spark.createDataFrame(data=dataframe)
        sdf = sdf.withColumn(prev_col_name, lag("y")
                             .over(w.partitionBy("ip")
                                   .orderBy(["ds"])))
        print("MODEL DATA:")
        sdf.show(50)
        print(f"Model data: {sdf}")

        indexer = StringIndexer(inputCol="ip", outputCol="ipNum")
        index_data = indexer.fit(sdf).transform(sdf)  # convert categorical data into numerical

        print(index_data)
        print("INDEX DATA:")
        index_data.show(50)

        encoder = OneHotEncoder(inputCol="ipNum", outputCol="ipVec")
        ohe_data = encoder.fit(index_data).transform(index_data)
        ohe_data.show(50)

        vector_assembler = VectorAssembler(
            inputCols=["ipNum", "ds", prev_col_name],
            outputCol="features"
        )
        out_data = vector_assembler.transform(ohe_data)
        out_data.show(50)

        (train, test) = out_data.randomSplit([0.75, 0.25])

        gbt = GBTRegressor(featuresCol="features", labelCol="y")
        model = gbt.fit(train)
        pred = model.transform(dataset=test)
        print(f"Predictions: {pred}")
    else:
        print("Could not find 'raw_data.csv' file..")


if __name__ == '__main__':
    sc = SparkContext(conf=SparkConf().setMaster("local"))
    spark = SparkSession \
        .builder \
        .config("spark.ui.port", "9090") \
        .appName('SparkMLDemo').getOrCreate()
    concat_ip_histories(folder_name="/opt/airflow/dags/history")
    create_spark_ml()
    spark.stop()
