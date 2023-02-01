# from KafkaAdapter import KafkaAdapter
import os
# import logging
# import requests
# from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

# adapter = KafkaAdapter()
# BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", default=["kafka:9092"])
SPARK_KAFKA_SERVER = "kafka-server:9092"
# SPARK_KAFKA_SERVER = os.getenv("SPARK_KAFKA_SERVER", default="kafka:9092")
# TOPIC_NAME = os.getenv("TOPIC_NAME", default=None)
TOPIC_NAME = "WeatherTopic"

"""
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
"""

spark = SparkSession \
    .builder \
    .appName('StreamProcessingDemo').getOrCreate()


struct = StructType(
    [
        StructField("time", StringType()),
        StructField("temperature", DoubleType()),
        StructField("feelsLikeTemp", DoubleType()),
        StructField("windSpeed", DoubleType()),
        StructField("windDir", DoubleType()),
        StructField("pressure", DoubleType()),
        StructField("symbolPhrase", StringType())
    ]
)


def read_stream_from_kafka():
    print(SPARK_KAFKA_SERVER)
    print(TOPIC_NAME)
    sdf = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", SPARK_KAFKA_SERVER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .option("delimiter", ",") \
        .load()

    print(sdf.isStreaming)  # Returns True for DataFrames that have streaming sources

    sdf.printSchema()

    df1 = sdf.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), struct).alias("data")).select(
        "data.*")
    df1.printSchema()
    df1.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


# def read_batch_from_kafka():
#     bdf = spark \
#         .read \
#         .format('kafka') \
#         .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS) \
#         .option('subscribe', TOPIC_NAME) \
#         .load()


def write_to_cassandra(write_df, _):
    write_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="weather", keyspace="sp") \
        .save()


if __name__ == '__main__':
    read_stream_from_kafka()
