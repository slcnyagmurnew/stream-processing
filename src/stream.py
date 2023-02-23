import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, avg, to_json

SPARK_KAFKA_SERVER = os.getenv("SPARK_KAFKA_SERVER", default="kafka:9092")
SPARK_KAFKA_TOPIC = os.getenv("SPARK_KAFKA_TOPIC", default=None)
TOPIC_NAME = os.getenv("TOPIC_NAME", default=None)

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
        StructField("src_ip", StringType()),
        StructField("timestamp", StringType()),
        StructField("unq_dst_ip", StringType()),
        StructField("allow", StringType()),
        StructField("drop", StringType()),
        StructField("frequency", StringType()),
        StructField("pkts_sent", StringType()),
        StructField("pkts_received", StringType()),
        StructField("bytesin", StringType()),
        StructField("bytesout", StringType()),
        StructField("unq_dst_port", StringType())
    ]
)


def read_stream_from_kafka():
    sdf = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", SPARK_KAFKA_SERVER) \
        .option("group.id", "SparkStreamGroup") \
        .option("subscribe", TOPIC_NAME) \
        .option("delimiter", ",") \
        .load()

    print(sdf.isStreaming)  # Returns True for DataFrames that have streaming sources

    sdf.printSchema()

    # It is necessary that convert data as string format for using in expressions
    sdf = sdf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .select(from_json(col("value"), struct)  # get all structured data into dataframe
                .alias("data"))  # access column names with data.$column_name

    sdf.printSchema()

    sdf = sdf.groupby("data.src_ip").agg(avg("data.unq_dst_ip").alias("value"))  # group by with column

    sdf.printSchema()

    """
    Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    Use "key" word to set "value" data as Kafka key field
    "value" keyword is required to run query, it searches for "value" column
    """
    sdf.select(col("src_ip").alias("key"), col("value").alias("value").cast("string"))  \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", SPARK_KAFKA_SERVER) \
        .option("topic", SPARK_KAFKA_TOPIC) \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .partitionBy("key") \
        .trigger(processingTime='10 minutes') \
        .outputMode("update") \
        .start() \
        .awaitTermination()

    """ DIRECT WRITE TO CONSOLE """
    # df1.writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start() \
    #     .awaitTermination()

    """TO CONSOLE WITH 10 MINI BATCHING"""
    # df1.writeStream \
    #     .format("console") \
    #     .outputMode("update") \
    #     .trigger(processingTime='10 minutes') \
    #     .start() \
    #     .awaitTermination()

    """WRITE TO REDIS"""
    # df1.writeStream \
    #     .foreachBatch(foreach_batch_to_redis) \
    #     .start() \
    #     .awaitTermination()


def write_to_cassandra(write_df, _):
    write_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="weather", keyspace="sp") \
        .save()


if __name__ == '__main__':
    read_stream_from_kafka()
