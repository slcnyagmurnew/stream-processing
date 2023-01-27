import os

from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession


BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', default=['kafka:9092'])
TOPIC_NAME = os.getenv('TOPIC_NAME', default=None)
spark = SparkSession.builder.master('local').getOrCreate()
struct = StructType(
    [
        StructField("text", StringType()),
        StructField("favorites")
    ]
)



def read_stream_from_kafka():
    df = spark\
        .readStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS)\
        .option('subscribe', TOPIC_NAME)\
        .load()

