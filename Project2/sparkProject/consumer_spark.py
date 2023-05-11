import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark.sql.functions import mean, min, max, col, count, round
from pyspark.sql.functions import concat, to_timestamp, lit
import sys


# Prva tabela
def writeToCassandra1(writeDF, _):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="statistika", keyspace="spark_keyspace") \
        .save()
    writeDF.show()

# Druga tabela
def writeToCassandra2(writeDF, _):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="top_locations", keyspace="spark_keyspace") \
        .save()
    writeDF.show()

conf = SparkConf()
conf.set("spark.cassandra.connection.host", "cassandra")
conf.set("spark.cassandra.connection.port", 9042)
spark = SparkSession.builder.config(conf=conf).appName("Projekat2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "locations")
        .option("startingOffsets", "latest")
        .load()
    )
schema = StructType(
    [
      #  StructField("date", StringType()),
      #  StructField("time", StringType()),
    #  StructField("busID", StringType()),
     #   StructField("busLine", StringType()),
      #  StructField("latitude", StringType()),
       ##StructField("speed", StringType()),
    ]
)