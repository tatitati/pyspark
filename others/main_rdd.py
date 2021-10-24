#!/usr/local/bin/python3
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from urllib.request import Request, urlopen
import certifi

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sparkContext=spark.sparkContext

rdd=sparkContext.parallelize([1,2,3,4,5])

print("Number of Partitions: "+str(rdd.getNumPartitions()))
# Number of Partitions: 8

print("Action: First element: "+str(rdd.first()))
# Action: First element: 1

rddCollect = rdd.collect()
print(rddCollect)
# [1, 2, 3, 4, 5]