#!/usr/local/bin/python3s
from pyspark.sql import SparkSession

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