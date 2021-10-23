#!/usr/local/bin/python3s
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from urllib.request import Request, urlopen
from pyspark.sql.types import *
import certifi

context = SparkContext(master="local[*]", appName="readJSON")
session = SparkSession.builder.getOrCreate()

# Online data source
onlineData = 'https://randomuser.me/api/0.8/?results=10'

# read the online data file
httpData = urlopen(onlineData, cafile=certifi.where()).read().decode('utf-8')
print(httpData)
# {
#     "results": [
#         {
#             "user": {
#                 "gender": "female",
#                 "name": {
#                     "title": "ms",
#                     "first": "leana",
#                     "last": "robin"
#                 },
#                 "location": {
#                     "street": "8396 rue paul-duvivier",
#                     "city": "tours",
#                     "state": "bouches-du-rhône",
#                     "zip": 85991
#                 },
#    ],
#   "nationality": "FI",
#   "seed": "783efc3ebdeb71f606",
#   "version": "0.8"
# }

# convert into RDD
rdd = context.parallelize([httpData])
print(rdd)
# ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274

# create a Dataframe
jsonDF = session.read.json(rdd)
print(jsonDF.printSchema())
# schema = StructType([
#     StructField("results", ArrayType(
#         StructField("user", StructType([
#                 StructField("gender", StringType()),
#                 StructField("name", StructType([
#                     StructField("title", StringType()),
#                     StructField("first", StringType()),
#                     StructField("last", StringType()),
#                 ])),
#                 StructField("location", StructType([
#                     StructField("street", StringType()),
#                     StructField("city", StringType()),
#                     StructField("state", StringType()),
#                     StructField("zip", StringType()),
#                 ]))
#             ]))
#         )
#     ),
#     StructField("nationality", StringType()),
#     StructField("seed", StringType()),
#     StructField("version", FloatType())
# ])

# schema = StructType([
#     StructField("results", StringType()),
#     StructField("nationality", StringType()),
#     StructField("seed", StringType()),
#     StructField("version", FloatType())
# ])

jsonDF = session.read.schema(schema).json(rdd)
jsonDF.show()
# +-----------+--------------------+------------------+-------+
# |nationality|             results|              seed|version|
# +-----------+--------------------+------------------+-------+
# |         BR|[{{(36) 9066-5656...|3fd31e560b5f5ea901|    0.8|
# +-----------+--------------------+------------------+-------+

# read all the users name:
readUser = jsonDF\
    .withColumn('Exp_Results',F.explode('results'))\
    .select('Exp_Results.user.name.*')

readUser.show(truncate=False)
# +---------+--------+-----+
# |first    |last    |title|
# +---------+--------+-----+
# |daniele  |gomes   |mrs  |
# |telmo    |moraes  |mr   |
# |daisy    |da mota |miss |
# |nélio    |lima    |mr   |
