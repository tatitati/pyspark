#!/usr/local/bin/python3s
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from urllib.request import Request, urlopen
import certifi

sc = SparkContext(master="local[*]", appName= "readJSON")
spark = SparkSession.builder.getOrCreate()

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
rdd = sc.parallelize([httpData])

# create a Dataframe
jsonDF = spark.read.json(rdd)
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
