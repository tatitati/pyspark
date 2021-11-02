#!/usr/local/bin/python3s
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from urllib.request import Request, urlopen
from pandas.io.json import json_normalize
from pyspark.sql.types import *
import certifi

context = SparkContext(master="local[*]", appName="readJSON")
spark = SparkSession.builder.getOrCreate()

# Online data source
onlineData = 'http://api.open-notify.org/iss-now.json'

# read the online data file
httpData = urlopen(onlineData).read().decode('utf-8')
print(httpData)
# {"iss_position": {"latitude": "-42.6785", "longitude": "111.3685"}, "message": "success", "timestamp": 1635089061}

schema = StructType([
        StructField("iss_position", StructType([
            StructField("latitude", StringType()),
            StructField("longitude", StringType()) # --> I can specify exactly the fields I want to pick up in the dataframe, so I dont need to specify all the fields
        ])),
        StructField("message", StringType()),
        StructField("timestamp", LongType())
])

# convert into RDD
rdd = context.parallelize([httpData])
print(rdd)
# ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274

jsonDF = spark.read.json(rdd, schema=schema)
jsonDF.printSchema()
# root
#  |-- iss_position: struct (nullable = true)
#  |    |-- latitude: string (nullable = true)
#  |    |-- longitude: string(nullable=true)
#  |-- message: string (nullable = true)
#  |-- timestamp: long (nullable = true)

jsonDF.show()
# +-------------------+-------+----------+
# |       iss_position|message| timestamp|
# +-------------------+-------+----------+
# |{-38.6722, 47.6465}|success|1635105895|
# +-------------------+-------+----------+

print(jsonDF.select("iss_position.latitude").show())
# +--------+
# |latitude|
# +--------+
# | 11.9283|
# +--------+

# flat
df = jsonDF.select("iss_position")
df.show()
# +--------------------+
# |        iss_position|
# +--------------------+
# |{-51.6238, 122.1086}|
# +--------------------+