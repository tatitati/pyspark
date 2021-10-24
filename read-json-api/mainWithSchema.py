#!/usr/local/bin/python3s
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from urllib.request import Request, urlopen
from pyspark.sql.types import *
import certifi

context = SparkContext(master="local[*]", appName="readJSON")
spark = SparkSession.builder.getOrCreate()

# Online data source
onlineData = 'http://api.open-notify.org/iss-now.json'

# read the online data file
httpData = urlopen(onlineData).read().decode('utf-8')
print(httpData)

schema = StructType([
        StructField("iss_position", StructType([
            StructField("latitude", StringType())
            # StructField("longitude", StringType()) # --> I can specify exactly the fields I want to pick up in the dataframe, so I dont need to specify all the fields
        ])),
        StructField("message", StringType()),
        StructField("timestamp", LongType())
])

# convert into RDD
rdd = context.parallelize([httpData])
print(rdd)
jsonDF = spark.read.json(rdd, schema=schema)
jsonDF.printSchema()
jsonDF.show()