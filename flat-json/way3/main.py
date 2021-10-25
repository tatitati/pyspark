#!/usr/bin/env python

from pyspark.sql import SparkSession
import requests
import json
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import Row
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness 
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
from pyspark.sql.functions import variance,var_samp,  var_pop

DS_Products = """{
   "ProductNum":"6000078",
   "Properties":[
      {
         "key":"invoice_id",
         "value":"923659"
      },
      {
         "key":"job_id",
         "value":"296160"
      },
      {
         "key":"sku_id",
         "value":"312002"
      }
   ],
   "UnitCount":"3"
}"""

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rddJson = spark.sparkContext.parallelize([DS_Products])
DF_Products = spark.read.json(rddJson)

# ## Flatten this json

df_flatten = DF_Products\
    .select("*", explode("Properties").alias("SubContent"))\
    .drop("Properties")
df_flatten.show()
# +----------+---------+--------------------+
# |ProductNum|UnitCount|          SubContent|
# +----------+---------+--------------------+
# |   6000078|        3|{invoice_id, 923659}|
# |   6000078|        3|    {job_id, 296160}|
# |   6000078|        3|    {sku_id, 312002}|
# +----------+---------+--------------------+

df_flatten_pivot = df_flatten\
    .groupBy("ProductNum","UnitCount")\
    .pivot("SubContent.key")\
    .agg(first("SubContent.value"))
df_flatten_pivot.show()
# +----------+---------+----------+------+------+
# |ProductNum|UnitCount|invoice_id|job_id|sku_id|
# +----------+---------+----------+------+------+
# |   6000078|        3|    923659|296160|312002|
# +----------+---------+----------+------+------+


