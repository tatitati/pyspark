#!/usr/local/bin/python3

import json
import pandas as pd
from json_flatten import flatten
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

# with open('dataset.json') as f:
d = json.loads("""{
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
   "unitCount":"3"
}""")

flat_json_dict = [flatten(d) for i in d]
print(flat_json_dict)
# [
#   {
#     "ProductNum": "6000078",
#     "Properties.[0].key": "invoice_id",
#     "Properties.[0].value": "923659",
#     "Properties.[1].key": "job_id",
#     "Properties.[1].value": "296160",
#     "Properties.[2].key": "sku_id",
#     "Properties.[2].value": "312002",
#     "unitCount": "3"
#   },
#   {
#     "ProductNum": "6000078",
#     "Properties.[0].key": "invoice_id",
#     "Properties.[0].value": "923659",
#     "Properties.[1].key": "job_id",
#     "Properties.[1].value": "296160",
#     "Properties.[2].key": "sku_id",
#     "Properties.[2].value": "312002",
#     "unitCount": "3"
#   },
#   {
#     "ProductNum": "6000078",
#     "Properties.[0].key": "invoice_id",
#     "Properties.[0].value": "923659",
#     "Properties.[1].key": "job_id",
#     "Properties.[1].value": "296160",
#     "Properties.[2].key": "sku_id",
#     "Properties.[2].value": "312002",
#     "unitCount": "3"
#   }
# ]

# df = pd.DataFrame(flat_json_dict)
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rddJson = spark.sparkContext.parallelize([flat_json_dict])
df = spark.read.json(rddJson)


print(df.show())
# +----------+------------------+--------------------+------------------+--------------------+------------------+--------------------+---------+
# |ProductNum|Properties.[0].key|Properties.[0].value|Properties.[1].key|Properties.[1].value|Properties.[2].key|Properties.[2].value|unitCount|
# +----------+------------------+--------------------+------------------+--------------------+------------------+--------------------+---------+
# |   6000078|        invoice_id|              923659|            job_id|              296160|            sku_id|              312002|        3|
# |   6000078|        invoice_id|              923659|            job_id|              296160|            sku_id|              312002|        3|
# |   6000078|        invoice_id|              923659|            job_id|              296160|            sku_id|              312002|        3|
# +----------+------------------+--------------------+------------------+--------------------+------------------+--------------------+---------+

print(df.columns)
# [
#     'ProductNum',
#     'Properties.[0].key',
#     'Properties.[0].value',
#     'Properties.[1].key',
#     'Properties.[1].value',
#     'Properties.[2].key',
#     'Properties.[2].value',
#     'unitCount'
# ]