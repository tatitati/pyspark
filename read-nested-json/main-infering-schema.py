#!/usr/local/bin/python3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("JSONFileRead").master("local").getOrCreate()

df = spark.read.option("multiLine", True).json("dataset-infering-schema.json")
df.printSchema()
# root
#  |-- Education: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- Qualification: string (nullable = true)
#  |    |    |-- year: long (nullable = true)
#  |-- name: string (nullable = true)

print(df.schema.json())
# {"fields":[{"metadata":{},"name":"Education","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"Qualification","nullable":true,"type":"string"},{"metadata":{},"name":"year","nullable":true,"type":"long"}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"name","nullable":true,"type":"string"}],"type":"struct"}


df.show()
# +--------------------+-------+
# |           Education|   name|
# +--------------------+-------+
# |[{BE, 2011}, {ME,...| Clarke|
# |        [{BE, 2010}]|Michael|
# +--------------------+-------+

flat=df.select(
    'name',
    explode('Education').alias('education_flat')
)
flat.show()
# +-------+--------------+
# |   name|education_flat|
# +-------+--------------+
# | Clarke|    {BE, 2011}|
# | Clarke|    {ME, 2013}|
# |Michael|    {BE, 2010}|
# +-------+--------------+

out_df=flat.select(
    'name',
    'education_flat.Qualification', 
    'education_flat.year'
)
out_df.show()
# +-------+-------------+----+
# |   name|Qualification|year|
# +-------+-------------+----+
# | Clarke|           BE|2011|
# | Clarke|           ME|2013|
# |Michael|           BE|2010|
# +-------+-------------+----+