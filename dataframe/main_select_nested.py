#!/usr/local/bin/python3
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from urllib.request import Request, urlopen
import certifi
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType

data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]


schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df2 = spark.createDataFrame(data = data, schema = schema)

df2.printSchema()
# root
#  |-- name: struct (nullable = true)
#  |    |-- firstname: string (nullable = true)
#  |    |-- middlename: string (nullable = true)
#  |    |-- lastname: string (nullable = true)
#  |-- state: string (nullable = true)
#  |-- gender: string (nullable = true)

df2.show(truncate=False) # shows all columns
# +----------------------+-----+------+
# |name                  |state|gender|
# +----------------------+-----+------+
# |{James, null, Smith}  |OH   |M     |
# |{Anna, Rose, }        |NY   |F     |
# |{Julia, , Williams}   |OH   |F     |
# |{Maria, Anne, Jones}  |NY   |M     |
# |{Jen, Mary, Brown}    |NY   |M     |
# |{Mike, Mary, Williams}|OH   |M     |
# +----------------------+-----+------+

df2.select("name").show(truncate=False)
# +----------------------+
# |name                  |
# +----------------------+
# |{James, null, Smith}  |
# |{Anna, Rose, }        |
# |{Julia, , Williams}   |
# |{Maria, Anne, Jones}  |
# |{Jen, Mary, Brown}    |
# |{Mike, Mary, Williams}|
# +----------------------+

df2.select("name.firstname","name.lastname").show(truncate=False)
# +---------+--------+
# |firstname|lastname|
# +---------+--------+
# |James    |Smith   |
# |Anna     |        |
# |Julia    |Williams|
# |Maria    |Jones   |
# |Jen      |Brown   |
# |Mike     |Williams|
# +---------+--------+

df2.select("name.*").show(truncate=False)
# +---------+----------+--------+
# |firstname|middlename|lastname|
# +---------+----------+--------+
# |James    |null      |Smith   |
# |Anna     |Rose      |        |
# |Julia    |          |Williams|
# |Maria    |Anne      |Jones   |
# |Jen      |Mary      |Brown   |
# |Mike     |Mary      |Williams|
# +---------+----------+--------+