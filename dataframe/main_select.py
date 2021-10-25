#!/usr/local/bin/python3
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from urllib.request import Request, urlopen
import certifi
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James","Smith","USA","CA"),
        ("Michael","Rose","USA","NY"),
        ("Robert","Williams","USA","CA"),
        ("Maria","Jones","USA","FL")
]
columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data=data, schema=columns)
df.show(truncate=False)
# +---------+--------+-------+-----+
# |firstname|lastname|country|state|
# +---------+--------+-------+-----+
# |James    |Smith   |USA    |CA   |
# |Michael  |Rose    |USA    |NY   |
# |Robert   |Williams|USA    |CA   |
# |Maria    |Jones   |USA    |FL   |
# +---------+--------+-------+-----+

# select
df.select("firstname","lastname").show()
# df.select(df.firstname,df.lastname).show()
# df.select(df["firstname"],df["lastname"]).show()
# df.select(col("firstname"),col("lastname")).show()
# +---------+--------+
# |firstname|lastname|
# +---------+--------+
# |    James|   Smith|
# |  Michael|    Rose|
# |   Robert|Williams|
# |    Maria|   Jones|
# +---------+--------+

df.select(df.columns[:3]).show(3)
# +---------+--------+-------+
# |firstname|lastname|country|
# +---------+--------+-------+
# |    James|   Smith|    USA|
# |  Michael|    Rose|    USA|
# |   Robert|Williams|    USA|
# +---------+--------+-------+