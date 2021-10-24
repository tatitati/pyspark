#!/usr/local/bin/python3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)
# +-----+------------+
# |Seqno|Name        |
# +-----+------------+
# |1    |john jones  |
# |2    |tracey smith|
# |3    |amy sanders |
# +-----+------------+

def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr

def upperCase(str):
    return str.upper()

# Using UDF with PySpark DataFrame select()
convertUDF = udf(lambda z: convertCase(z),StringType())

df.select(col("Seqno"), \
    convertUDF(col("Name")).alias("Name") ) \
   .show(truncate=False)
# +-----+-------------+
# |Seqno|Name         |
# +-----+-------------+
# |1    |John Jones   |
# |2    |Tracey Smith |
# |3    |Amy Sanders  |
# +-----+-------------+

# Using UDF with PySpark DataFrame withColumn()
upperCaseUDF = udf(lambda z:upperCase(z),StringType())

df.withColumn("Cureated Name", upperCaseUDF(col("Name"))) \
  .show(truncate=False)
# +-----+------------+-------------+
# |Seqno|Name        |Cureated Name|
# +-----+------------+-------------+
# |1    |john jones  |JOHN JONES   |
# |2    |tracey smith|TRACEY SMITH |
# |3    |amy sanders |AMY SANDERS  |
# +-----+------------+-------------+

# Registering PySpark UDF & use it on SQL
spark.udf.register("convertUDF", convertCase,StringType())

df.createOrReplaceTempView("NAME_TABLE")
spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE") \
     .show(truncate=False)
# +-----+-------------+
# |Seqno|Name         |
# +-----+-------------+
# |1    |John Jones   |
# |2    |Tracey Smith |
# |3    |Amy Sanders  |
# +-----+-------------+
