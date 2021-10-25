#!/usr/local/bin/python3

# UDEMY URL: https://www.udemy.com/course/pyspark-build-dataframes-with-python-apache-spark-and-sql/learn/lecture/25144674#overview

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number

# start an spark session
# =================================
app = SparkSession.builder.appName("firstApp").getOrCreate()

# load data into a dataframe from a csv
# =================================

# financial data here: https://finance.yahoo.com/quote/GOOG?p=GOOG&.tsrc=fin-srch
df = app.read.csv("GOOG.csv", header=True, inferSchema=True)

print(df)
# DataFrame[Date: string, Open: double, High: double, Low: double, Close: double, Adj Close: double, Volume: int]

# perform basic dataframe operations
# =================================

print(df.columns)
# ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']

df.printSchema()
# root
#  |-- Date: string (nullable = true)
#  |-- Open: double (nullable = true)
#  |-- High: double (nullable = true)
#  |-- Low: double (nullable = true)
#  |-- Close: double (nullable = true)
#  |-- Adj Close: double (nullable = true)
#  |-- Volume: integer (nullable = true)

for row in df.head(5):
    print(row)
# Row(Date='2016-08-09', Open=781.099976, High=788.940002, Low=780.570007, Close=784.26001, Adj Close=784.26001, Volume=1318900)
# Row(Date='2016-08-10', Open=783.75, High=786.812012, Low=782.778015, Close=784.679993, Adj Close=784.679993, Volume=786400)
# Row(Date='2016-08-11', Open=785.0, High=789.75, Low=782.969971, Close=784.849976, Adj Close=784.849976, Volume=975100)
# Row(Date='2016-08-12', Open=781.5, High=783.39502, Low=780.400024, Close=783.219971, Adj Close=783.219971, Volume=740500)
# Row(Date='2016-08-15', Open=783.75, High=787.48999, Low=780.109985, Close=782.440002, Adj Close=782.440002, Volume=938200)

df.describe().show()
# +-------+----------+------------------+------------------+------------------+------------------+------------------+------------------+
# |summary|      Date|              Open|              High|               Low|             Close|         Adj Close|            Volume|
# +-------+----------+------------------+------------------+------------------+------------------+------------------+------------------+
# |  count|      1258|              1258|              1258|              1258|              1258|              1258|              1258|
# |   mean|      null|1273.8954070373602|1286.4667442257555|1262.4356319737662|1275.0457098521456|1275.0457098521456|1592491.1764705882|
# | stddev|      null| 442.8594046043667|447.52195525144407| 439.4064822385544| 443.8874282856768| 443.8874282856768| 697243.6245150303|
# |    min|2016-08-09|        744.590027|             754.0|        727.539978|        736.080017|        736.080017|            346800|
# |    max|2021-08-06|       2800.219971|       2800.219971|        2753.02002|       2792.889893|       2792.889893|           6207000|
# +-------+----------+------------------+------------------+------------------+------------------+------------------+------------------+

# format df table
# =================================

description = df.describe()
description.select(
    description["summary"],
    format_number(description["Open"].cast("float"),2).alias("Opened"),
    format_number(description["High"].cast("float"),2).alias("Highed")
).show()
# +-------+--------+--------+
# |summary|  Opened|  Highed|
# +-------+--------+--------+
# |  count|1,258.00|1,258.00|
# |   mean|1,273.90|1,286.47|
# | stddev|  442.86|  447.52|
# |    min|  744.59|  754.00|
# |    max|2,800.22|2,800.22|
# +-------+--------+--------+


# perform df math
# =================================

new_df = df.withColumn("OV ratio", df["Open"]/df["Volume"])
new_df.show()

# +----------+----------+----------+----------+----------+----------+-------+--------------------+
# |      Date|      Open|      High|       Low|     Close| Adj Close| Volume|            OV ratio|
# +----------+----------+----------+----------+----------+----------+-------+--------------------+
# |2016-08-09|781.099976|788.940002|780.570007| 784.26001| 784.26001|1318900|5.922359360072787E-4|
# |2016-08-10|    783.75|786.812012|782.778015|784.679993|784.679993| 786400|9.966302136317396E-4|
# |2016-08-11|     785.0|    789.75|782.969971|784.849976|784.849976| 975100|8.050456363449903E-4|
# |2016-08-12|     781.5| 783.39502|780.400024|783.219971|783.219971| 740500|0.001055367994598...|
# ....

new_df.select("OV ratio").show()

# +--------------------+
# |            OV ratio|
# +--------------------+
# |5.922359360072787E-4|
# |9.966302136317396E-4|
# |8.050456363449903E-4|
# |0.001055367994598...|

df.orderBy(df["High"].desc()).show()

# +----------+-----------+-----------+-----------+-----------+-----------+-------+
# |      Date|       Open|       High|        Low|      Close|  Adj Close| Volume|
# +----------+-----------+-----------+-----------+-----------+-----------+-------+
# |2021-07-27|2800.219971|2800.219971|     2702.0|2735.929932|2735.929932|2108200|
# |2021-07-26|     2765.0| 2794.26001| 2753.02002|2792.889893|2792.889893|1152600|
# |2021-07-28| 2771.23999| 2793.52002|     2727.0|2727.629883|2727.629883|2734400|
# |2021-07-23|2705.199951|2776.169922| 2694.01001|2756.320068|2756.320068|1318900|

# Build SQL queries
# =================================

df.createOrReplaceTempView("stock")

result = app.sql("select Date, Low from stock limit 3")
print(result)
# DataFrame[Date: string, Low: double]

app.sql("select Date, Low from stock limit 3").show()
# +----------+----------+
# |      Date|       Low|
# +----------+----------+
# |2016-08-09|780.570007|
# |2016-08-10|782.778015|
# |2016-08-11|782.969971|
# +----------+----------+