#!/usr/local/bin/python3

import pyspark.sql
from pyspark.sql import SparkSession

# start an spark session
# =================================
spark = SparkSession.builder.appName("firstApp").getOrCreate()

# load data into a datafrom from a csv
# =================================

# financial data here: https://finance.yahoo.com/quote/GOOG?p=GOOG&.tsrc=fin-srch
df = spark.read.csv("GOOG.csv", header=True, inferSchema=True)

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