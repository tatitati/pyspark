#!/usr/local/bin/python3

import pyspark.sql
from pyspark.sql import SparkSession

# start an spark session
spark = SparkSession.builder.appName("firstApp").getOrCreate()

# load data into a datafrom from a csv
# financial data here: https://finance.yahoo.com/quote/GOOG?p=GOOG&.tsrc=fin-srch
df = spark.read.csv("GOOG.csv", header=True, inferSchema=True)
print(df) # DataFrame[Date: string, Open: double, High: double, Low: double, Close: double, Adj Close: double, Volume: int]

# perform basic dataframe operations
print(df.columns) # ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']


