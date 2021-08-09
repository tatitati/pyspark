#!/usr/local/bin/python3

import pyspark.sql
from pyspark.sql import SparkSession

# start an spark session
spark = SparkSession.builder.appName("firstApp").getOrCreate()





