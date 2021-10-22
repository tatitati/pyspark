from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from urllib.request import Request, urlopen
import certifi

sc = SparkContext(master="local[*]", appName= "readJSON")
spark = SparkSession.builder.getOrCreate()