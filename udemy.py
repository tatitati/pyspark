#!/usr/local/bin/python3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate() 

# Extract
mydata = spark.read.format("csv").option("header", "true").load("original.csv")
mydata.show()
# +---+----------+----------+------+---------------+--------------------+---------+----------+-----------+
# | id|first_name| last_name|gender|           City|            JobTitle|   Salary|  Latitude|  Longitude|
# +---+----------+----------+------+---------------+--------------------+---------+----------+-----------+
# |  1|   Melinde| Shilburne|Female|      Nowa Ruda| Assistant Professor|$57438.18|50.5774075| 16.4967184|
# |  2|  Kimberly|Von Welden|Female|         Bulgan|       Programmer II|$62846.60|48.8231572|103.5218199|
# |  3|    Alvera|  Di Boldi|Female|           null|                null|$57576.52|39.9947462|116.3397725|
# |  4|   Shannon| O'Griffin|  Male|  Divnomorskoye|Budget/Accounting...|$61489.23|44.5047212| 38.1300171|
# |  5|  Sherwood|   Macieja|  Male|      Mytishchi|            VP Sales|$63863.09|      null| 37.6489954|

# Transform (cleaning, etc)
# null city => "Unknown"
mydata2 = mydata.withColumn("clean_city", when(mydata.City.isNull(), "Unknown").otherwise(mydata.City))
mydata2.show()
# +---+----------+----------+------+---------------+--------------------+---------+----------+-----------+---------------+
# | id|first_name| last_name|gender|           City|            JobTitle|   Salary|  Latitude|  Longitude|     clean_city|
# +---+----------+----------+------+---------------+--------------------+---------+----------+-----------+---------------+
# |  1|   Melinde| Shilburne|Female|      Nowa Ruda| Assistant Professor|$57438.18|50.5774075| 16.4967184|      Nowa Ruda|
# |  2|  Kimberly|Von Welden|Female|         Bulgan|       Programmer II|$62846.60|48.8231572|103.5218199|         Bulgan|
# |  3|    Alvera|  Di Boldi|Female|           null|                null|$57576.52|39.9947462|116.3397725|        Unknown|
# |  4|   Shannon| O'Griffin|  Male|  Divnomorskoye|Budget/Accounting...|$61489.23|44.5047212| 38.1300171|  Divnomorskoye|
# |  5|  Sherwood|   Macieja|  Male|      Mytishchi|            VP Sales|$63863.09|      null| 37.6489954|      Mytishchi|

# null JobTitle => delete whole row
mydata2 = mydata2.filter(mydata2.JobTitle.isNotNull())
mydata2.show()
# +---+----------+----------+------+---------------+--------------------+---------+----------+-----------+---------------+
# | id|first_name| last_name|gender|           City|            JobTitle|   Salary|  Latitude|  Longitude|     clean_city|
# +---+----------+----------+------+---------------+--------------------+---------+----------+-----------+---------------+
# |  1|   Melinde| Shilburne|Female|      Nowa Ruda| Assistant Professor|$57438.18|50.5774075| 16.4967184|      Nowa Ruda|
# |  2|  Kimberly|Von Welden|Female|         Bulgan|       Programmer II|$62846.60|48.8231572|103.5218199|         Bulgan|
# |  4|   Shannon| O'Griffin|  Male|  Divnomorskoye|Budget/Accounting...|$61489.23|44.5047212| 38.1300171|  Divnomorskoye|
# |  5|  Sherwood|   Macieja|  Male|      Mytishchi|            VP Sales|$63863.09|      null| 37.6489954|      Mytishchi|
