#!/usr/local/bin/python3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate() 

# json example
# {
#   "RecordNumber": 1,
#   "Zipcode": 704,
#   "ZipCodeType": "STANDARD",
#   "City": "PARC PARQUE",
#   "State": "PR",
#   "LocationType": "NOT ACCEPTABLE",
#   "Lat": 17.96,
#   "Long": -66.22,
#   "Xaxis": 0.38,
#   "Yaxis": -0.87,
#   "Zaxis": 0.3,
#   "WorldRegion": "NA",
#   "Country": "US",
#   "LocationText": "Parc Parque, PR",
#   "Location": "NA-US-PR-PARC PARQUE",
#   "Decommisioned": false
# }
# =========================

df = spark.read.json("dataset1.json")
df.printSchema()
# root
#  |-- City: string (nullable = true)
#  |-- Country: string (nullable = true)
#  |-- Decommisioned: boolean (nullable = true)
#  |-- EstimatedPopulation: long (nullable = true)
#  |-- Lat: double (nullable = true)
#  |-- Location: string (nullable = true)
#  |-- LocationText: string (nullable = true)
#  |-- LocationType: string (nullable = true)
#  |-- Long: double (nullable = true)
#  |-- Notes: string (nullable = true)
#  |-- RecordNumber: long (nullable = true)
#  |-- State: string (nullable = true)
#  |-- TaxReturnsFiled: long (nullable = true)
#  |-- TotalWages: long (nullable = true)
#  |-- WorldRegion: string (nullable = true)
#  |-- Xaxis: double (nullable = true)
#  |-- Yaxis: double (nullable = true)
#  |-- Zaxis: double (nullable = true)
#  |-- ZipCodeType: string (nullable = true)
#  |-- Zipcode: long (nullable = true)

df.show()
# +-------------------+-------+-------------+-------------------+-----+--------------------+--------------------+--------------+-------+-------------+------------+-----+---------------+----------+-----------+-----+-----+-----+-----------+-------+
# |               City|Country|Decommisioned|EstimatedPopulation|  Lat|            Location|        LocationText|  LocationType|   Long|        Notes|RecordNumber|State|TaxReturnsFiled|TotalWages|WorldRegion|Xaxis|Yaxis|Zaxis|ZipCodeType|Zipcode|
# +-------------------+-------+-------------+-------------------+-----+--------------------+--------------------+--------------+-------+-------------+------------+-----+---------------+----------+-----------+-----+-----+-----+-----------+-------+
# |        PARC PARQUE|     US|        false|               null|17.96|NA-US-PR-PARC PARQUE|     Parc Parque, PR|NOT ACCEPTABLE| -66.22|         null|           1|   PR|           null|      null|         NA| 0.38|-0.87|  0.3|   STANDARD|    704|
# |PASEO COSTA DEL SUR|     US|        false|               null|17.96|NA-US-PR-PASEO CO...|Paseo Costa Del S...|NOT ACCEPTABLE| -66.22|         null|           2|   PR|           null|      null|         NA| 0.38|-0.87|  0.3|   STANDARD|    704|
# |       BDA SAN LUIS|     US|        false|               null|18.14|NA-US-PR-BDA SAN ...|    Bda San Luis, PR|NOT ACCEPTABLE| -66.26|         null|          10|   PR|           null|      null|         NA| 0.38|-0.86| 0.31|   STANDARD|    709|
# |  CINGULAR WIRELESS|     US|        false|               null|32.72|NA-US-TX-CINGULAR...|Cingular Wireless...|NOT ACCEPTABLE| -97.31|         null|       61391|   TX|           null|      null|         NA| -0.1|-0.83| 0.54|     UNIQUE|  76166|
# |         FORT WORTH|     US|        false|               4053|32.75| NA-US-TX-FORT WORTH|      Fort Worth, TX|       PRIMARY| -97.33|         null|       61392|   TX|           2126| 122396986|         NA| -0.1|-0.83| 0.54|   STANDARD|  76177|

# reading with an specific schema
from pyspark.sql.types import *
schema = StructType([
      StructField("RecordNumber",IntegerType()),
      StructField("Zipcode",IntegerType()),
      StructField("ZipCodeType",StringType()),
      StructField("City",StringType()),
      StructField("State",StringType()),
      StructField("LocationType",StringType()),
      StructField("Lat",DoubleType()),
      StructField("Long",DoubleType()),
      StructField("Xaxis",IntegerType()),
      StructField("Yaxis",DoubleType()),
      StructField("Zaxis",DoubleType()),
      StructField("WorldRegion",StringType()),
      StructField("Country",StringType()),
      StructField("LocationText",StringType()),
      StructField("Location",StringType()),
      StructField("Decommisioned",BooleanType()),
      StructField("TaxReturnsFiled",StringType()),
      StructField("EstimatedPopulation",IntegerType()),
      StructField("TotalWages",IntegerType()),
      StructField("Notes",StringType())
])

df_with_schema = spark.read.schema(schema).json("dataset1.json")
df_with_schema.printSchema()
# root
#  |-- RecordNumber: integer (nullable = true)
#  |-- Zipcode: integer (nullable = true)
#  |-- ZipCodeType: string (nullable = true)
#  |-- City: string (nullable = true)
#  |-- State: string (nullable = true)
#  |-- LocationType: string (nullable = true)
#  |-- Lat: double (nullable = true)
#  |-- Long: double (nullable = true)
#  |-- Xaxis: integer (nullable = true)
#  |-- Yaxis: double (nullable = true)
#  |-- Zaxis: double (nullable = true)
#  |-- WorldRegion: string (nullable = true)
#  |-- Country: string (nullable = true)
#  |-- LocationText: string (nullable = true)
#  |-- Location: string (nullable = true)
#  |-- Decommisioned: boolean (nullable = true)
#  |-- TaxReturnsFiled: string (nullable = true)
#  |-- EstimatedPopulation: integer (nullable = true)sss
#  |-- TotalWages: integer (nullable = true)
#  |-- Notes: string (nullable = true)
df_with_schema.show()


# load json using Spark SQL:
# from pyspark.sql import SQLContext
spark.sql("CREATE TEMPORARY VIEW zipcodes USING json OPTIONS (path 'dataset1.json')")
spark.sql("select * from zipcodes").show()
# +-------------------+-------+-------------+-------------------+-----+--------------------+--------------------+--------------+-------+-------------+------------+-----+---------------+----------+-----------+-----+-----+-----+-----------+-------+
# |               City|Country|Decommisioned|EstimatedPopulation|  Lat|            Location|        LocationText|  LocationType|   Long|        Notes|RecordNumber|State|TaxReturnsFiled|TotalWages|WorldRegion|Xaxis|Yaxis|Zaxis|ZipCodeType|Zipcode|
# +-------------------+-------+-------------+-------------------+-----+--------------------+--------------------+--------------+-------+-------------+------------+-----+---------------+----------+-----------+-----+-----+-----+-----------+-------+
# |        PARC PARQUE|     US|        false|               null|17.96|NA-US-PR-PARC PARQUE|     Parc Parque, PR|NOT ACCEPTABLE| -66.22|         null|           1|   PR|           null|      null|         NA| 0.38|-0.87|  0.3|   STANDARD|    704|
# |PASEO COSTA DEL SUR|     US|        false|               null|17.96|NA-US-PR-PASEO CO...|Paseo Costa Del S...|NOT ACCEPTABLE| -66.22|         null|           2|   PR|           null|      null|         NA| 0.38|-0.87|  0.3|   STANDARD|    704|