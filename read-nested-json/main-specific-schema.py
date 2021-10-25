#!/usr/local/bin/python3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import explode
from pyspark.sql.types import *

spark = SparkSession.builder.appName("JSONFileRead").master("local").getOrCreate()

rawJson="""
{
    "sensorName": "snx001",
    "sensorDate": "2020-01-01",
    "sensorReadings": [
        {
            "sensorChannel": 1,
            "sensorReading": 3.7465084060850105,
            "datetime": "2020-01-01 00:00:00"
        },
        {
            "sensorChannel": 2,
            "sensorReading": 10.543041369293153,
            "datetime": "2020-01-01 00:00:00"
        }
    ]
}
"""

sensor_schema = StructType(fields=[
    StructField('sensorName', StringType(), False),
    StructField('sensorDate', StringType(), True),
    StructField(
        'sensorReadings', ArrayType(
            StructType([
                StructField('sensorChannel', IntegerType(), False),
                StructField('sensorReading', DoubleType(), True),
                StructField('datetime', StringType(), True)
            ])
        )
    )
])

rddJson = spark.sparkContext.parallelize([rawJson])
df = spark.read.option("multiLine", True).json(rddJson, schema=sensor_schema)
df.show()
# +----------+----------+--------------------+
# |sensorName|sensorDate|      sensorReadings|
# +----------+----------+--------------------+
# |    snx001|2020-01-01|[{1, 3.7465084060...|
# +----------+----------+--------------------+