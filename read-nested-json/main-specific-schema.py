#!/usr/local/bin/python3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import explode
from pyspark.sql.types import *

spark = SparkSession.builder.appName("JSONFileRead").master("local").getOrCreate()

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

data_df = spark.read.option("multiLine", True).json("dataset-specific-schema.json", schema=sensor_schema)
data_df.show()
# +----------+----------+--------------------+
# |sensorName|sensorDate|      sensorReadings|
# +----------+----------+--------------------+
# |    snx001|2020-01-01|[{1, 3.7465084060...|
# +----------+----------+--------------------+