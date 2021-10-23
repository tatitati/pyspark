from pyspark.sql import SparkSession
import requests
import json
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import Row

#
headers = {
    'content-type': "application/json"
}

body = json.dumps({
})

# response function - udf
def executeRestApi(url, headers, body):
  res = None
  # Make API request, get response object back, create dataframe from above schema.
  try:
      res = requests.get(url, data=body, headers=headers)
  except Exception as e:
    return e

  if res != None and res.status_code == 200:
    return json.loads(res.text)

  return None

#
schema = StructType([
  StructField("Count", IntegerType(), True),
  StructField("Message", StringType(), True),
  StructField("SearchCriteria", StringType(), True),
  StructField("Results", ArrayType(
    StructType([
      StructField("Make_ID", IntegerType()),
      StructField("Make_Name", StringType())
    ])
  ))
])

#
udf_executeRestApi = udf(executeRestApi, schema)

spark = SparkSession.builder.appName("UDF REST Demo").getOrCreate()

# requests
RestApiRequest = Row("url", "headers", "body")
request_df = spark.createDataFrame([
            RestApiRequest("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json", headers, body)
          ])\
          .withColumn("execute", udf_executeRestApi(col("url"), col("headers"), col("body")))

request_df.select(explode(col("execute.Results")).alias("results"))\
    .select(col("results.Make_ID"), col("results.Make_Name"))\
    .show()

# |Make_ID|           Make_Name|
# +-------+--------------------+
# |    440|        ASTON MARTIN|
# |    441|               TESLA|
# |    442|              JAGUAR|
# |    443|            MASERATI|
# |    444|          LAND ROVER|

spark.stop()