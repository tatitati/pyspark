import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
# root
#  |-- Product: string (nullable = true)
#  |-- Amount: long (nullable = true)
#  |-- Country: string (nullable = true)

df.show(truncate=False)
# +-------+------+-------+
# |Product|Amount|Country|
# +-------+------+-------+
# |Banana |1000  |USA    |
# |Carrots|1500  |USA    |
# |Beans  |1600  |USA    |
# |Orange |2000  |USA    |
# |Orange |2000  |USA    |
# |Banana |400   |China  |
# |Carrots|1200  |China  |
# |Beans  |1500  |China  |
# |Orange |4000  |China  |
# |Banana |2000  |Canada |
# |Carrots|2000  |Canada |
# |Beans  |2000  |Mexico |
# +-------+------+-------+

pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.printSchema()
# root
#  |-- Product: string (nullable = true)
#  |-- Canada: long (nullable = true)
#  |-- China: long (nullable = true)
#  |-- Mexico: long (nullable = true)
#  |-- USA: long (nullable = true)

pivotDF.show(truncate=False)
# +-------+------+-----+------+----+
# |Product|Canada|China|Mexico|USA |
# +-------+------+-----+------+----+
# |Orange |null  |4000 |null  |4000|
# |Beans  |null  |1500 |2000  |1600|
# |Banana |2000  |400  |null  |1000|
# |Carrots|2000  |1200 |null  |1500|
# +-------+------+-----+------+----+
