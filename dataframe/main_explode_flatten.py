import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import flatten

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayArrayData = [
  ("James",[["madrid","barcelona","jaen"],["kotlin","Java"]]),
  ("Michael",[["rishikesh","sidney","buenos_aires"],["ruby","python"]]),
  ("Robert",[["paris","rome"],["php","javascript"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.printSchema()
# root
#  |-- name: string (nullable = true)
#  |-- subjects: array (nullable = true)
#  |    |-- element: array (containsNull = true)
#  |    |    |-- element: string (containsNull = true)
df.show(truncate=False)
# +-------+---------------------------------------------------+
# |name   |subjects                                           |
# +-------+---------------------------------------------------+
# |James  |[[madrid, barcelona, jaen], [kotlin, Java]]        |
# |Michael|[[rishikesh, sidney, buenos_aires], [ruby, python]]|
# |Robert |[[paris, rome], [php, javascript]]                 |
# +-------+---------------------------------------------------+


# explode stuff
df\
  .select(df.name,explode(df.subjects))\
  .show(truncate=False)
# +-------+---------------------------------+
# |name   |col                              |
# +-------+---------------------------------+
# |James  |[madrid, barcelona, jaen]        |
# |James  |[kotlin, Java]                   |
# |Michael|[rishikesh, sidney, buenos_aires]|
# |Michael|[ruby, python]                   |
# |Robert |[paris, rome]                    |
# |Robert |[php, javascript]                |
# +-------+---------------------------------+

# flatten stuff
df\
  .select(df.name,flatten(df.subjects))\
  .show(truncate=False)
# +-------+-----------------------------------------------+
# |name   |flatten(subjects)                              |
# +-------+-----------------------------------------------+
# |James  |[madrid, barcelona, jaen, kotlin, Java]        |
# |Michael|[rishikesh, sidney, buenos_aires, ruby, python]|
# |Robert |[paris, rome, php, javascript]                 |
# +-------+-----------------------------------------------+