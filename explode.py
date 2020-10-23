import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, flatten

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayArrayData = [
    ("James", [["Java", "Scala", "C++"], ["Spark", "Java"]]),
    ("Michael", [["Spark", "Java", "C++"], ["Spark", "Java"]]),
    ("Robert", [["CSharp", "VB"], ["Spark", "Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema=['name', 'subjects'])
# df.printSchema()

#df.show(truncate=False)

# explode array columns to array rows

df.select(df.name, explode(df.subjects)).show()

df.select(df.name, flatten(df.subjects)).show(truncate=False)