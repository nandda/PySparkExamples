import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col, array_contains

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

df = spark.read.csv("D:\\Development\\xxxplatform\\PysparkTut\\zipcodes.csv")
df.printSchema()

df2 = spark.read.options(delimiter=" ").csv("D:\\Development\\xxxplatform\\PysparkTut\\zipcodes.csv", header=True).show(truncate=False)
#df2.printSchema()


