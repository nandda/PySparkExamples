import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col, array_contains

spark = SparkSession.builder.appName('scenario.com').getOrCreate()

#to remove the unwanted characters from the input file
#using only sparksession


df = spark.read.text("D:\\Development\\xxxplatform\\PysparkTut\python_tut\\scenario.csv")

df.show()

header = df.first()[0]
schema = header.split('~|')
data_df = df.filter(df['value'] != header).rdd.map(lambda x: x[0].split('~|')).toDF(schema).show()








