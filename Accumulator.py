from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Accumulator").getOrCreate()

accum = spark.sparkContext.accumulator(0)
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd.foreach(lambda x: accum.add(x))
# print(accum.value)

accumSum = spark.sparkContext.accumulator(0)

data = [("111", 50000), ("222", 60000), ("333", 40000)]
columns = ["EmpId", "Salary"]
df = spark.createDataFrame(data=data, schema=columns)

from pyspark.sql.functions import col, lit
from pyspark.sql.functions import when

df.withColumn("lit_value2", when(col("Salary") >=40000 & col("Salary") <= 50000,lit("100")).otherwise(lit("200"))).show(truncate=False)
