from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("windowsFunction").getOrCreate()

simpleData = (("James", "Sales", 3000),
              ("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("Maria", "Finance", 3000),
              ("James", "Sales", 3000),
              ("Scott", "Finance", 3300),
              ("Jen", "Finance", 3900),
              ("Jeff", "Marketing", 3000),
              ("Kumar", "Marketing", 2000),
              ("Saif", "Sales", 4100)
              )

columns = ["employee_name", "department", "salary"]

df = spark.createDataFrame(data=simpleData, schema=columns)

# df.printSchema()
# df.show(truncate=False)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, percent_rank, ntile, cume_dist, lag, lead

windowSpec = Window.partitionBy("department").orderBy("salary")
# row_number
# df.withColumn("row_number", row_number().over(windowSpec)).show(truncate=False)

# rank
# df.withColumn("Rank", rank().over(windowSpec)).show(truncate=False)

# dense Rank
# df.withColumn("dense_rank",dense_rank().over(windowSpec)).show(truncate=False)

# percent_rank
# df.withColumn("percent_rank", percent_rank().over(windowSpec)).show(truncate=False)

# ntile
# df.withColumn("ntile",ntile(1).over(windowSpec)).show(truncate=False)

# cume_dist
# df.withColumn("cume_dist",cume_dist().over(windowSpec)).show(truncate=False)

# lag
# df.withColumn("lag",lag("salary",1).over(windowSpec)).show(truncate=False)

# lead
# df.withColumn("lead",  lead("salary", 1).over(windowSpec)).show(truncate=False)


windowsAgg = Window.partitionBy("department")
from pyspark.sql.functions import col, avg, max, min, row_number, sum

df.withColumn("row", row_number().over(windowSpec))\
    .withColumn("avg", avg(col("salary")).over(windowsAgg)) \
    .withColumn("sum", sum(col("salary")).over(windowsAgg)) \
    .withColumn("min", min(col("salary")).over(windowsAgg)) \
    .withColumn("max", max(col("salary")).over(windowsAgg)) \
    .where(col("row") == 1)\
    .show()
