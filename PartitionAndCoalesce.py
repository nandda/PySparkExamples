from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[5]").appName("PartitionAndCoalesce").getOrCreate()

df = spark.range(0, 20)
print(df.rdd.getNumPartitions())

rdd = spark.sparkContext.parallelize((0, 20))
print("local[5]" + str(rdd.getNumPartitions()))

rdd2 = spark.sparkContext.parallelize((0, 20), 6)
print("Parition Size: " + str(rdd2.getNumPartitions()))

rdd3 = rdd.repartition(2)
print("Repartition size" + str(rdd3.getNumPartitions()))

df2 = df.groupBy("id").count()
print(df2.rdd.getNumPartitions())

