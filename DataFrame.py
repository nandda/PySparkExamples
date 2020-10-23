from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("DataFrame").getOrCreate()

data = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]

rdd1 = spark.sparkContext.parallelize(data)
# DF1 = rdd1.toDF()
# DF1.printSchema()
# DF1.show()


columns = ["DeptName", "DeptID"]

# DF1 = rdd1.toDF(columns)
# DF1.printSchema()
# DF1.show()

# deptRDD = spark.createDataFrame(data=rdd1, schema=columns)
# deptRDD.show()

from pyspark.sql.types import StringType, StructType, StructField

deptSchema = StructType([
    StructField('deptName', StringType(), True),
    StructField('deptId', StringType(), True)])

deptRDD1 = spark.createDataFrame(data=rdd1, schema=deptSchema)
deptRDD1.show()
