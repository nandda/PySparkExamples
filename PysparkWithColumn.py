from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[5]").appName("WithColumn").getOrCreate()

data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]

columns = ["FirstName", "MiddleName", "LastName", "dob", "Gender", "Salary"]

DF1 = spark.createDataFrame(data=data, schema=columns)
DF1.printSchema()
# DF1.show()

df2 = DF1.withColumn("salary", F.col("salary").cast("Integer"))
# df2.printSchema()

df3 = DF1.withColumn("salary", F.col("salary") * 100)
# df3.show()

df4 = DF1.withColumn("CopiedColumn", F.col("Salary") * -1)
# df4.show()

DF1.withColumn("Country", F.lit("USA").alias("Lit_value"))
