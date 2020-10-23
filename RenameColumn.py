from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[5]").appName("RenameColumn").getOrCreate()

dataDF = [(('James', '', 'Smith'), '1991-04-01', 'M', 3000),
          (('Michael', 'Rose', ''), '2000-05-19', 'M', 4000),
          (('Robert', '', 'Williams'), '1978-09-05', 'M', 4000),
          (('Maria', 'Anne', 'Jones'), '1967-12-01', 'F', 4000),
          (('Jen', 'Mary', 'Brown'), '1980-02-17', 'F', -1)
          ]

schema = StructType([StructField("name",
                                 StructType([StructField("FirstName", StringType(), True),
                                             StructField("MiddleName", StringType(), True),
                                             StructField("LastName", StringType(), True)
                                             ])),
                     StructField("Dob", StringType(), True),
                     StructField("Gender", StringType(), True),
                     StructField("Salary", StringType(), True)])

df = spark.createDataFrame(data=dataDF, schema=schema)
df.printSchema()

df.withColumnRenamed("Dob", "DateOfBirth").printSchema()

schema2 = StructType([StructField("Fname", StringType(), True),
                      StructField("Mname", StringType(), True),
                      StructField("Lname", StringType(), True)])

df.select(col("name").cast(schema2),
          col("dob"),
          col("gender"),
          col("salary")).printSchema()


df4 = df.select(col("name.Firs").alias(""))
