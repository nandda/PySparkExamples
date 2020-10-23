from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, when
from pyspark.sql.types import StringType, StructField, StructType, IntegerType

spark = SparkSession.builder.master("local[3]").appName("StructType").getOrCreate()

data = [("James", "", "Smith", "36636", "M", 3000),
        ("Michael", "Rose", "", "40288", "M", 4000),
        ("Robert", "", "Williams", "42114", "M", 4000),
        ("Maria", "Anne", "Jones", "39192", "F", 4000),
        ("Jen", "Mary", "Brown", "", "F", -1)
        ]

schema = StructType([StructField("FirstName", StringType(), True),
                     StructField("MiddleName", StringType(), True),
                     StructField("LastName", StringType(), True),
                     StructField("Dob", StringType(), True),
                     StructField("Gender", StringType(), True),
                     StructField("Salary", StringType(), True)])

df = spark.createDataFrame(data=data, schema=schema)
# df.printSchema()
# df.show()

structureData = [(("James", "", "Smith"), "36636", "M", 3100),
                 (("Michael", "Rose", ""), "40288", "M", 4300),
                 (("Robert", "", "Williams"), "42114", "M", 1400),
                 (("Maria", "Anne", "Jones"), "39192", "F", 5500),
                 (("Jen", "Mary", "Brown"), "", "F", -1)]

StructureSchema = StructType([StructField("Name",
                                          StructType([StructField("Fname", StringType(), True),
                                                      StructField("Mname", StringType(), True),
                                                      StructField("Lname", StringType(), True)])),
                              StructField("Dob", StringType(), True),
                              StructField("Gender", StringType(), True),
                              StructField("Salary", StringType(), True)
                              ])

df2 = spark.createDataFrame(data=structureData, schema=StructureSchema)
# df2.printSchema()
# df2.show()

updatedDF=df2.withColumn("OtherInfo",
               struct(col("Name").alias("Identifier"),
                      col("Gender").alias("gender"),
                      col("Salary").alias("salary"),
                      when(col("Salary").cast(IntegerType()) < 2000, "Low")
                      .when(col("Salary").cast(IntegerType()) > 2000, "Medium")
                      .otherwise("High").alias("Salary_grade")
                      ))
updatedDF.printSchema()
updatedDF.show()

