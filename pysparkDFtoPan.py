from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("pysparktoPandas").getOrCreate()

data = [("James", "", "Smith", "36636", "M", 60000),
        ("Michael", "Rose", "", "40288", "M", 70000),
        ("Robert", "", "Williams", "42114", "", 400000),
        ("Maria", "Anne", "Jones", "39192", "F", 500000),
        ("Jen", "Mary", "Brown", "", "F", 0)]

columns = ["FirstName", "MiddleName", "LastName", "dob", "Gender", "salary"]

rdd1 = spark.sparkContext.parallelize(data)
# rdd1.toDF(columns).show()

DF1 = spark.createDataFrame(data=data, schema=columns)

pandasDF = DF1.toPandas()
print(pandasDF)

from pyspark.sql.types import StructType, StringType, StructField

dataStruct = [(("james", "", "smith"), "36636", "M", "3000"),
              (("Michael", "Rose", ""), "40288", "M", "4000"),
              (("Robert", "", "Willams"), "42114", "M", "4000"),
              (("Maria", "anna", "brown"), "39192", "F", "4000"),
              (("Jen", "Marry", "brown"), "", "F", "-1")]

structSchema = StructType([StructField('name',
                                       StructType([StructField('FirstName', StringType(), True),
                                                   StructField('MiddleName', StringType(), True),
                                                   StructField('LastName', StringType(), True)
                                                   ])),
                           StructField('dob', StringType(), True),
                           StructField('gender', StringType(), True),
                           StructField('salary', StringType(), True)
                           ])

DF2 = spark.createDataFrame(data=dataStruct, schema=structSchema)
# DF2.show()

pandasDF = DF2.toPandas()
print(pandasDF)
