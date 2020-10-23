from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.master("local[2]").appName("Whenotherwise").getOrCreate()

data = [("James", "", "Smith", "36636", "M", 60000),
        ("Michael", "Rose", "", "40288", "M", 70000),
        ("Robert", "", "Williams", "42114", "", 400000),
        ("Maria", "Anne", "Jones", "39192", "F", 500000),
        ("Jen", "Mary", "Brown", "", "F", 0)]

columns = ["first_name", "middle_name", "last_name", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
# df.printSchema()
# df.show(truncate=False)

df2 = df.withColumn("new_gender", when(col("gender") == "M", "Male")
                    .when(col("gender") == "F", "Femaile")
                    .otherwise("Unknown"))
# df2.show()

# df22 = df.select(col('*'), when(col("gender") == "M", "Male")
#                  .when(col("gender") == "F", "Female")
#                  .otherwise("unknown")).alias("new").show()

data2 = [(66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4")]

df5 = spark.createDataFrame(data=data2, schema=["id", "code", "amt"])
df5.show()

df5.withColumn("new_column", when(col("code") == "a" | col("code") == "d", "A")
               .when(col("code") == "b" & col("amt") == "4", "B")
               .otherwise("A1")).show()
