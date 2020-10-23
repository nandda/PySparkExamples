from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum, avg

spark = SparkSession.builder.master("local[2]").appName("GroupBy").getOrCreate()

simpleData = [("James", "Sales", "NY", 90000, 34, 10000),
              ("Michael", "Sales", "NY", 86000, 56, 20000),
              ("Robert", "Sales", "CA", 81000, 30, 23000),
              ("Maria", "Finance", "CA", 90000, 24, 23000),
              ("Raman", "Finance", "CA", 99000, 40, 24000),
              ("Scott", "Finance", "NY", 83000, 36, 19000),
              ("Jen", "Finance", "NY", 79000, 53, 15000),
              ("Jeff", "Marketing", "CA", 80000, 25, 18000),
              ("Kumar", "Marketing", "NY", 91000, 50, 21000)
              ]

columns = ["Name", "Department", "State", "Salary", "Age", "Bonus"]

DF1 = spark.createDataFrame(data=simpleData, schema=columns)
DF1.show()

DF1.groupBy("Department").count().show()

DF1.groupBy("Department", "State").sum("Salary", "Bonus").show()

DF1.groupBy("Department").agg(sum("Salary").alias("Sum_Salary"),
                              avg("Salary").alias("Avg_Salary"),
                              sum("Bonus").alias("Sum_Bonus"),
                              avg("Bonus").alias("Avg_Bonus")).where(col("Sum_Salary") >= 50000).show()
