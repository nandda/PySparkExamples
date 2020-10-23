from pyspark.sql import SparkSession
import pyspark.sql.functions

from pyspark.sql.functions import avg, collect_list, collect_set,countDistinct

spark = SparkSession.builder.master("local[2]").appName("Agg_fun").getOrCreate()

simpleData = [("James", "Sales", 3000),
              ("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("Maria", "Finance", 3000),
              ("James", "Sales", 3000),
              ("Scott", "Finance", 3300),
              ("Jen", "Finance", 3900),
              ("Jeff", "Marketing", 3000),
              ("Kumar", "Marketing", 2000),
              ("Saif", "Sales", 4100)
              ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=schema)
#df.printSchema()
#df.show(truncate=False)

#print("Approx count distinct:" + str(df.select(pyspark.sql.functions.approx_count_distinct("salary")).collect()[0][0]))

#print("Average Salary:" + str(df.select(avg("salary")).collect()[0][0]))

#df.select(collect_list("salary")).show(truncate=False)

#df.select(collect_set("salary")).show(truncate=False)

#df.select(avg("salary")).show()

df.select(countDistinct("department","salary")).show()

