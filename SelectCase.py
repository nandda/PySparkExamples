from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("select").getOrCreate()

data = [("James", "Smith", "USA", "CA"),
        ("Michael", "Rose", "USA", "NY"),
        ("Robert", "Williams", "USA", "CA"),
        ("Maria", "Jones", "USA", "FL")]

columns = ["FirstName", "LastName", "country", "state"]

df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
#df.show()
df.select("Firstname").show()
df.select("Firstname","LastName").show()

#using dataFrame object Name:
df.select(df.Firstname,df.LastName).show()
