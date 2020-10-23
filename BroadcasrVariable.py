from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("BroadCastVariable").getOrCreate()

states = {"NY": "New York", "CA": "California", "FL": "Florida"}
broadcaststates = spark.sparkContext.broadcast(states)

data = [("James", "Smith", "USA", "CA"),
        ("Michael", "Rose", "USA", "NY"),
        ("Robert", "Williams", "USA", "CA"),
        ("Maria", "Jones", "USA", "FL")]

rdd = spark.sparkContext.parallelize(data)
columns = ["FirstName", "LastName", "Country", "State"]
df = spark.createDataFrame(data=data, schema=columns)


def state_convert(code):
    return broadcaststates.value[code]


resultDF = df.rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).toDF()
resultDF.show()
#result = rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).collect()
#print(result)
