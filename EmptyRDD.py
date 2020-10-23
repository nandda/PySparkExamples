# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType
#
# spark = SparkSession.builder.master("local[2]").appName("CreateEmptyRDD").getOrCreate()
#
# schema = StructType([StructField('FirstName', StringType(), True),
#                      StructField("MiddleName", StringType(), True),
#                      StructField("LastName", StringType(), True)
#                      ])
#
# df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
# df.printSchema()
#
# df1 = spark.sparkContext.parallelize([]).toDF(schema)
# df1.printSchema()


###########Row Class###############
from pyspark.sql import Row, SparkSession

spark = SparkSession.builder.master("local[2]").appName("RowClass").getOrCreate()

row = Row("James", 40)
# print(row[0] + "," + str(row[1]))

Person = Row("name", "age")
p1 = Person("james", 40)
p2 = Person("Alice", 30)

# print(p1.name + "," + p2.name)

data = [Row(name="James,,Swith", lang=["Java", "scala", "c++"], state="ca"),
        Row(name="Michael,rose,", lang=["spark", "Java", "c++"], state="Nj"),
        Row(name="Robert,,Williams", lang=["CSharp", "VB"], state="NV")]

rdd = spark.sparkContext.parallelize(data)
collData = rdd.collect()
# print(collData)

# for row in collData:
#     print(row.name + ',' + str(row.lang))

# RDD2

PersonRDD2 = Row("name", "lang", "state")

dataRDD2 = [PersonRDD2("James,,Smith", ["Java", "Scala", "C++"], "CA"),
            PersonRDD2("Michael,Rose,", ["Spark", "Java", "C++"], "NJ"),
            PersonRDD2("Robert,,Williams", ["CSharp", "VB"], "NV")]

rdd = spark.sparkContext.parallelize(dataRDD2)
collDataRDD1 = rdd.collect()

# for person in collDataRDD1:
#     print(person.name+","+str(person.lang))


columns = ["name", "languagesAtSchool", "currentState"]
df = spark.createDataFrame(dataRDD2).toDF(*columns)
df.printSchema()

