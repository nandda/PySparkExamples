from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains
from pyspark.sql.types import StructField, StructType, StringType, ArrayType

spark = SparkSession.builder.master("local[2]").appName("WhereFilter").getOrCreate()

arrayStructureData = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

StructureSchema = StructType([StructField("Name",
                                          StructType([StructField("Fname", StringType(), True),
                                                      StructField("Mname", StringType(), True),
                                                      StructField("Lname", StringType(), True)])),
                              StructField("Languages", ArrayType(StringType()), True),
                              StructField("State", StringType(), True),
                              StructField("Gender", StringType(), True)
                              ])

df = spark.createDataFrame(data=arrayStructureData, schema=StructureSchema)
df.printSchema()
df.show()

df.filter(df.Gender == "M").show()
df.filter(array_contains(df.Languages,"Java")).show()
