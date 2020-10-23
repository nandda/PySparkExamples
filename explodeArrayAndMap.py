from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode, posexplode_outer

spark = SparkSession.builder.appName('pyspark-by-examples2').getOrCreate()

arrayData = [
    ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
    ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
    ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
    ('Washington', None, None),
    ('Jefferson', ['1', '2'], {})]

df = spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])
# df.printSchema()
# df.show(truncate=False)

# df2 = df.select(df.name, explode(df.knownLanguages))
# df2.show()

df3 = df.select(df.name, explode(df.properties))
# df3.show()

# df.select(df.name,explode_outer(df.knownLanguages)).show()
# df.select(df.name,explode_outer(df.properties)).show(truncate=False)


df.select(df.name, posexplode(df.knownLanguages)).show()
df.select(df.name, posexplode(df.properties)).show()

df.select(df.name, posexplode_outer(df.knownLanguages)).show()
df.select(df.name, posexplode_outer(df.properties)).show()
