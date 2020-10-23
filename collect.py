from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("collect").getOrCreate()

dept = [("Finance",10), \
        ("Marketing",20), \
        ("Sales",30), \
        ("IT",40) \
        ]

dept_cols = ["dept","deptId"]

df = spark.createDataFrame(data=dept,schema=dept_cols)
df.show()



datacollect = df.collect()
print(datacollect)

for row in datacollect:
    print(row["dept"] + "," + str(row["deptId"]))
