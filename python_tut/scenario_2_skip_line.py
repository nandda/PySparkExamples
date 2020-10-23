import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("skip_header").master("local[*]").getOrCreate()
sc = spark.sparkContext

data = sc.textFile("D:\\Development\\xxxplatform\\SparkExample\\src\\main\\scala\\Data_analysis\\pageView.csv", 2)

data_1 = data.map(lambda x: x.split(","))

rdd_drop = data_1.mapPartitionsWithIndex(lambda idx, itr: list(itr)[8:] if (idx == 0) else itr)


out_df = spark.createDataFrame(rdd_drop).show()
