
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").load(r"data\overseas-trade-indexes-December-2022-quarter-provisional.csv")


df.show()