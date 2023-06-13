from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructField, StructType,IntegerType,DateType
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
# root
#  |-- show_id: string (nullable = true)
#  |-- type: string (nullable = true)
#  |-- title: string (nullable = true)
#  |-- director: string (nullable = true)
#  |-- cast: string (nullable = true)
#  |-- country: string (nullable = true)
#  |-- date_added: string (nullable = true)
#  |-- release_year: string (nullable = true)
#  |-- rating: string (nullable = true)
#  |-- duration: string (nullable = true)
#  |-- listed_in: string (nullable = true)
#  |-- description: string (nullable = true)
# 8809

df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(r"data\archive\netflix_titles.csv")

# Get countrywise ranks based on year released
dfr = df.withColumn("release_year",f.col("release_year").cast(IntegerType()))
dfr.printSchema()
rank_window = Window.partitionBy("country").orderBy("release_year")
dfr2= dfr.withColumn("country_wise_rank",f.dense_rank().over(rank_window))
dfr2.select("country","release_year","country_wise_rank","title").show()