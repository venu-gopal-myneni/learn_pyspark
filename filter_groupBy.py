
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructField, StructType,IntegerType,DateType

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


# get a count of all rows
print(df.count())

# get number of Movies
print(df.filter(f.col("type")=="Movie").count())

# get movies where director and cast is unknown
df.filter((f.col("director").isNull()) & (f.col("cast").isNull())).show()

# get total number of movies release by year
df.filter(f.col("type")=="Movie").groupBy(f.col("release_year")).agg(f.count("show_id").alias("num_movies")).select("release_year","num_movies").show()

# get all Docuseries
df.filter(f.col("listed_in").contains("Docuseries")).show()

# replace TV Show with TV in type column
df.replace("TV Show","TV","type").show()

# convert date_added to datetime, release_year to int 
# # (pyspark version >=3.4.0)
# new_schema = StructType([StructField("date_added", DateType()), StructField("release_year", IntegerType())])
# df.to(new_schema).show()


#df.show()