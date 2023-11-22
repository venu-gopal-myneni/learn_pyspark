
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder.master("local[*]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
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

df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("./data/archive/netflix_titles"
                                                                                      ".csv")

df.explain()
df.cache()
df.explain()

# get schema
df.printSchema()
print(df.schema)

stop = input("Press enter to stop : ")
