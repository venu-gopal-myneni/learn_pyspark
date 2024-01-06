import os
from pyspark.sql import *

from pyspark import SparkConf
from settings import warehouse_location
conf = SparkConf()
dev_catalog_loc = warehouse_location #"file://" + os.path.dirname(os.getcwd()) + "/spark_warehouse/iceberg"

conf.set(
	"spark.jars.packages",
	"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2")
conf.set("spark.sql.execution.pyarrow.enabled", "true")

conf.set("spark.sql.catalog.nyc_taxi_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.nyc_taxi_catalog.type", "hadoop")

conf.set("spark.sql.catalog.nyc_taxi_catalog.warehouse", dev_catalog_loc)
conf.set("spark.sql.defaultCatalog", "nyc_taxi_catalog")

spark = SparkSession.builder.config(conf=conf).getOrCreate()


def ingest():
	df: DataFrame = spark.read.format("parquet").load(
		"/home/venumyneni/personal/projects/nyc-taxi-data/data/fhv_tripdata_2015-*.parquet")

	df.show(20)
	print(df.rdd.getNumPartitions())
	print(df.count())
	print(df.rdd.getStorageLevel())
	print(df.groupBy("dispatching_base_num").count())
	print(spark._jvm.org.apache.spark.util.SizeEstimator.estimate(df._jdf))
	df.writeTo("nyc_taxi_catalog.nyc.fhv_trip_data_2015").create()


ingest()

a=input("asdasd")