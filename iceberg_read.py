import os
from pyspark.sql import *

from pyspark import SparkConf
conf = SparkConf()
dev_catalog_loc = "file://" + os.getcwd() + "/spark_warehouse/iceberg"

conf.set(
    "spark.jars.packages",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2")
conf.set("spark.sql.execution.pyarrow.enabled", "true")

conf.set("spark.sql.catalog.dev_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.dev_catalog.type","hadoop")


conf.set("spark.sql.catalog.dev_catalog.warehouse",dev_catalog_loc )
conf.set("spark.sql.defaultCatalog","dev_catalog")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

spark.table("dev_catalog.nyc.taxis").show()

spark.sql("SELECT * FROM dev_catalog.nyc.taxis.history;").show()
