
from pyspark.sql import *
from pyspark import SparkConf
from settings import warehouse_location
import os
dev_catalog_loc = warehouse_location #"file://" + os.path.dirname(os.getcwd()) + "/spark_warehouse/iceberg"
print(f"DCL : {dev_catalog_loc}")
conf = SparkConf()
# we need iceberg libraries and the nessie sql extensions
#org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.4.2
conf.set(
    "spark.jars.packages",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2")
f"#,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.2_2.12:0.74.0"
# ensure python <-> java interactions are w/ pyarrow
conf.set("spark.sql.execution.pyarrow.enabled", "true")
# create catalog dev_catalog as an iceberg catalog
conf.set("spark.sql.catalog.dev_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.dev_catalog.type","hadoop")
# tell the dev_catalog that its a Nessie catalog
#conf.set("spark.sql.catalog.dev_catalog.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
# set the location for Nessie catalog to store data. Spark writes to this directory
conf.set("spark.sql.catalog.dev_catalog.warehouse",dev_catalog_loc )
conf.set("spark.sql.defaultCatalog","dev_catalog")
# set the location of the nessie server. In this demo its running locally. There are many ways to run it (see https://projectnessie.org/try/)
#conf.set("spark.sql.catalog.dev_catalog.uri", "http://localhost:19120/api/v1")
# default branch for Nessie catalog to work on
#conf.set("spark.sql.catalog.dev_catalog.ref", "main")
# use no authorization. Options are NONE AWS BASIC and aws implies running Nessie on a lambda
#conf.set("spark.sql.catalog.dev_catalog.auth_type", "NONE")
# enable the extensions for both Nessie and Iceberg
conf.set(
    "spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
# finally, start up the Spark server
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")
from pyspark.sql.types import DoubleType, FloatType, LongType, StructType,StructField, StringType
schema = StructType([
  StructField("vendor_id", LongType(), True),
  StructField("trip_id", LongType(), True),
  StructField("trip_distance", FloatType(), True),
  StructField("fare_amount", DoubleType(), True),
  StructField("store_and_fwd_flag", StringType(), True)
])

df = spark.createDataFrame([{"vendor_id":1,"trip_id":2,"trip_distance":78.2}], schema)
df.writeTo("dev_catalog.nyc.taxis").create()

spark.table("dev_catalog.nyc.taxis").show()