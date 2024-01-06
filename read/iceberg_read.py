import os
from pyspark.sql import *

from pyspark import SparkConf
from settings import warehouse_location
conf = SparkConf()
catalog_loc = warehouse_location
print(catalog_loc)

conf.set(
    "spark.jars.packages",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2")
conf.set("spark.sql.execution.pyarrow.enabled", "true")

def read(catalog, schema, table):
    """
        Catalog name can be set to anything. It is abstract.
    """
    conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{catalog}.type","hadoop")


    conf.set(f"spark.sql.catalog.{catalog}.warehouse",catalog_loc )
    conf.set("spark.sql.defaultCatalog",catalog)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    spark.table(f"{catalog}.{schema}.{table}").show()

    spark.sql(f"SELECT * FROM {catalog}.{schema}.{table}.history;").show()

if __name__ == "__main__":
    catalog = "dev_catalog1"
    schema = "nyc"
    table = "taxis"
    read(catalog,schema,table)