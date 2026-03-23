"""
Load serp_data.parquet into a Hopsworks offline feature group using Spark.
"""

import hopsworks
from hsfs.feature import Feature

project = hopsworks.login()
fs = project.get_feature_store()

fg = fs.get_feature_group("serp_data", version=1)

import importlib
spark_engine = importlib.import_module("hsfs.engine.spark")

# Use PySpark to read and insert
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("load_serp_data") \
    .getOrCreate()

print("Reading parquet file with Spark...")
df = spark.read.parquet("hdfs:///Projects/jim/Users/meb10000/duckdb_benchmarks/data/serp_data.parquet")
print(f"Loaded {df.count():,} rows")
df.printSchema()

print("Inserting into feature group...")
fg.insert(df)
print("Done!")

spark.stop()
