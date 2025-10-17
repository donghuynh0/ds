import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
import random

# configs
SPARK_MASTER = "192.168.80.55"
NAMENODE = "192.168.80.227"
HADOOP_PATH = f"hdfs://{NAMENODE}:9000/donghuynh0"


# Create Spark session
spark = SparkSession.builder \
    .appName("Generate Matrix") \
    .master(f"spark://{SPARK_MASTER}:7077") \
    .getOrCreate()


path = f"{HADOOP_PATH}/matrices_2/matrix1.parquet"

df = spark.read.parquet(path)

cols = len(df.columns)/2**2
print(f"\n✅ Shape: ({cols}, {cols})")


spark.stop()

print("\n✓ Spark session stopped.")