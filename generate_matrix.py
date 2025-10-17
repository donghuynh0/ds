import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
import random

# configs
SPARK_MASTER = "192.168.80.55"
NAMENODE = "192.168.80.57"
HADOOP_PATH = f"hdfs://{NAMENODE}:9000/matrices"

# Worker configuration
NUM_WORKERS = 5
MEMORY_PER_WORKER = "6g"
CORES_PER_WORKER = 12


spark = SparkSession.builder \
    .appName("Generate Matrix") \
    .master(f"spark://{SPARK_MASTER}:7077") \
    .config("spark.executor.memory", MEMORY_PER_WORKER) \
    .getOrCreate()

schema = StructType([
    StructField("row", IntegerType(), False),
    StructField("col", IntegerType(), False),
    StructField("value", DoubleType(), False)
])

for n in range(12, 21, 1):
    # Calculate optimal partitions based on matrix size
    matrix_size = 2 ** n
    num_elements = matrix_size * matrix_size
    # Aim for ~100-200MB per partition
    num_partitions = max(NUM_WORKERS * CORES_PER_WORKER * 4, num_elements // 10000000)

    print(f"Matrix size: {matrix_size}x{matrix_size}")
    print(f"Total elements: {num_elements:,}")
    print(f"Using {num_partitions} partitions")

    # Generate matrix 1 using parallelization
    def generate_matrix_partition(partition_id, total_partitions, size):
        """Generate a partition of matrix data"""
        elements_per_partition = (size * size) // total_partitions
        start_idx = partition_id * elements_per_partition
        end_idx = start_idx + elements_per_partition if partition_id < total_partitions - 1 else size * size

        result = []
        for idx in range(start_idx, end_idx):
            i = idx // size
            j = idx % size
            value = random.uniform(0, 1000)
            result.append((i, j, value))
        return result


    # Create RDD with partitions and convert to DataFrame
    partition_ids = spark.sparkContext.parallelize(range(num_partitions), num_partitions)

    matrix1_rdd = partition_ids.flatMap(
        lambda pid: generate_matrix_partition(pid, num_partitions, matrix_size)
    )
    df_matrix1 = spark.createDataFrame(matrix1_rdd, schema)

    matrix2_rdd = partition_ids.flatMap(
        lambda pid: generate_matrix_partition(pid, num_partitions, matrix_size)
    )
    df_matrix2 = spark.createDataFrame(matrix2_rdd, schema)

    # Repartition before writing for better distribution
    df_matrix1 = df_matrix1.repartition(num_partitions)
    df_matrix2 = df_matrix2.repartition(num_partitions)

    # path_matrices_n
    output_path = f"{HADOOP_PATH}/matrices_{n}"

    df_matrix1.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"{output_path}/matrix1.parquet")

    df_matrix2.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"{output_path}/matrix2.parquet")

spark.stop()
print("\n✓ Spark session stopped.")