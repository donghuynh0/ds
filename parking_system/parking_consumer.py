# parking_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import json


BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "192.168.1.56:9092")

# Checkpoint directories (make sure these paths are writable)
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "/tmp/parking_checkpoints")
CHECKPOINT_PARKED = os.path.join(CHECKPOINT_BASE, "checkpoint_parked_vehicles")
CHECKPOINT_LOC = os.path.join(CHECKPOINT_BASE, "checkpoint_locations")
CHECKPOINT_SUM = os.path.join(CHECKPOINT_BASE, "checkpoint_summary")

PARKING_FEE_PER_10_MINUTES = 1000


spark = SparkSession.builder \
    .appName("ParkingLotStatefulProcessing") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("timestamp_unix", LongType(), True),   
    StructField("license_plate", StringType(), True),
    StructField("location", StringType(), True),
    StructField("status_code", StringType(), True),
    StructField("entry_timestamp", LongType(), True)    
])


kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", "parking-events") \
    .option("startingOffsets", "latest") \
    .load()


parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

events_df = parsed_df.withColumn(
    "event_time",
    to_timestamp( (col("timestamp_unix") / 1000).cast("long") )
).withWatermark("event_time", "10 minutes") \
 .filter(col("event_time").isNotNull())
 

#  per-vehicle state, compute parked duration & fee
parking_state_df = events_df.groupBy(
    col("license_plate"),
    col("location")
).agg(
    first("entry_timestamp").alias("entry_timestamp"),
    last("status_code").alias("current_status"),
    last("timestamp_unix").alias("last_update_timestamp"),
    last("timestamp").alias("last_update_time")
)

parking_with_fee_df = parking_state_df.withColumn(
    "parked_duration_seconds",
    (col("last_update_timestamp") - col("entry_timestamp")) / 1000  # milliseconds -> seconds
).withColumn(
    "parked_duration_minutes",
    (col("parked_duration_seconds") / 60).cast(IntegerType())
).withColumn(
    "parked_blocks",
    ceil(col("parked_duration_minutes") / 10).cast(IntegerType())
).withColumn(
    "parking_fee",
    col("parked_blocks") * lit(PARKING_FEE_PER_10_MINUTES)
).filter(
    col("current_status") == "PARKED"
)

# location status
location_status_df = events_df.groupBy("location").agg(
    last("status_code").alias("status"),
    last("license_plate").alias("license_plate"),
    last("timestamp").alias("last_update")
)

occupied_locations_df = location_status_df.filter(
    col("status") == "PARKED"
).select(
    col("location"),
    col("license_plate"),
    lit("Đang có xe").alias("availability_status")
)

available_locations_df = location_status_df.filter(
    (col("status") != "PARKED") | (col("status").isNull())
).select(
    col("location"),
    lit(None).cast(StringType()).alias("license_plate"),
    lit("Còn trống").alias("availability_status")
)

all_locations_df = occupied_locations_df.unionByName(available_locations_df)


# summary windowed stats (streaming-compatible)
summary_df = events_df.groupBy(window(col("event_time"), "1 minute")).agg(
    approx_count_distinct(when(col("status_code") == "PARKED", col("license_plate"))).alias("total_parked_vehicles"),
    approx_count_distinct(col("location")).alias("total_locations_used"),
    count("*").alias("total_events")
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_parked_vehicles"),
    col("total_locations_used"),
    col("total_events")
)

# print parked vehicles details per batch
def write_parked_vehicles(batch_df, batch_id):
    print(f"\n{'='*80}")
    print(f"BATCH {batch_id} - CHI TIẾT XE ĐANG ĐỖ")
    print(f"{'='*80}")
    batch_df.select(
        "license_plate",
        "location",
        "parked_duration_minutes",
        "parking_fee",
        "last_update_time"
    ).orderBy("location").show(100, truncate=False)
    print(f"Tổng số xe đang đỗ: {batch_df.count()}")
    print(f"{'='*80}\n")

query1 = parking_with_fee_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_parked_vehicles) \
    .option("checkpointLocation", CHECKPOINT_PARKED) \
    .start()

# print location status per batch
def write_location_status(batch_df, batch_id):
    print(f"\n{'='*80}")
    print(f"BATCH {batch_id} - TRẠNG THÁI VỊ TRÍ ĐỖ XE")
    print(f"{'='*80}")
    occupied = batch_df.filter(col("availability_status") == "Đang có xe").count()
    available = batch_df.filter(col("availability_status") == "Còn trống").count()
    print(f"\nTổng quan:")
    print(f"  - Vị trí đang có xe: {occupied}")
    print(f"  - Vị trí còn trống: {available}")
    print(f"\nChi tiết vị trí đang có xe:")
    batch_df.filter(col("availability_status") == "Đang có xe") \
        .select("location", "license_plate", "availability_status") \
        .orderBy("location") \
        .show(60, truncate=False)
    print(f"{'='*80}\n")

query2 = all_locations_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_location_status) \
    .option("checkpointLocation", CHECKPOINT_LOC) \
    .start()


# print summary per batch
def write_summary(batch_df, batch_id):
    print(f"\n{'='*80}")
    print(f"BATCH {batch_id} - THỐNG KÊ TỔNG QUAN")
    print(f"{'='*80}")
    batch_df.select(
        "window_start",
        "window_end",
        "total_parked_vehicles",
        "total_locations_used",
        "total_events"
    ).orderBy("window_start", ascending=False).show(10, truncate=False)
    print(f"{'='*80}\n")

query3 = summary_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_summary) \
    .option("checkpointLocation", CHECKPOINT_SUM) \
    .start()


try:
    print(f"Spark Streaming running. Kafka bootstrap servers = {BOOTSTRAP_SERVERS}")
    print("Press Ctrl+C to stop...")
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nStopping Spark Streaming...")
    query1.stop()
    query2.stop()
    query3.stop()
    spark.stop()
    print("Stopped.")
