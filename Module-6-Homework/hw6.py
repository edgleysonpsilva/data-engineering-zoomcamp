import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("module_6_homework")
    .getOrCreate()
)

# Silence verbose Spark logs 
spark.sparkContext.setLogLevel("WARN")



#  Question 1

print(f"\nQ1 | Spark version: {spark.version}")

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")


#  Question 2 

OUTPUT_DIR = "output_q2"

# Split
df.repartition(4).write.mode("overwrite").parquet(OUTPUT_DIR)

# Measure 
parquet_files = [
    os.path.join(OUTPUT_DIR, f)
    for f in os.listdir(OUTPUT_DIR)
    if f.endswith(".parquet")
]

total_bytes = sum(os.path.getsize(fp) for fp in parquet_files)
avg_size_mb = (total_bytes / len(parquet_files)) / (1024 ** 2)

print(f"\nQ2 | ~{avg_size_mb:.0f} MB")


#  Question 3

nov_15_trips = df.filter(
    F.to_date("tpep_pickup_datetime") == "2025-11-15"
).count()

print(f"\nQ3 | {nov_15_trips:,}")


#  Question 4

df = df.withColumn(
    "duration_hours",
    (
        F.unix_timestamp("tpep_dropoff_datetime")
        - F.unix_timestamp("tpep_pickup_datetime")
    ) / 3600,
)

longest_trip_hours = df.select(F.max("duration_hours")).collect()[0][0]

print(f"\nQ4 |{longest_trip_hours:.1f} hours")

#  Question 5 

print("\nQ5 |4040")

#  Question 6

df_zones = (
    spark.read
    .option("header", "true")
    .csv("taxi_zone_lookup.csv")
)

# Count pickups per location
df_pickup_counts = df.groupBy("PULocationID").count()

df_with_zones = df_pickup_counts.join(
    df_zones,
    df_pickup_counts["PULocationID"] == df_zones["LocationID"],
    how="inner",
)

print("\nQ6 | Least frequent pickup zone:")
(
    df_with_zones
    .orderBy("count")            
    .select("Zone", "count")
    .show(1, truncate=False)
)

print("=" * 50 + "\n")