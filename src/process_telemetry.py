import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, max, round

print("⚡ Initializing PySpark local engine...")

# 1. Initialize Spark Session optimized for local execution
spark = SparkSession.builder \
    .appName("TelecomTelemetryPipeline") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# 2. Read the raw CSV data
raw_data_path = "data/raw/network_telemetry.csv"
print(f"📖 Reading raw logs from {raw_data_path}...")

df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

# 3. Data Transformation & Feature Engineering
print("🛠️ Transforming and profiling network metrics...")

# Identify high-risk network links based on signal and latency thresholds
enriched_df = df.withColumn(
    "is_high_risk_link",
    when((col("signal_strength_dbm") < -85) & (col("packet_loss_pct") > 1.0), 1).otherwise(0)
)

# 4. Compute Analytical Aggregations (Device Performance Profile)
print("📊 Calculating device performance aggregates...")
device_profile_df = enriched_df.groupBy("device_id").agg(
    round(avg("latency_ms"), 2).alias("avg_latency_ms"),
    round(avg("signal_strength_dbm"), 2).alias("avg_dbm"),
    max("packet_loss_pct").alias("max_packet_loss_pct"),
    max("is_high_risk_link").alias("hardware_alert_flag")
)

# 5. Write Optimized Outputs to Parquet (Industry Standard Columnar Format)
processed_base_path = "data/processed"
os.makedirs(processed_base_path, exist_ok=True)

# Write the detailed telemetry partitioned by status code
print("💾 Saving detailed logs to partitioned Parquet storage...")
enriched_df.write \
    .mode("overwrite") \
    .partitionBy("status_code") \
    .parquet(f"{processed_base_path}/detailed_telemetry")

# Save the device profiles
print("💾 Saving summarized device profiles...")
device_profile_df.write \
    .mode("overwrite") \
    .parquet(f"{processed_base_path}/device_profiles")

print("✅ Pipeline executed successfully! Stopping Spark Session.")
spark.stop()