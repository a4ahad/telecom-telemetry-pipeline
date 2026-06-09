from pyspark.sql import SparkSession

print("🔍 Spin up Spark SQL Engine for Analytics...")

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("TelecomLakehouseAnalytics") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# 2. Register data layers as temporary SQL Views
processed_base_path = "data/processed"

print("🗂️ Registering Parquet data tables into the SQL catalog...")
spark.read.parquet(f"{processed_base_path}/detailed_telemetry").createOrReplaceTempView("v_detailed_telemetry")
spark.read.parquet(f"{processed_base_path}/device_profiles").createOrReplaceTempView("v_device_profiles")

print("\n" + "="*50 + "\n📊 RUNNING ENTERPRISE ANALYTICS QUERIES\n" + "="*50)

# --- QUERY 1: High-Priority Network Alerts ---
print("\n🚨 QUERY 1: Detecting Top 5 Devices under CRITICAL status with severe Packet Loss...")
critical_alert_sql = """
    SELECT 
        device_id, 
        timestamp, 
        signal_strength_dbm, 
        packet_loss_pct,
        is_high_risk_link
    FROM v_detailed_telemetry
    WHERE status_code = 'CRITICAL' 
      AND packet_loss_pct > 5.0
    ORDER BY packet_loss_pct DESC
    LIMIT 5
"""
spark.sql(critical_alert_sql).show()


# --- QUERY 2: Fleet Maintenance & Resource Allocation (FIXED COLUMN) ---
print("\n🛠️ QUERY 2: Identifying Hardware Assets requiring urgent Field Maintenance...")
maintenance_dispatch_sql = """
    SELECT 
        device_id,
        avg_latency_ms,
        avg_dbm,
        max_packet_loss_pct
    FROM v_device_profiles
    WHERE hardware_alert_flag = 1
       OR avg_latency_ms > 25.0
    ORDER BY avg_latency_ms DESC
    LIMIT 5
"""
spark.sql(maintenance_dispatch_sql).show()

print("✅ Analytical reporting complete. Shutting down engines.")
spark.stop()