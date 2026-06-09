import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

print("🚀 Generating 500,000 rows of telecom telemetry data...")

# Configuration
num_rows = 500000
np.random.seed(42)

# Generate base data
start_time = datetime(2026, 6, 1)
timestamps = [start_time + timedelta(seconds=i * 0.1) for i in range(num_rows)]
device_ids = [f"DEV-{np.random.randint(1000, 1500)}" for _ in range(num_rows)]

# Generate realistic telecom metrics
signal_strength = np.random.normal(loc=-75, scale=15, size=num_rows)  # dBm
latency = np.random.exponential(scale=20, size=num_rows) + 10         # ms
packet_loss = np.random.choice([0.0, 0.1, 0.5, 1.0, 5.0, 12.0], size=num_rows, p=[0.85, 0.08, 0.04, 0.02, 0.007, 0.003])

# Status codes mapping to network conditions
status_codes = np.random.choice(["OK", "WARNING", "CRITICAL"], size=num_rows, p=[0.88, 0.10, 0.02])

# Create DataFrame
df = pd.DataFrame({
    'timestamp': timestamps,
    'device_id': device_ids,
    'signal_strength_dbm': np.round(signal_strength, 2),
    'latency_ms': np.round(latency, 2),
    'packet_loss_pct': packet_loss,
    'status_code': status_codes
})

# Save to raw directory
os.makedirs("data/raw", exist_ok=True)
output_path = "data/raw/network_telemetry.csv"
df.to_csv(output_path, index=False)

print(f"✅ Success! Raw dataset saved to: {output_path} ({os.path.getsize(output_path) / (1024*1024):.2f} MB)")