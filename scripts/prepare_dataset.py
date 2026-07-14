import pandas as pd
import numpy as np
from faker import Faker

fake = Faker()

# Read original dataset
df = pd.read_csv("data/raw/sensor_data.csv")

# Rename label column
df.rename(columns={"label": "crop"}, inplace=True)

# Add Farm ID
df["farm_id"] = [
    f"FARM_{str(i).zfill(4)}"
    for i in range(1, len(df) + 1)
]

# Add Sensor ID
df["sensor_id"] = [
    f"SENSOR_{np.random.randint(1000,9999)}"
    for _ in range(len(df))
]

# Add Region
regions = [
    "North",
    "South",
    "East",
    "West",
    "Central"
]

df["region"] = np.random.choice(regions, len(df))

# Simulated NDVI
df["ndvi"] = np.round(
    np.random.uniform(0.25, 0.95, len(df)),
    2
)

# Simulated Soil Organic Carbon
df["soc"] = np.round(
    np.random.uniform(1.0, 4.5, len(df)),
    2
)

# Timestamp
df["timestamp"] = pd.date_range(
    start="2026-01-01",
    periods=len(df),
    freq="min"
)

# Reorder columns
df = df[
    [
        "farm_id",
        "sensor_id",
        "timestamp",
        "region",
        "N",
        "P",
        "K",
        "temperature",
        "humidity",
        "ph",
        "rainfall",
        "ndvi",
        "soc",
        "crop",
    ]
]

# Save processed dataset
df.to_csv(
    "data/raw/enriched_sensor_data.csv",
    index=False
)

print(df.head())

print("\nDataset prepared successfully!")