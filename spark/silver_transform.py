from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)

BRONZE_PATH = "/opt/project/data/bronze"
SILVER_PATH = "/opt/project/data/silver"

spark = (
    SparkSession.builder
    .appName("CropHealthSilverTransformation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

sensor_schema = StructType([
    StructField("farm_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("region", StringType(), True),
    StructField("N", LongType(), True),
    StructField("P", LongType(), True),
    StructField("K", LongType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("ph", DoubleType(), True),
    StructField("rainfall", DoubleType(), True),
    StructField("ndvi", DoubleType(), True),
    StructField("soc", DoubleType(), True),
    StructField("crop", StringType(), True),
])

print("Reading Bronze data...")

bronze_df = spark.read.parquet(BRONZE_PATH)

parsed_df = (
    bronze_df
    .withColumn("sensor_data", from_json(col("raw_json"), sensor_schema))
    .select(
        "sensor_data.*",
        "kafka_timestamp",
        "partition",
        "offset",
    )
)

clean_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

# Remove records without essential IDs
clean_df = clean_df.dropna(
    subset=["farm_id", "sensor_id", "timestamp"]
)

# Data-quality validation
clean_df = clean_df.filter(
    (col("ph").between(0, 14))
    & (col("humidity").between(0, 100))
    & (col("ndvi").between(-1, 1))
    & (col("soc") >= 0)
    & (col("rainfall") >= 0)
)

# Remove duplicate events
clean_df = clean_df.dropDuplicates(
    ["farm_id", "sensor_id", "timestamp"]
)

clean_df.write.mode("overwrite").parquet(SILVER_PATH)

print("Silver transformation completed!")
print(f"Valid records: {clean_df.count()}")

clean_df.show(10, truncate=False)

spark.stop()