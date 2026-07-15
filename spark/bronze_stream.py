from pyspark.sql import SparkSession
from pyspark.sql.functions import col


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "crop-sensor-data"

BRONZE_PATH = "/opt/project/data/bronze"
CHECKPOINT_PATH = "/opt/project/data/checkpoints/bronze"


spark = (
    SparkSession.builder
    .appName("CropHealthBronzeStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Starting Crop Health Bronze Streaming Pipeline...")


# Read streaming events from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


# Keep Kafka metadata and raw JSON
bronze_df = kafka_df.select(
    col("key").cast("string").alias("message_key"),
    col("value").cast("string").alias("raw_json"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
)


# Write streaming records as Parquet
query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)


print("Bronze streaming pipeline is running.")
print(f"Kafka topic: {KAFKA_TOPIC}")
print(f"Bronze location: {BRONZE_PATH}")

query.awaitTermination()