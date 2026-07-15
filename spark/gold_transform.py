from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    round as spark_round,
)

SILVER_PATH = "/opt/project/data/silver"
GOLD_PATH = "/opt/project/data/gold"


spark = (
    SparkSession.builder
    .appName("CropHealthGoldTransformation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Reading Silver data...")

df = spark.read.parquet(SILVER_PATH)


# -------------------------------------------------
# 1. NDVI Health Category
# -------------------------------------------------

df = df.withColumn(
    "vegetation_health",
    when(col("ndvi") >= 0.7, "Healthy")
    .when(col("ndvi") >= 0.5, "Moderate")
    .otherwise("Low")
)


# -------------------------------------------------
# 2. Soil Organic Carbon Category
# -------------------------------------------------

df = df.withColumn(
    "soc_category",
    when(col("soc") >= 3.0, "High")
    .when(col("soc") >= 2.0, "Medium")
    .otherwise("Low")
)


# -------------------------------------------------
# 3. Average NPK indicator
# -------------------------------------------------

df = df.withColumn(
    "nutrient_index",
    spark_round(
        (
            col("N") +
            col("P") +
            col("K")
        ) / 3,
        2
    )
)


# -------------------------------------------------
# 4. pH Category
# -------------------------------------------------

df = df.withColumn(
    "ph_category",
    when(
        col("ph").between(6.0, 7.5),
        "Optimal"
    )
    .when(
        col("ph") < 6.0,
        "Acidic"
    )
    .otherwise("Alkaline")
)


# -------------------------------------------------
# 5. Demonstration Crop Health Score
#
# This is a portfolio/demo metric, not a validated
# agronomic health model.
# -------------------------------------------------

df = df.withColumn(
    "crop_health_score",
    spark_round(
        (
            col("ndvi") * 50
            + (col("soc") / 4.5) * 30
            + (col("humidity") / 100) * 20
        ),
        2
    )
)


# -------------------------------------------------
# 6. Health Risk Category
# -------------------------------------------------

df = df.withColumn(
    "health_risk",
    when(
        col("crop_health_score") >= 70,
        "Low Risk"
    )
    .when(
        col("crop_health_score") >= 50,
        "Medium Risk"
    )
    .otherwise("High Risk")
)


# -------------------------------------------------
# Select final analytics columns
# -------------------------------------------------

gold_df = df.select(

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

    "vegetation_health",
    "soc_category",
    "nutrient_index",
    "ph_category",
    "crop_health_score",
    "health_risk"
)


# -------------------------------------------------
# Write Gold data
# -------------------------------------------------

gold_df.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)


print("Gold transformation completed!")

print(
    f"Gold records: {gold_df.count()}"
)

gold_df.show(
    10,
    truncate=False
)


spark.stop()