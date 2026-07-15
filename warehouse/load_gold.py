from pyspark.sql import SparkSession


GOLD_PATH = "/opt/project/data/gold"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/crop_health"

POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

TABLE_NAME = "crop_health_analytics"


spark = (
    SparkSession.builder
    .appName("GoldToPostgreSQL")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Reading Gold data...")

gold_df = spark.read.parquet(GOLD_PATH)

print(f"Gold records found: {gold_df.count()}")


print("Loading data into PostgreSQL...")

(
    gold_df.write
    .mode("overwrite")
    .jdbc(
        url=POSTGRES_URL,
        table=TABLE_NAME,
        properties=POSTGRES_PROPERTIES,
    )
)


print("Gold data successfully loaded into PostgreSQL!")
print(f"Table created: {TABLE_NAME}")


spark.stop()