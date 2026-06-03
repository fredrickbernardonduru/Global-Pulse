from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_OUTPUT_PATH = str(PROJECT_ROOT / "data" / "bronze" / "news")
CHECKPOINT_PATH = str(PROJECT_ROOT / "data" / "bronze" / "_checkpoints" / "news")


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-news"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("GlobalPulseKafkaToBronze")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .getOrCreate()
    )


def read_from_kafka(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )


def transform_to_bronze(kafka_df):
    return (
        kafka_df
        .select(
            col("key").cast("string").alias("message_key"),
            col("value").cast("string").alias("raw_json"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("bronze_ingested_at", current_timestamp())
    )


def write_to_bronze(bronze_df):
    return (
        bronze_df.writeStream
        .format("parquet")
        .option("path", BRONZE_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_from_kafka(spark)
    bronze_df = transform_to_bronze(kafka_df)

    query = write_to_bronze(bronze_df)

    print("Streaming from Kafka topic 'raw-news' to Bronze layer...")
    print(f"Bronze output path: {BRONZE_OUTPUT_PATH}")
    print(f"Checkpoint path: {CHECKPOINT_PATH}")

    query.awaitTermination()


if __name__ == "__main__":
    main()