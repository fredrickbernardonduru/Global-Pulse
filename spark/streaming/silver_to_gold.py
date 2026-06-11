from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    count,
    window,
    to_timestamp,
)


SILVER_INPUT_PATH = "/app/data/silver/news"
GOLD_OUTPUT_PATH = "/app/data/gold/news_trends"
CHECKPOINT_PATH = "/app/data/gold/_checkpoints/news_trends"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("GlobalPulseSilverToGold")
        .master("local[*]")
        .getOrCreate()
    )


def read_silver(spark):
    return (
        spark.readStream
        .format("parquet")
        .load(SILVER_INPUT_PATH)
    )


def transform_to_gold(silver_df):
    enriched_df = silver_df.withColumn(
        "published_timestamp",
        to_timestamp(col("published_at"))
    )

    gold_df = (
        enriched_df
        .filter(col("published_timestamp").isNotNull())
        .withWatermark("published_timestamp", "1 hour")
        .groupBy(
            window(col("published_timestamp"), "1 hour"),
            col("source_name")
        )
        .agg(
            count("*").alias("article_count")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("source_name"),
            col("article_count"),
            current_timestamp().alias("gold_processed_at")
        )
    )

    return gold_df


def write_gold(gold_df):
    return (
        gold_df.writeStream
        .format("parquet")
        .option("path", GOLD_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = read_silver(spark)
    gold_df = transform_to_gold(silver_df)

    query = write_gold(gold_df)

    print("Streaming Silver news to Gold trend layer...")
    print(f"Silver input path: {SILVER_INPUT_PATH}")
    print(f"Gold output path: {GOLD_OUTPUT_PATH}")
    print(f"Checkpoint path: {CHECKPOINT_PATH}")

    query.awaitTermination()


if __name__ == "__main__":
    main()