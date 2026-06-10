from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lower, trim
from pyspark.sql.types import StructType, StructField, StringType, MapType


BRONZE_INPUT_PATH = "/app/data/bronze/news"
SILVER_OUTPUT_PATH = "/app/data/silver/news"
CHECKPOINT_PATH = "/app/data/silver/_checkpoints/news"


news_schema = StructType([
    StructField("source", MapType(StringType(), StringType()), True),
    StructField("author", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    StructField("urlToImage", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("content", StringType(), True),
    StructField("ingested_at", StringType(), True),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("GlobalPulseBronzeToSilver")
        .master("local[*]")
        .getOrCreate()
    )


def read_bronze(spark):
    return (
        spark.readStream
        .format("parquet")
        .load(BRONZE_INPUT_PATH)
    )


def transform_to_silver(bronze_df):
    parsed_df = bronze_df.withColumn(
        "parsed_json",
        from_json(col("raw_json"), news_schema)
    )

    silver_df = parsed_df.select(
        col("message_key"),
        col("parsed_json.source.name").alias("source_name"),
        col("parsed_json.author").alias("author"),
        trim(col("parsed_json.title")).alias("title"),
        trim(col("parsed_json.description")).alias("description"),
        col("parsed_json.url").alias("url"),
        col("parsed_json.urlToImage").alias("image_url"),
        col("parsed_json.publishedAt").alias("published_at"),
        trim(col("parsed_json.content")).alias("content"),
        col("parsed_json.ingested_at").alias("source_ingested_at"),
        col("kafka_timestamp"),
        col("bronze_ingested_at"),
        current_timestamp().alias("silver_processed_at"),
    )

    return (
        silver_df
        .filter(col("title").isNotNull())
        .filter(col("url").isNotNull())
        .withColumn("title_lower", lower(col("title")))
        .dropDuplicates(["url"])
    )


def write_silver(silver_df):
    return (
        silver_df.writeStream
        .format("parquet")
        .option("path", SILVER_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = read_bronze(spark)
    silver_df = transform_to_silver(bronze_df)

    query = write_silver(silver_df)

    print("Streaming Bronze news to Silver layer...")
    print(f"Bronze input path: {BRONZE_INPUT_PATH}")
    print(f"Silver output path: {SILVER_OUTPUT_PATH}")
    print(f"Checkpoint path: {CHECKPOINT_PATH}")

    query.awaitTermination()


if __name__ == "__main__":
    main()