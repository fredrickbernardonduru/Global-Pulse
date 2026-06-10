from pyspark.sql import SparkSession
from pyspark.sql.functions import col


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-news"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("GlobalPulseKafkaToConsole")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
        )
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    output_df = kafka_df.select(
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
    )

    query = (
        output_df.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("append")
        .start()
    )

    print("Reading from Kafka topic 'raw-news' and printing to console...")
    query.awaitTermination()


if __name__ == "__main__":
    main()