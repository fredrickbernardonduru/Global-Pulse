import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer

from etl.extract.newsapi_client import fetch_top_headlines

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW_NEWS", "raw-news")


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
    )


def prepare_article(article: dict) -> dict:
    return {
        "source": article.get("source", {}),
        "author": article.get("author"),
        "title": article.get("title"),
        "description": article.get("description"),
        "url": article.get("url"),
        "urlToImage": article.get("urlToImage"),
        "publishedAt": article.get("publishedAt"),
        "content": article.get("content"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def publish_news():
    producer = create_producer()

    news_data = fetch_top_headlines()
    articles = news_data.get("articles", [])

    if not articles:
        print("No articles found.")
        return

    print(f"Publishing {len(articles)} articles to Kafka topic: {KAFKA_TOPIC}")

    for article in articles:
        prepared_article = prepare_article(article)

        article_key = prepared_article.get("url") or prepared_article.get("title")

        producer.send(
            KAFKA_TOPIC,
            key=article_key,
            value=prepared_article,
        )

    producer.flush()
    producer.close()

    print("Publishing complete.")


if __name__ == "__main__":
    publish_news()