FROM apache/spark:4.0.0

USER root

WORKDIR /app

RUN mkdir -p /app/data/bronze/news \
    && mkdir -p /app/data/bronze/_checkpoints/news

USER spark