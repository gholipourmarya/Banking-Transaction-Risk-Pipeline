FROM python:3.11-slim-bookworm AS python-base
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

FROM python-base AS producer
RUN pip install --no-cache-dir kafka-python==2.2.15
COPY streaming/producer.py streaming/producer.py
COPY data/input.csv data/input.csv
ENTRYPOINT ["python", "streaming/producer.py"]

FROM python-base AS spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==3.5.7 'psycopg[binary]==3.2.10' kafka-python==2.2.15
COPY streaming/spark_job.py streaming/ivysettings.xml streaming/
COPY postgres/streaming.sql postgres/streaming.sql
ENTRYPOINT ["spark-submit", "--master", "local[2]", "--conf", "spark.ui.enabled=false", "--conf", "spark.jars.ivy=/state/ivy", "--conf", "spark.jars.ivySettings=/app/streaming/ivysettings.xml", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7", "streaming/spark_job.py"]

FROM python-base AS dbt
RUN pip install --no-cache-dir dbt-core==1.10.15 dbt-postgres==1.9.1
COPY dbt/ /app/dbt/
WORKDIR /app/dbt
ENTRYPOINT ["dbt"]
CMD ["build", "--profiles-dir", "."]
