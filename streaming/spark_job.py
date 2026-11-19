"""Validate and score Kafka transactions with Spark Structured Streaming."""
import argparse
import logging
import os
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep

import psycopg
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructType

TRANSACTION_SCHEMA = StructType()
for field in ("transaction_id", "account_id", "amount", "country", "hour", "event_at", "_corrupt_record"):
    TRANSACTION_SCHEMA = TRANSACTION_SCHEMA.add(field, StringType())

WINDOW_SIZE = timedelta(minutes=10)
WATERMARK_DELAY = timedelta(minutes=5)
VELOCITY_THRESHOLD = 5
VELOCITY_POINTS = 60
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

TRANSACTION_COLUMNS = """
    transaction_id, account_id, amount, country, transaction_hour, risk_flag,
    risk_reason, risk_score, recorded_at, event_at, velocity_window_start,
    late_for_velocity, kafka_topic, kafka_partition, kafka_offset
"""
INSERT_TRANSACTION = f"""
    insert into ingest.transactions ({TRANSACTION_COLUMNS})
    select {TRANSACTION_COLUMNS} from batch_transactions
    order by kafka_topic, kafka_partition, kafka_offset
    on conflict do nothing
    returning account_id, event_at, velocity_window_start, late_for_velocity
"""

INSERT_REJECTION = """
    insert into ingest.rejected_transactions (
        kafka_topic, kafka_partition, kafka_offset, recorded_at, payload, rejection_reason
    ) values (%s, %s, %s, %s, %s, %s)
    on conflict (kafka_topic, kafka_partition, kafka_offset) do nothing
"""


def validate_transactions(messages):
    parsed = (
        messages.select(
            F.col("value").alias("payload"),
            F.col("timestamp").alias("recorded_at"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.from_json(
                F.col("value").cast("string"),
                TRANSACTION_SCHEMA,
                {"columnNameOfCorruptRecord": "_corrupt_record", "allowSingleQuotes": "false"},
            ).alias("event"),
        )
        .withColumn("transaction_id", F.trim("event.transaction_id"))
        .withColumn("account_id", F.trim("event.account_id"))
        .withColumn("country", F.upper(F.trim("event.country")))
        .withColumn("amount_text", F.trim("event.amount"))
        .withColumn("hour_text", F.trim("event.hour"))
        .withColumn("amount", F.expr("try_cast(amount_text as decimal(18, 2))"))
        .withColumn("transaction_hour", F.expr("try_cast(hour_text as smallint)"))
        .withColumn("event_time_text", F.trim("event.event_at"))
        .withColumn("event_at", F.try_to_timestamp("event_time_text"))
    )

    transaction_id = F.col("transaction_id")
    account_id = F.col("account_id")
    country = F.col("country")
    amount = F.col("amount")
    hour = F.col("transaction_hour")

    rejection = (
        F.when(F.col("event").isNull() | F.col("event._corrupt_record").isNotNull(), "invalid_json")
        .when(transaction_id.isNull() | ~transaction_id.rlike(r"^[^\s\p{C}]+\z"), "invalid_transaction_id")
        .when(account_id.isNull() | ~account_id.rlike(r"^[^\s\p{C}]+\z"), "invalid_account_id")
        .when(country.isNull() | ~country.rlike(r"^[A-Z]{2}\z"), "invalid_country")
        .when(
            amount.isNull() | ~F.col("amount_text").rlike(r"^[0-9]+(\.[0-9]{1,2})?\z"),
            "invalid_amount",
        )
        .when(
            hour.isNull() | ~F.col("hour_text").rlike(r"^[0-9]{1,2}\z") | ~hour.between(0, 23),
            "invalid_hour",
        )
        .when(
            F.col("event_time_text").isNotNull()
            & (
                ~F.col("event_time_text").rlike(
                    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z\z"
                )
                | F.col("event_at").isNull()
                | (F.col("event_at") < F.lit(EPOCH))
                | (F.col("event_at") > F.expr("recorded_at + interval 5 minutes"))
            ),
            "invalid_event_at",
        )
    )
    high_amount = amount >= 10_000
    overnight = hour < 5
    amount_points = F.when(amount >= 50_000, 80).when(high_amount, 60).otherwise(0)
    hour_points = F.when(hour < 2, 70).when(overnight, 60).otherwise(0)
    return (
        parsed.withColumn("rejection_reason", rejection)
        .withColumn("event_at", F.coalesce("event_at", "recorded_at"))
        .withColumn("velocity_window", F.window("event_at", "10 minutes"))
        .withColumn("recorded_us", F.unix_micros("recorded_at"))
        .withColumn("event_us", F.unix_micros("event_at"))
        .withColumn("window_us", F.unix_micros("velocity_window.start"))
        .withColumn("risk_flag", high_amount | overnight)
        .withColumn("risk_score", F.least(F.lit(100), amount_points + hour_points))
        .withColumn(
            "risk_reason",
            F.when(high_amount & overnight, "high_amount,overnight")
            .when(high_amount, "high_amount")
            .when(overnight, "overnight")
            .otherwise("none"),
        )
        .drop("event", "amount_text", "hour_text", "event_time_text")
    )


def migrate_database():
    with psycopg.connect(connect_timeout=5) as connection, connection.cursor() as cursor:
        cursor.execute("select pg_advisory_xact_lock(hashtext('banking.streaming'))")
        cursor.execute("select to_regclass('ingest.stream_state')")
        if cursor.fetchone()[0] is None:
            migration = Path(__file__).resolve().parents[1] / "postgres" / "streaming.sql"
            cursor.execute(migration.read_text(encoding="utf-8"))


def update_velocity(cursor, spark, inserted, watermark):
    windows = [(account, start) for account, _, start, _ in inserted if start is not None]
    if windows:
        counts = (
            spark.createDataFrame(windows, "account_id string, window_start timestamp")
            .groupBy("account_id", "window_start")
            .count()
            .select("account_id", F.unix_micros("window_start").alias("window_us"), "count")
            .collect()
        )
        cursor.executemany(
            """
            insert into ingest.account_windows (account_id, window_start, transaction_count)
            values (%s, %s, %s)
            on conflict (account_id, window_start) do update
            set transaction_count = ingest.account_windows.transaction_count + excluded.transaction_count
            """,
            [
                (row.account_id, EPOCH + timedelta(microseconds=row.window_us), row["count"])
                for row in counts
            ],
        )
        cursor.execute(
            """
            update ingest.transactions as t
            set velocity_flag = true, risk_flag = true, risk_score = least(100, t.risk_score + %s),
                risk_reason = case when t.risk_reason = 'none' then 'high_velocity'
                                   else t.risk_reason || ',high_velocity' end
            from ingest.account_windows as w
            where t.account_id = w.account_id and t.velocity_window_start = w.window_start
              and not t.velocity_flag and w.transaction_count >= %s
            """,
            (VELOCITY_POINTS, VELOCITY_THRESHOLD),
        )
    if watermark is not None:
        cursor.execute(
            "delete from ingest.account_windows where window_start <= %s",
            (watermark - WINDOW_SIZE,),
        )


def commit_batch(spark, rows, batch_id, retries):
    with psycopg.connect(connect_timeout=5) as connection, connection.cursor() as cursor:
        cursor.execute("select max_event_at from ingest.stream_state where singleton for update")
        (previous_max,) = cursor.fetchone()
        previous_watermark = previous_max - WATERMARK_DELAY if previous_max is not None else None
        cursor.execute("select kafka_topic, kafka_partition, next_offset from ingest.stream_offsets")
        committed_offsets = {(topic, partition): offset for topic, partition, offset in cursor.fetchall()}

        accepted, rejected, recorded_times = [], [], []
        next_offsets = {}
        for row in rows:
            partition = (row.kafka_topic, row.kafka_partition)
            if row.kafka_offset < committed_offsets.get(partition, 0):
                continue
            position = (*partition, row.kafka_offset)
            next_offsets[partition] = max(next_offsets.get(partition, 0), row.kafka_offset + 1)
            recorded_at = EPOCH + timedelta(microseconds=row.recorded_us)
            recorded_times.append(recorded_at)
            if row.rejection_reason:
                payload = None if row.payload is None else bytes(row.payload)
                rejected.append((*position, recorded_at, payload, row.rejection_reason))
                continue
            event_at = EPOCH + timedelta(microseconds=row.event_us)
            window_start = EPOCH + timedelta(microseconds=row.window_us)
            late = previous_watermark is not None and window_start + WINDOW_SIZE <= previous_watermark
            accepted.append((
                row.transaction_id, row.account_id, row.amount, row.country, row.transaction_hour,
                row.risk_flag, row.risk_reason, row.risk_score, recorded_at, event_at,
                None if late else window_start, late, *position,
            ))

        if not next_offsets:
            return

        inserted = []
        if accepted:
            cursor.execute(f"""
                create temp table batch_transactions on commit drop as
                select {TRANSACTION_COLUMNS} from ingest.transactions with no data
            """)
            with cursor.copy(f"copy batch_transactions ({TRANSACTION_COLUMNS}) from stdin") as copy:
                for record in accepted:
                    copy.write_row(record)
            cursor.execute(INSERT_TRANSACTION)
            inserted = cursor.fetchall()

        max_event_at = max((event_at for _, event_at, _, _ in inserted), default=previous_max)
        if previous_max is not None:
            max_event_at = max(previous_max, max_event_at)
        watermark = max_event_at - WATERMARK_DELAY if max_event_at is not None else None
        update_velocity(cursor, spark, inserted, watermark)
        cursor.execute("update ingest.stream_state set max_event_at = %s where singleton", (max_event_at,))

        logged = 0
        if rejected:
            cursor.executemany(INSERT_REJECTION, rejected)
            logged = cursor.rowcount

        cursor.executemany(
            """
            insert into ingest.stream_offsets (kafka_topic, kafka_partition, next_offset)
            values (%s, %s, %s)
            on conflict (kafka_topic, kafka_partition) do update set next_offset = excluded.next_offset
            """,
            [(*partition, offset) for partition, offset in next_offsets.items()],
        )
        processed_at = datetime.now(timezone.utc)
        delays = sorted(max(0, (processed_at - ts).total_seconds() * 1000) for ts in recorded_times)
        p95_index = (95 * len(delays) + 99) // 100 - 1
        cursor.execute(
            """
            insert into ingest.stream_batches (
                batch_id, input_records, accepted_records, rejected_records, late_records,
                db_retries, p95_processing_delay_ms, watermark
            ) values (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (batch_id, len(recorded_times), len(inserted), logged,
             sum(late for _, _, _, late in inserted), retries, delays[p95_index], watermark),
        )
    logging.info("Batch %s committed %s accepted and %s rejected records", batch_id, len(inserted), logged)


def write_batch(batch, batch_id):
    rows = batch.select(
        "transaction_id", "account_id", "amount", "country", "transaction_hour", "risk_flag",
        "risk_reason", "risk_score", "recorded_us", "event_us", "window_us", "kafka_topic",
        "kafka_partition", "kafka_offset", "rejection_reason", "payload",
    ).collect()
    if not rows:
        return
    for retries in range(4):
        try:
            commit_batch(batch.sparkSession, rows, batch_id, retries)
            return
        except psycopg.OperationalError as error:
            logging.warning("Database write attempt %s for batch %s failed: %s", retries + 1, batch_id, error)
            if retries == 3:
                raise
            sleep(2 ** retries)


def log_error(stage, error):
    logging.error("%s failed: %s", stage, error)
    try:
        with psycopg.connect(connect_timeout=3) as connection:
            connection.execute(
                "insert into ingest.stream_errors (stage, error_type) values (%s, %s)",
                (stage, type(error).__name__),
            )
    except psycopg.Error as db_error:
        logging.warning("Could not store the pipeline error in PostgreSQL: %s", db_error)


def sample_kafka_lag(brokers, topic):
    try:
        with closing(KafkaConsumer(
            bootstrap_servers=brokers, group_id=None, api_version_auto_timeout_ms=3000,
            request_timeout_ms=5000,
        )) as consumer:
            partition_numbers = consumer.partitions_for_topic(topic)
            if not partition_numbers:
                raise KafkaError(f"No partitions visible for {topic}")
            partitions = [TopicPartition(topic, number) for number in partition_numbers]
            beginnings = consumer.beginning_offsets(partitions)
            ends = consumer.end_offsets(partitions)
        with psycopg.connect(connect_timeout=3) as connection, connection.cursor() as cursor:
            cursor.execute(
                "select kafka_partition, next_offset from ingest.stream_offsets where kafka_topic = %s", (topic,)
            )
            offsets = dict(cursor.fetchall())
            values = []
            for partition in partitions:
                offset = max(beginnings[partition], offsets.get(partition.partition, 0))
                end = ends[partition]
                values.append((topic, partition.partition, offset, end, max(0, end - offset)))
            cursor.executemany(
                """
                insert into ingest.kafka_lag (kafka_topic, kafka_partition, next_offset, end_offset, lag)
                values (%s, %s, %s, %s, %s)
                """,
                values,
            )
    except (KafkaError, psycopg.Error) as error:
        log_error("kafka_lag", error)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--continuous", action="store_true", help="Wait for new transactions")
    args = parser.parse_args()
    continuous = args.continuous or os.getenv("SPARK_CONTINUOUS") == "1"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    brokers, topic = os.environ["KAFKA_BOOTSTRAP_SERVERS"], os.environ["KAFKA_TOPIC"]
    migrate_database()
    spark = SparkSession.builder.appName("banking-transaction-risk").config(
        "spark.sql.session.timeZone", "UTC"
    ).config("spark.sql.shuffle.partitions", "2").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        messages = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "10000")
            .load()
        )
        query = (
            validate_transactions(messages)
            .writeStream.foreachBatch(write_batch)
            .option("checkpointLocation", os.environ["SPARK_CHECKPOINT"])
            .trigger(**({"processingTime": "5 seconds"} if continuous else {"availableNow": True}))
            .start()
        )
        if continuous:
            while not query.awaitTermination(10):
                sample_kafka_lag(brokers, topic)
        else:
            query.awaitTermination()
        sample_kafka_lag(brokers, topic)
    except Exception as error:
        log_error("stream", error)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
