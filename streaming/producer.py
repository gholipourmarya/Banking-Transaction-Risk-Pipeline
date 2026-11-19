"""Publish the repository's synthetic transactions to Kafka."""
import argparse
import csv
import json
import os
import sys
from collections import deque
from contextlib import nullcontext
from pathlib import Path

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import NoError, TopicAlreadyExistsError, for_code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input", type=Path, nargs="?", default=Path("data/input.csv"),
        help="CSV path, or - to read CSV from standard input",
    )
    args = parser.parse_args()
    brokers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    topic = os.environ["KAFKA_TOPIC"]

    admin = KafkaAdminClient(bootstrap_servers=brokers, api_version_auto_timeout_ms=10_000)
    try:
        response = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        for topic_name, error_code, *_ in response.topic_errors:
            error = for_code(error_code)
            if error not in (NoError, TopicAlreadyExistsError):
                raise error(f"Could not create Kafka topic {topic_name}")
    finally:
        admin.close()

    producer = KafkaProducer(bootstrap_servers=brokers, api_version_auto_timeout_ms=10_000, acks="all", linger_ms=10)
    try:
        pending = deque()
        published = 0
        source_file = (nullcontext(sys.stdin) if args.input == Path("-")
                       else args.input.open(encoding="utf-8", newline=""))
        with source_file as source:
            for row in csv.DictReader(source):
                key = (row.get("transaction_id") or "").encode("utf-8")
                pending.append(producer.send(topic, key=key, value=json.dumps(row).encode("utf-8")))
                if len(pending) >= 1000:
                    pending.popleft().get(timeout=30)
                    published += 1
            for delivery in pending:
                delivery.get(timeout=30)
                published += 1
        print(f"Published {published:,} transactions to {topic}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
