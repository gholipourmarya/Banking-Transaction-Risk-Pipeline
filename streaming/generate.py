"""Generate reproducible synthetic banking transactions as CSV."""
import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

FIELDS = ("transaction_id", "account_id", "amount", "country", "hour", "event_at")


def generate_rows(count, seed, start, prefix):
    rng = random.Random(seed)
    for index in range(count):
        event_at = start + timedelta(milliseconds=20 * index)
        anomaly = index % 1000
        amount = rng.randrange(100, 500_001)
        account = f"A{rng.randrange(20_000):05}"
        if anomaly < 8:
            account = f"BURST-{index // 1000:05}"
        if anomaly == 8:
            amount += 1_000_000
        yield {
            "transaction_id": f"{prefix}-{index + 1:06}",
            "account_id": account,
            "amount": "-1" if anomaly == 9 else f"{amount // 100}.{amount % 100:02}",
            "country": rng.choice(("DE", "FR", "NL", "ES", "GB")),
            "hour": event_at.hour,
            "event_at": event_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--count", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--prefix")
    parser.add_argument("--start", type=datetime.fromisoformat, help="ISO 8601 start time with timezone")
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")
    if args.start is not None and args.start.utcoffset() is None:
        parser.error("--start must include a timezone")
    start = args.start or datetime.now(timezone.utc) - timedelta(milliseconds=20 * args.count)
    start = start.astimezone(timezone.utc)
    prefix = args.prefix if args.prefix is not None else f"SYN-{start:%Y%m%dT%H%M%S%f}-{args.seed}"
    if not prefix or any(char.isspace() or not char.isprintable() for char in prefix):
        parser.error("--prefix must be nonempty and contain no whitespace or control characters")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.DictWriter(destination, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(generate_rows(args.count, args.seed, start, prefix))
    print(f"Wrote {args.count:,} transactions to {args.output}")


if __name__ == "__main__":
    main()
