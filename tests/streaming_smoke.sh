#!/usr/bin/env bash
set -euo pipefail

workdir=$(mktemp -d)
temporary_project=
if [[ -z "${COMPOSE_PROJECT_NAME:-}" ]]; then
    export COMPOSE_PROJECT_NAME="banking-smoke-$$"
    temporary_project=1
fi
cleanup() {
    if [[ -n "$temporary_project" ]]; then
        docker compose down --volumes
    fi
    rm -f "$workdir/first.csv" "$workdir/second.csv" "$workdir/advance.csv" "$workdir/late.csv"
    rmdir "$workdir"
}
trap cleanup EXIT
python3 - "$workdir" <<'PY'
import csv
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

directory = Path(sys.argv[1])
now = datetime.now(timezone.utc)
start = now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0) - timedelta(minutes=40)

def write(name, rows, timestamp):
    with (directory / f"{name}.csv").open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(("transaction_id", "account_id", "amount", "country", "hour", "event_at"))
        for transaction_id, account_id, amount in rows:
            writer.writerow((transaction_id, account_id, amount, "DE", 12, timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")))

write("first", [(f"burst-{i}", "burst", 100) for i in range(4)] + [("invalid", "quality", -1)], start)
write("second", [("burst-0", "burst", 100), ("burst-4", "burst", 100)], start)
write("advance", [("advance", "advance", 100)], start + timedelta(minutes=30))
write("late", [("late", "burst", 100)], start)
PY

publish() {
    docker compose run --rm -T producer - < "$workdir/$1.csv"
}
query() {
    docker compose exec -T postgres psql -XAt -v ON_ERROR_STOP=1 -U banking -d banking -c "$1"
}
assert_eq() {
    local expected=$1 actual=$2
    if [[ "$actual" != "$expected" ]]; then
        printf 'Expected %s, got %s\n' "$expected" "$actual" >&2
        return 1
    fi
}

docker compose config -q
docker compose up -d --wait kafka postgres
docker compose build producer spark dbt
publish first
docker compose run --rm -T spark
assert_eq '4|0|4' "$(query "select count(*), max(risk_score), (select transaction_count from ingest.account_windows) from ingest.transactions")"
assert_eq invalid_amount "$(query 'select rejection_reason from ingest.rejected_transactions')"

publish second
# Force the sink to fail after inserting and scoring new transactions, then resume the same checkpoint.
query 'alter table ingest.stream_batches add constraint smoke_pause check (batch_id < 0) not valid'
if docker compose run --rm -T spark; then
    printf 'Spark unexpectedly succeeded during injected PostgreSQL failure\n' >&2
    exit 1
fi
assert_eq '4|0|5' "$(query 'select count(*), max(risk_score), (select next_offset from ingest.stream_offsets) from ingest.transactions')"
query 'alter table ingest.stream_batches drop constraint smoke_pause'
docker compose run --rm -T spark
assert_eq '5|5|60|5' "$(query 'select count(*), count(*) filter (where velocity_flag), min(risk_score), (select transaction_count from ingest.account_windows) from ingest.transactions')"

publish advance
docker compose run --rm -T spark
publish late
docker compose run --rm -T spark
assert_eq '7|5|1' "$(query 'select count(*), count(*) filter (where velocity_flag), count(*) filter (where late_for_velocity) from ingest.transactions')"
assert_eq 0 "$(query "select count(*) from ingest.account_windows where account_id = 'burst'")"

# A fresh Spark checkpoint replays Kafka without applying the same records twice.
docker compose run --rm -T -e SPARK_CHECKPOINT=/state/smoke-replay spark
assert_eq '7|1|4|1' "$(query 'select (select count(*) from ingest.transactions), (select count(*) from ingest.rejected_transactions), (select count(*) from ingest.stream_batches), (select count(*) from ingest.stream_errors where stage = $$stream$$)')"
assert_eq 0 "$(query 'select lag from ingest.kafka_lag order by measured_at desc limit 1')"

docker compose run --rm -T dbt
assert_eq '6|5|1|50.00' "$(query "select transactions, velocity_flagged_transactions, late_transactions, avg_risk_score from analytics.account_risk where account_id = 'burst'")"
