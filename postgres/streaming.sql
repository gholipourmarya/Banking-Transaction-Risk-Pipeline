-- Apply to both new and existing databases before starting the stream.
alter table ingest.transactions
    add column if not exists risk_score smallint check (risk_score between 0 and 100),
    add column if not exists event_at timestamptz,
    add column if not exists velocity_window_start timestamptz,
    add column if not exists velocity_flag boolean not null default false,
    add column if not exists late_for_velocity boolean not null default false;

alter table ingest.transactions drop constraint if exists transactions_risk_reason_check;
alter table ingest.transactions
    add constraint transactions_risk_reason_check check (
        risk_reason in (
            'none', 'high_amount', 'overnight', 'high_amount,overnight',
            'high_velocity', 'high_amount,high_velocity', 'overnight,high_velocity',
            'high_amount,overnight,high_velocity'
        )
    );

update ingest.transactions
set risk_score = least(
        100,
        case when amount >= 50000 then 80 when amount >= 10000 then 60 else 0 end +
        case when transaction_hour < 2 then 70 when transaction_hour < 5 then 60 else 0 end
    ),
    event_at = recorded_at
where risk_score is null;

alter table ingest.transactions
    alter column risk_score set not null,
    alter column event_at set not null;

create index if not exists transactions_pending_velocity_idx
    on ingest.transactions (account_id, velocity_window_start)
    where velocity_window_start is not null and not velocity_flag;

create table if not exists ingest.stream_state (
    singleton boolean primary key default true check (singleton),
    max_event_at timestamptz
);

insert into ingest.stream_state (singleton) values (true) on conflict do nothing;

create table if not exists ingest.account_windows (
    account_id text not null,
    window_start timestamptz not null,
    transaction_count bigint not null check (transaction_count > 0),
    primary key (account_id, window_start)
);

create table if not exists ingest.stream_offsets (
    kafka_topic text not null,
    kafka_partition integer not null,
    next_offset bigint not null,
    primary key (kafka_topic, kafka_partition)
);

insert into ingest.stream_offsets (kafka_topic, kafka_partition, next_offset)
select kafka_topic, kafka_partition, max(kafka_offset) + 1
from (
    select kafka_topic, kafka_partition, kafka_offset from ingest.transactions
    union all
    select kafka_topic, kafka_partition, kafka_offset from ingest.rejected_transactions
) as processed
group by kafka_topic, kafka_partition
on conflict do nothing;

create table if not exists ingest.stream_batches (
    processed_at timestamptz not null default clock_timestamp(),
    batch_id bigint not null,
    input_records integer not null,
    accepted_records integer not null,
    rejected_records integer not null,
    late_records integer not null,
    db_retries integer not null,
    p95_processing_delay_ms double precision not null,
    watermark timestamptz
);

create table if not exists ingest.stream_errors (
    occurred_at timestamptz not null default clock_timestamp(),
    stage text not null,
    error_type text not null
);

create table if not exists ingest.kafka_lag (
    measured_at timestamptz not null default transaction_timestamp(),
    kafka_topic text not null,
    kafka_partition integer not null,
    next_offset bigint not null,
    end_offset bigint not null,
    lag bigint not null
);

create index if not exists stream_batches_time_idx on ingest.stream_batches (processed_at);
create index if not exists stream_errors_time_idx on ingest.stream_errors (occurred_at);
create index if not exists kafka_lag_time_idx on ingest.kafka_lag (measured_at);

grant usage on schema ingest to grafana_reader;
grant select on ingest.stream_batches, ingest.stream_errors, ingest.kafka_lag to grafana_reader;
