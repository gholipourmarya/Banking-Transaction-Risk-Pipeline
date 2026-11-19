\getenv grafana_password GRAFANA_DB_PASSWORD

create schema ingest;
create schema analytics;

create table ingest.transactions (
    transaction_id text primary key,
    account_id text not null,
    amount numeric(18, 2) not null check (amount >= 0 and amount <> 'NaN'::numeric),
    country text not null check (country ~ '^[A-Z]{2}$'),
    transaction_hour smallint not null check (transaction_hour between 0 and 23),
    risk_flag boolean not null,
    risk_reason text not null check (risk_reason in ('none', 'high_amount', 'overnight', 'high_amount,overnight')),
    recorded_at timestamptz not null,
    kafka_topic text not null,
    kafka_partition integer not null,
    kafka_offset bigint not null,
    unique (kafka_topic, kafka_partition, kafka_offset)
);

create table ingest.rejected_transactions (
    kafka_topic text not null,
    kafka_partition integer not null,
    kafka_offset bigint not null,
    recorded_at timestamptz not null,
    payload bytea,
    rejection_reason text not null,
    primary key (kafka_topic, kafka_partition, kafka_offset)
);

create role grafana_reader login password :'grafana_password';
grant usage on schema analytics to grafana_reader;
alter default privileges in schema analytics grant select on tables to grafana_reader;
