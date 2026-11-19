select
    account_id,
    count(*) as transactions,
    count(*) filter (where risk_flag) as flagged_transactions,
    count(*) filter (where velocity_flag) as velocity_flagged_transactions,
    count(*) filter (where late_for_velocity) as late_transactions,
    round(avg(risk_score), 2) as avg_risk_score,
    sum(amount) as transaction_amount,
    coalesce(sum(amount) filter (where risk_flag), 0) as flagged_amount,
    round(100.0 * count(*) filter (where risk_flag) / count(*), 2) as risk_rate_pct,
    max(recorded_at) as last_seen_at
from {{ source('ingest', 'transactions') }}
group by account_id
