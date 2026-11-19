select transaction_id
from {{ source('ingest', 'transactions') }}
where risk_score not between 0 and 100
   or risk_flag is distinct from (risk_score >= 60)
   or velocity_flag is distinct from (risk_reason like '%high_velocity')
   or (late_for_velocity and (velocity_flag or velocity_window_start is not null))
