select
    day,
    total_transactions,
    fraud_transactions,
    round(fraud_rate * 100, 3) as fraud_rate_pct
from {{ ref('stg_fraud_rate_daily') }}
order by day
