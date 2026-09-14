select
    name_dest,
    merchant_name,
    merchant_category,
    total_transactions,
    fraud_count,
    round(avg_risk_score, 3) as avg_risk_score,
    round(fraud_count / nullif(total_transactions, 0) * 100, 3) as fraud_ratio_pct
from {{ ref('stg_high_risk_merchants') }}

where total_transactions > 0 and merchant_name is not null
order by fraud_ratio_pct desc
