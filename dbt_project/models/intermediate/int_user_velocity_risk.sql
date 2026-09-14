{% set velocity_threshold = 2 %}

select
    name_orig,
    step,
    tx_count_in_step,
    case
        when tx_count_in_step >= {{ velocity_threshold }} then 1
        else 0
    end as velocity_risk_flag
from {{ ref('stg_transaction_velocity') }}
