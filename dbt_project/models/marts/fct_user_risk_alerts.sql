select
    name_orig,
    step,
    tx_count_in_step,
    velocity_risk_flag
from {{ ref('int_user_velocity_risk') }}
where velocity_risk_flag = 1
order by step desc
