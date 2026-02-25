with payment_type_lookup as (
    select distinct payment_type,
    from {{ ref('int_trips_union') }}
)

select 
    distinct payment_type,
    {{ get_payment_names('payment_type')}} as payment_name
from payment_type_lookup
