with vendors as (
    select distinct vendor_id
    from {{ ref('int_trips_union') }}
)

select
    distinct vendor_id,
    {{ get_vendor_names('vendor_id')}} as vendor_name
from vendors