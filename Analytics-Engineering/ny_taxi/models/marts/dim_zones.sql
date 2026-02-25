with taxi_zone_lookup as (
    select
    locationid as location_id,
    borough,
    zone,
    service_zone
from {{ ref('taxi_zone_lookup') }}
)

select * from taxi_zone_lookup order by location_id
