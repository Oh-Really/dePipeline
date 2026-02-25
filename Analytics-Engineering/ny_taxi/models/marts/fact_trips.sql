with trips as (
    select * from {{ ref('int_trips_union') }} 
)

select * from trips