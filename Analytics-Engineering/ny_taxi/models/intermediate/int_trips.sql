with trips_unioned as (
    select * from {{ ref('int_trips_union')}}
),
payment_types as (
    select * from {{ ref('payment_type_lookup')}}
),

cleaned as (
    select
        -- Generate unique trip identifier (surrogate key pattern)
        {{ dbt_utils.generate_surrogate_key(['u.vendor_id', 'u.pickup_datetime', 'u.pickup_location_id', 'u.service_type']) }} as trip_id,

        -- Identifiers
        u.vendor_id,
        u.service_type,
        u.ratecode_id,

        -- Location IDs
        u.pickup_location_id,
        u.dropoff_location_id,

        -- Timestamps
        u.pickup_datetime,
        u.dropoff_datetime,

        -- Trip details
        u.store_and_fwd_flag,
        u.passenger_count,
        u.trip_distance,
        u.trip_type,

        -- Payment breakdown
        u.fare_amount,
        u.extra,
        u.mta_tax,
        u.tip_amount,
        u.tolls_amount,
        u.ehail_fee,
        u.improvement_surcharge,
        u.total_amount,

        -- Enrich with payment type description
        coalesce(u.payment_type, 0) as payment_type,
        coalesce(pt.description, 'Unknown') as payment_type_description

    from trips_unioned u
    left join payment_types pt
        on coalesce(u.payment_type, 0) = pt.payment_type
)

select * from cleaned

qualify row_number() over(
    partition by vendor_id, pickup_datetime, pickup_location_id, service_type
    order by dropoff_datetime
) = 1