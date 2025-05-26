{{
    config(
        materialized='table'
    )
}}

with base as (
    select
        *
    from
        {{ ref('dim_taxi_unified') }}
),
payment_type as (
    select
        *
    from
        {{ ref('stg_taxi_payment_type') }}
),
zone as (
    select
        *
    from
        {{ ref('stg_taxi_zone') }}
),
final as (
    select
        a.id,
        a.VendorID,
        a.store_and_fwd_flag,
        a.RatecodeID,
        a.passenger_count,
        a.lpep_pickup_datetime,
        a.lpep_dropoff_datetime,
        date_diff(a.lpep_dropoff_datetime, a.lpep_pickup_datetime, hour) as trip_duration_h,
        date_diff(a.lpep_dropoff_datetime, a.lpep_pickup_datetime, minute) as trip_duration_m,
        a.trip_distance as trip_distance_mile,
        a.trip_distance * 1.6 as trip_distance_km,
        a.fare_amount,
        a.extra,
        a.mta_tax,
        a.tip_amount,
        a.tolls_amount,
        a.ehail_fee,
        a.improvement_surcharge,
        a.congestion_surcharge,
        a.total_amount,
        a.trip_type,

        -- Payment
        a.payment_type,
        b.description as payment_method,

        -- Pick Up zone
        a.PULocationID,
        c1.Borough as pickup_town,
        c1.Zone as pickup_area,
        c1.service_zone as pickup_service_zone,

        -- Drop Off zone
        a.DOLocationID,
        c2.Borough as dropoff_town,
        c2.Zone as dropoff_area,
        c2.service_zone as dropoff_service_zone,

        a.source
    from
        base a
    left join
        payment_type b
    on
        a.payment_type = b.payment_type
    -- Pick Up zone
    left join
        zone c1
    on
        a.PULocationID = c1.LocationID
    -- Drop Off zone
    left join
        zone c2
    on
        a.DOLocationID = c2.LocationID
)
select
    *
from
    final