{{
    config(
        materialized='table'
    )
}}

with batch as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID'])}} as id
    from
        {{ source('taxi', 'taxi') }}
)
select
    *
from
    batch