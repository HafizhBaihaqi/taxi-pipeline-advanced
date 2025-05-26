{{
    config(
        materialized='table'
    )
}}

with taxi_zone as (
    select
        *
    from
        {{ source('taxi', 'taxi_zone') }}
)
select
    *
from
    taxi_zone