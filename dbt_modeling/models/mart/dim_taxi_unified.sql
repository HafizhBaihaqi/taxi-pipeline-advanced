{{
    config(
        materialized='table'
    )
}}

with unified as (
    select
        *,
        'batch' as source
    from
        {{ ref('stg_taxi_batch') }}
    
    union all

    select
        *,
        'stream' as source
    from
        {{ ref('stg_taxi_stream') }}
)
select
    *
from
    unified