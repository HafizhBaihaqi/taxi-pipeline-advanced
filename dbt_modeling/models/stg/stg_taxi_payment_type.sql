{{
    config(
        materialized='table'
    )
}}

with payment_type as (
    select
        *
    from
        {{ source('taxi', 'payment_type') }}
)
select
    *
from
    payment_type