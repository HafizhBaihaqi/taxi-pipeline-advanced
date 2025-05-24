-- incremental configuration with insert overwrite strategy
{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'lpep_pickup_datetime',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        unique_key='id',
        incremental_strategy = 'insert_overwrite'
    )
}}

with increment as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID'])}} as id
    from
        {{ source('taxi', 'taxi_streaming') }}
    where
        true
        -- incremental filter
        {% if is_incremental() %}
            {%- call statement('latest', fetch_result=True) -%}
                select max(lpep_pickup_datetime) from {{ this }}
            {%- endcall -%}
            {%- set latest = load_result('latest') -%}
            and date(lpep_pickup_datetime) >= date('{{ latest["data"][0][0] }}')
        {% endif %}
)
select
    *
from
    increment