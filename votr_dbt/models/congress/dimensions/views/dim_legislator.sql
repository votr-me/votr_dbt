{{ config(
    materialized='view'
) }}

select *
from {{ ref('dim_legislator_historical') }}
where is_current_member
