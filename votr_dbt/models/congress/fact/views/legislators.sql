
{{ config(
    materialized='view'
) }}

select *
from {{ ref('legislators_historical')}}
where is_current_member