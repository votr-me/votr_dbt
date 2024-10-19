{{ config(
    materialized='view'
) }}

select distinct *
from {{ ref('legislators_historical') }}
where is_current_member
