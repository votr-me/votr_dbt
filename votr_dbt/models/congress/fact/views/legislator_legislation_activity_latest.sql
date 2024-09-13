
{{ config(
    materialized='view'
) }}

with current_members as (
    select *
    from {{ ref('legislator_legislation_activity_historical') }}
    where is_current_member
)

select * 
from current_members