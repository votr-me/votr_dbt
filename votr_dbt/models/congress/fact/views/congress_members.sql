
{{ config(
    materialized='view'
) }}

with current_members as (
    select *
    from {{ ref('congress_members_historical') }}
    where is_current_member
)

select * 
from current_members