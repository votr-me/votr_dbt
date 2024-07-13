
{{ config(
    materialized='view'
) }}

with current_members as (
    select *
    from {{ ref('dim_congress_member_leadership_historical') }}
    where is_current
)

select * from current_members