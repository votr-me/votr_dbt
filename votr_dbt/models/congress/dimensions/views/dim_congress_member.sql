
{{ config(
    materialized='view'
) }}

with current_members as (
    select *
    from {{ ref('dim_congress_member_historical') }}
    where is_current_member
),

leadership_roles as (
    select *
    from {{ ref('dim_congress_member_leadership_historical') }}
    where is_current
)

select
    current_members.*,
    leadership_roles.leadership_type
from current_members left join leadership_roles
    on current_members.bioguide_id = leadership_roles.bioguide_id