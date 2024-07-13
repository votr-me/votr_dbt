
{{ config(
    materialized='table'
) }}

select
    bioguide_id,
    is_current_member,
    num_sponsored_legislation,
    sponsored_legislation_url,
    num_consponsored_legislation,
    cosponsored_legislation_url
from {{ ref('stg_congress_member_legislation_activity')}}