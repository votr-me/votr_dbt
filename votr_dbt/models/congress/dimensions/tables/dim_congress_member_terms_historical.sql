
{{ config(
    materialized='table'
) }}

select 
    bioguide_id,
    is_current_member,
    chamber,
    member_type,
    congress,
    state_code,
    state_name,
    district,
    start_year,
    end_year
from {{ ref('stg_congress_member_terms') }}