
{{ config(
    materialized='table',
    primary_key = 'id'
) }}

select 
    {{dbt_utils.generate_surrogate_key(['bioguide_id', 'chamber', 'member_type', 'congress', 'state_code'])}} as id,
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
