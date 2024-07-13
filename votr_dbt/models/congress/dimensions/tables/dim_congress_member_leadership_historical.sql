
{{ config(
    materialized='table'
) }}

select
    bioguide_id,
    congress,
    is_current,
    leadership_type
from {{ ref('stg_congress_member_leadership') }}