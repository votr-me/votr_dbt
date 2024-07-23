{{ config(
    materialized='table'
) }}

select 
    bioguide_id,
    last_name,
    first_name,
    middle_name,
    suffix,
    nickname,
    birthday,
    gender,
    member_type,
    member_state,
    member_district,
    senate_class,
    member_party,
    is_current_member,
    depiction_attribution,
    depication_image_url
from {{ ref('stg_congress_member') }}