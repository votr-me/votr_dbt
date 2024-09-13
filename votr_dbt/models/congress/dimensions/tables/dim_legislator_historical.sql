{{ config(
    materialized='table'
) }}


with congressional_leadership_roles as (
    SELECT bioguide_id,
           jsonb_agg(jsonb_build_object(congress::text, leadership_type)) AS leadership_titles
    FROM {{ ref('stg_legislator_leadership') }}
    WHERE leadership_type is not null
    GROUP BY bioguide_id
)


select 
    legislators.bioguide_id,
    legislators.last_name,
    legislators.first_name,
    legislators.middle_name,
    legislators.suffix,
    legislators.nickname,
    legislators.birthday,
    legislators.gender,
    legislators.member_type,
    legislators.member_state,
    legislators.member_district,
    legislators.senate_class,
    legislators.member_party,
    legislators.is_current_member,
    legislators.depiction_attribution,
    legislators.depiction_image_url,
    congressional_leadership_roles.leadership_titles
from {{ ref('stg_legislator') }} legislators left join congressional_leadership_roles 
    on legislators.bioguide_id = congressional_leadership_roles.bioguide_id
