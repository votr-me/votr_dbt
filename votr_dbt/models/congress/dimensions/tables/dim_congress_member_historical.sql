{{ config(
    materialized='table'
) }}


with congressional_leadership_roles as (
    SELECT bioguide_id,
           jsonb_agg(jsonb_build_object(congress::text, leadership_type)) AS leadership_titles
    FROM {{ ref('stg_congress_member_leadership') }}
    WHERE leadership_type is not null
    GROUP BY bioguide_id
)


select 
    congress_members.bioguide_id,
    congress_members.last_name,
    congress_members.first_name,
    congress_members.middle_name,
    congress_members.suffix,
    congress_members.nickname,
    congress_members.birthday,
    congress_members.gender,
    congress_members.member_type,
    congress_members.member_state,
    congress_members.member_district,
    congress_members.senate_class,
    congress_members.member_party,
    congress_members.is_current_member,
    congress_members.depiction_attribution,
    congress_members.depiction_image_url,
    congressional_leadership_roles.leadership_titles
from {{ ref('stg_congress_member') }} congress_members left join congressional_leadership_roles 
    on congress_members.bioguide_id = congressional_leadership_roles.bioguide_id
