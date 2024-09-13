
{{ config(
    materialized='view'
) }}

with current_members as (
    select *
    from {{ ref('legislators_historical') }}
    where is_current_member
),

leadership_titles as (
    select bioguide_id, leadership_type
    from {{ ref('stg_legislator_leadership')}}
    where is_current
)

select     
    current_members.bioguide_id,
    current_members.is_current_member,
    current_members.birthday,
    current_members.member_age,
    current_members.last_name,
    current_members.first_name,
    current_members.middle_name,
    current_members.suffix,
    current_members.member_party,
    current_members.member_state,
    current_members.member_district,
    current_members.member_type,
    current_members.member_title,
    current_members.depiction_image_url,
    current_members.depiction_attribution,
    leadership_titles.leadership_type,
    current_members.address,
    current_members.office_phone_number,
    current_members.contact_form,
    current_members.office_address,
    current_members.office_city,
    current_members.office_zipcode,
    current_members.official_website_url,
    current_members.thomas_id,
    current_members.opensecrets_id,
    current_members.lis_id,
    current_members.govtrack_id,
    current_members.votesmart_id,
    current_members.ballotpedia_id,
    current_members.icpsr_id,
    current_members.wikipedia_id,
    current_members.fec_ids
from current_members left join leadership_titles
    on current_members.bioguide_id = leadership_titles.bioguide_id