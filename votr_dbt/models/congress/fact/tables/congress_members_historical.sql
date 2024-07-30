-- models/member_bio_info.sql
{{ config(
    materialized='table'
) }}

WITH member_bio_info AS (
    SELECT
        bioguide_id,
        is_current_member,
        birthday::date AS birthday,
        DATE_PART('year', AGE(CURRENT_DATE, birthday::date)) AS member_age,
        INITCAP(last_name) AS last_name,
        INITCAP(first_name) AS first_name,
        INITCAP(middle_name) AS middle_name,
        INITCAP(suffix) AS suffix,
        INITCAP(member_party) AS member_party,
        member_state,
        member_district,
        member_type,
        CASE
            WHEN member_type = 'rep' THEN 'Representative'
            WHEN member_type = 'sen' THEN 'Senator'
            ELSE NULL
        END AS member_title,
        depiction_image_url,
        depiction_attribution
    FROM {{ ref('dim_congress_member_historical') }}
),

contact_info AS (
    SELECT DISTINCT
        bioguide_id,
        address,
        office_phone_number,
        contact_form,
        office_address,
        office_city,
        office_zipcode,
        official_website_url
    FROM {{ ref('dim_congress_member_contact_info_historical') }}
),

social_ids AS (
    SELECT
        bioguide_id,
        is_current_member,
        thomas_id,
        opensecrets_id,
        lis_id,
        govtrack_id,
        votesmart_id,
        ballotpedia_id,
        icpsr_id,
        wikipedia_id,
        ARRAY_AGG(fec_id) AS fec_ids
    FROM {{ ref('dim_congress_member_ids_historical') }}
    GROUP BY 
        bioguide_id, 
        is_current_member, 
        thomas_id, 
        opensecrets_id, 
        lis_id, 
        govtrack_id, 
        votesmart_id, 
        ballotpedia_id, 
        icpsr_id, 
        wikipedia_id
)

SELECT
    bio.bioguide_id,
    bio.is_current_member,
    bio.birthday,
    bio.member_age,
    bio.last_name,
    bio.first_name,
    bio.middle_name,
    bio.suffix,
    bio.member_party,
    bio.member_state,
    bio.member_district,
    bio.member_type,
    bio.member_title,
    bio.depiction_image_url,
    bio.depiction_attribution,
    contact.address,
    contact.office_phone_number,
    contact.contact_form,
    contact.office_address,
    contact.office_city,
    contact.office_zipcode,
    contact.official_website_url,
    social.thomas_id,
    social.opensecrets_id,
    social.lis_id,
    social.govtrack_id,
    social.votesmart_id,
    social.ballotpedia_id,
    social.icpsr_id,
    social.wikipedia_id,
    social.fec_ids
FROM member_bio_info AS bio
LEFT JOIN contact_info AS contact
    ON bio.bioguide_id = contact.bioguide_id
LEFT JOIN social_ids AS social
    ON bio.bioguide_id = social.bioguide_id
