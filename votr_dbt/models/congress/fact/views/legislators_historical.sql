{{ config(
    materialized='table'
) }}

WITH member_bio_info AS (
    SELECT
        bioguide_id,
        is_current_member,
        CAST(birthday AS DATE) AS birthday,
        member_state,
        member_district,
        member_type,
        depiction_image_url,
        depiction_attribution,
        leadership_titles,
        CAST(DATE_DIFF(CURRENT_DATE(), CAST(birthday AS DATE), YEAR) AS INT64) AS member_age,
        INITCAP(last_name) AS last_name,
        INITCAP(first_name) AS first_name,
        INITCAP(middle_name) AS middle_name,
        INITCAP(suffix) AS suffix,
        INITCAP(member_party) AS member_party,
        CASE
            WHEN member_type = 'rep' THEN 'Representative'
            WHEN member_type = 'sen' THEN 'Senator'
        END AS member_title
    FROM
        {{ ref('dim_legislator_historical') }}
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
    FROM
        {{ ref('dim_legislator_contact_info_historical') }}
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
        fec_ids
    FROM
        {{ ref('dim_legislator_ids_historical') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11  -- Simplified GROUP BY
),

legislative_activity AS (
    SELECT
        bioguide_id,
        num_sponsored_legislation,
        sponsored_legislation_url,
        num_cosponsored_legislation,
        cosponsored_legislation_url
    FROM
        {{ ref('legislator_legislation_activity_historical') }}
),

legislator_term_summary AS (
    SELECT
        bioguide_id,
        is_current_member,
        term_count,
        first_year_in_chamber,
        last_year_in_chamber,
        total_years_served,
        num_congresses_served
    FROM
        {{ ref('legislator_term_summary') }}
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
    bio.leadership_titles AS leadership_type,
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
    social.fec_ids,
    legislative_activity.num_sponsored_legislation,
    legislative_activity.num_cosponsored_legislation,
    legislator_term_summary.term_count,
    legislator_term_summary.first_year_in_chamber,
    legislator_term_summary.last_year_in_chamber,
    legislator_term_summary.total_years_served,
    legislator_term_summary.num_congresses_served
FROM
    member_bio_info AS bio
LEFT JOIN
    contact_info AS contact ON bio.bioguide_id = contact.bioguide_id
LEFT JOIN
    social_ids AS social ON bio.bioguide_id = social.bioguide_id
LEFT JOIN
    legislative_activity ON bio.bioguide_id = legislative_activity.bioguide_id
LEFT JOIN
    legislator_term_summary ON bio.bioguide_id = legislator_term_summary.bioguide_id