{{ config(materialized='table') }}
with member_terms as (
    select
        `bioguideId` AS bioguide_id,
        chamber,
        `memberType` AS member_type,
        CAST(congress AS STRING) AS congress,
        `stateCode` AS state_code,
        `stateName` AS state_name,
        CAST(district AS STRING) AS district,
        CAST(`startYear` AS INT64) AS start_year,
        CAST(`endYear` AS INT64) AS end_year
    from {{ source('raw', 'legislator_terms') }}
),

current_members as (
    select
        `bioguideId` AS bioguide_id,
        `currentMember` AS is_current_member
    from {{ source('raw', 'legislators') }}
)

SELECT
    mt.bioguide_id,
    mt.chamber,
    mt.member_type,
    mt.congress,
    mt.state_code,
    mt.state_name,
    mt.district,
    mt.start_year,
    mt.end_year,
    cm.is_current_member
FROM
    member_terms AS mt
LEFT JOIN
    current_members AS cm ON mt.bioguide_id = cm.bioguide_id