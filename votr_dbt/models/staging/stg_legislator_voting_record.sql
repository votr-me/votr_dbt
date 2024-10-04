-- File: models/staging/stg_legislator_voting_record.sql

{{ config(materialized='incremental') }}

WITH cleaned_votes AS (

    SELECT
        vote_id,
        vote_type,
        display_name AS legislator_display_name,
        {{ clean_text('first_name') }} AS first_name_clean,
        {{ clean_text('last_name') }} AS last_name_clean,
        id AS legislator_id,
        party AS legislator_party,
        state AS legislator_state,
        vote_value AS legislator_vote,
        (regexp_matches(vote_id, '^(.*)\.(.*)$'))[1] AS vote_num,
        (regexp_matches(vote_id, '^(.*)\.(.*)$'))[2] AS vote_session,
        (regexp_matches(vote_id, '^(.*)-(.*)\.(.*)$'))[2] AS congress
    FROM {{ source('raw', 'legislator_voting_record') }}

),

cleaned_legislators AS (

    SELECT
        bioguide_id,
        lis_id,
        {{ clean_text('first_name') }} AS first_name_clean,
        {{ clean_text('last_name') }} AS last_name_clean,
        state
    FROM {{ source('raw', 'legislators') }}

),

joined_data AS (

    SELECT DISTINCT
        votes.vote_id,
        votes.vote_type,
        votes.legislator_display_name,
        COALESCE(
            votes.first_name_clean,
            legislators_by_id.first_name_clean,
            legislators_by_lis_id.first_name_clean,
            legislators_by_name.first_name_clean
        ) AS legislator_first_name,
        COALESCE(
            votes.last_name_clean,
            legislators_by_id.last_name_clean,
            legislators_by_lis_id.last_name_clean,
            legislators_by_name.last_name_clean
        ) AS legislator_last_name,
        votes.legislator_id,
        votes.legislator_party,
        votes.legislator_state,
        votes.vote_num,
        votes.vote_session,
        votes.congress,
        votes.legislator_vote,
        COALESCE(
            CASE
                WHEN LEFT(legislators_by_id.bioguide_id, 1) = LEFT(legislators_by_id.last_name_clean, 1)
                    AND legislators_by_id.bioguide_id ~ '^[A-Z]\d{6}$' THEN legislators_by_id.bioguide_id
                WHEN LEFT(legislators_by_lis_id.bioguide_id, 1) = LEFT(legislators_by_lis_id.last_name_clean, 1)
                    AND legislators_by_lis_id.bioguide_id ~ '^[A-Z]\d{6}$' THEN legislators_by_lis_id.bioguide_id
                WHEN LEFT(legislators_by_name.bioguide_id, 1) = LEFT(legislators_by_name.last_name_clean, 1)
                    AND legislators_by_name.bioguide_id ~ '^[A-Z]\d{6}$' THEN legislators_by_name.bioguide_id
                WHEN LEFT(votes.legislator_id, 1) = LEFT(votes.last_name_clean, 1)
                    AND votes.legislator_id ~ '^[A-Z]\d{6}$' THEN votes.legislator_id
                ELSE NULL
            END
        ) AS bioguide_id
    FROM cleaned_votes AS votes
    LEFT JOIN cleaned_legislators AS legislators_by_id
        ON votes.legislator_id = legislators_by_id.bioguide_id
    LEFT JOIN cleaned_legislators AS legislators_by_lis_id
        ON votes.legislator_id = legislators_by_lis_id.lis_id
    LEFT JOIN cleaned_legislators AS legislators_by_name
        ON votes.first_name_clean = legislators_by_name.first_name_clean
        AND votes.last_name_clean = legislators_by_name.last_name_clean
        AND votes.legislator_state = legislators_by_name.state

)

SELECT *
FROM joined_data
