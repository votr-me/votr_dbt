{{ config(
    materialized='table'
) }}

SELECT DISTINCT
    votes.vote_id,
    votes.vote_type,
    votes.display_name AS legislator_display_name,
    COALESCE(
        votes.first_name,
        legislators_by_id.first_name,
        legislators_by_name.first_name
    ) AS legislator_first_name,
    COALESCE(
        votes.last_name,
        legislators_by_id.last_name,
        legislators_by_name.last_name
    ) AS legislator_last_name,
    votes.id AS legislator_id,
    votes.party AS legislator_party,
    votes.state AS legislator_state,
    (regexp_matches(votes.vote_id, '^(.*)\.(.*)$'))[1] AS vote_num,
    (regexp_matches(votes.vote_id, '^(.*)\.(.*)$'))[2] AS vote_session,
    (regexp_matches(votes.vote_id, '^(.*)-(.*)\.(.*)$'))[2] AS congress,
    votes.vote_value AS legislator_vote,
    COALESCE(
        legislators_by_id.bioguide_id,
        legislators_by_name.bioguide_id
    ) AS bioguide_id
FROM {{ source('raw', 'legislator_voting_record') }} AS votes
LEFT JOIN {{ source('raw', 'legislators') }} AS legislators_by_id
    ON votes.id = legislators_by_id.bioguide_id
LEFT JOIN {{ source('raw', 'legislators') }} AS legislators_by_name
    ON votes.first_name = legislators_by_name.first_name
    AND votes.last_name = legislators_by_name.last_name
    AND votes.state = legislators_by_name.state
WHERE COALESCE(
    legislators_by_id.bioguide_id,
    legislators_by_name.bioguide_id
) IS NULL