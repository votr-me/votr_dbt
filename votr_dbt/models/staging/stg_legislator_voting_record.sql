{{ config(materialized='incremental', primary_key = 'id')}}

WITH cleaned_votes AS (
    SELECT
        vote_id,
        vote_type,
        id AS legislator_id,
        first_name,
        last_name,
        party AS legislator_party,
        state AS legislator_state,
        vote_value AS legislator_vote,
        SPLIT(vote_id, '.')[OFFSET(0)] AS vote_num,
        SPLIT(vote_id, '.')[OFFSET(1)] AS vote_session,
        SPLIT(vote_id, '-')[OFFSET(1)] AS congress
    FROM 
        {{ source('raw', 'legislator_voting_record') }}   -- Added comma here
),
cleaned_legislators AS (
    SELECT
        bioguide_id,
        lis_id,
        first_name,
        last_name,
        state,
        party
    FROM 
        {{ source('raw', 'legislators') }} -- Replace your-project
),

joined_data AS (
    SELECT DISTINCT
        votes.vote_id,
        votes.vote_type,
        votes.legislator_id,
        votes.legislator_state,
        votes.vote_num,
        votes.vote_session,
        votes.congress,
        votes.legislator_vote,
        COALESCE(
            legislators_by_id.bioguide_id,
            legislators_by_lis_id.bioguide_id,  -- Prioritize lis_id match
            legislators_by_name.bioguide_id,
            CASE
                WHEN
                    SUBSTR(votes.legislator_id, 1, 1) = SUBSTR(votes.last_name, 1, 1)
                    AND votes.legislator_id LIKE '^[A-Z]%[0-9]{6}$'
                    THEN votes.legislator_id
            END
        ) AS bioguide_id
    FROM
        cleaned_votes AS votes
    LEFT JOIN
        cleaned_legislators AS legislators_by_id ON votes.legislator_id = legislators_by_id.bioguide_id
    LEFT JOIN
        cleaned_legislators AS legislators_by_lis_id ON votes.legislator_id = legislators_by_lis_id.lis_id
    LEFT JOIN
        cleaned_legislators AS legislators_by_name ON
            votes.first_name = legislators_by_name.first_name
            AND votes.last_name = legislators_by_name.last_name
            AND votes.legislator_state = legislators_by_name.state
            AND votes.legislator_party = SUBSTR(legislators_by_name.party, 1, 1)

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['bioguide_id', 'vote_id', 'vote_type']) }} as id,
    bioguide_id,
    vote_id,
    vote_type,
    legislator_id AS legislator_vote_id,
    vote_num,
    vote_session,
    congress,
    legislator_vote
FROM
    joined_data