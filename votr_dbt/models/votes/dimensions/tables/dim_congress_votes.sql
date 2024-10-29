{{ config(
    materialized='incremental'
) }}

WITH category_votes AS (
    SELECT
        congress,
        category,
        count(distinct vote_id) AS n_votes
    FROM
        {{ ref('stg_vote_info') }}
    GROUP BY 1, 2
),

congress_votes AS (
    SELECT
        congress,
        count(distinct vote_id) AS total_congress_votes
    FROM
        {{ ref('stg_vote_info') }}
    GROUP BY 1
)

SELECT
    category_votes.congress,
    category_votes.category,
    category_votes.n_votes,
    congress_votes.total_congress_votes,
    CAST(category_votes.n_votes AS BIGNUMERIC) / CAST(congress_votes.total_congress_votes AS BIGNUMERIC) AS pct_category_total_votes
FROM
    category_votes
LEFT JOIN
    congress_votes ON category_votes.congress = congress_votes.congress
