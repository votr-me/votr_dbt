{{ config(
    materialized='incremental'
) }}

with category_votes as (
    SELECT
        congress,
        category,
        count(distinct vote_id) as n_votes
    FROM {{ ref('stg_vote_info')}}
    GROUP BY 1, 2
),

congress_votes as (
    SELECT
        congress,
        count(distinct vote_id) as total_congress_votes
    FROM {{ ref('stg_vote_info')}}
    GROUP BY 1
)

SELECT
    category_votes.congress,
    category_votes.category,
    category_votes.n_votes,
    congress_votes.total_congress_votes,
    category_votes.n_votes::float/congress_votes.total_congress_votes::float as pct_category_total_votes
from category_votes left join congress_votes
    on category_votes.congress = congress_votes.congress