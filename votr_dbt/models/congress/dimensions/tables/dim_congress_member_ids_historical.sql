-- models/dimensions/congress/dim_congress_member_ids_historical.sql

{{ config(
    materialized='table'
) }}

select
    bioguide_id,
    is_current_member,
    thomas_id,
    opensecrets_id,
    lis_id,
    cspan_id,
    govtrack_id,
    votesmart_id,
    ballotpedia_id,
    washington_post_id,
    icpsr_id,
    wikipedia_id,
    fec_id  -- Use the split and unnested FEC ID
from {{ ref('stg_congress_member_ids') }}
